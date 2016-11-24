<?php

ini_set('display_errors', true);

require_once __DIR__ . '/vendor/autoload.php';

$arguments = getopt("d::", array("data::"));
$dataFolder = "/data";
if (isset($arguments["data"])) {
    $dataFolder = $arguments["data"];
}
$config = json_decode(file_get_contents($dataFolder . "/config.json"), true)["parameters"];

$csvFileSplit = new \Keboola\Snowflake\Optimization\CsvFileSplit();
$csvToolSplit = new \Keboola\Snowflake\Optimization\CsvToolSplit();

$chars = [
    "\t", "\n", "a", "b", "c", "d", "e", "f"
];

function generateCell($bytes, $chars) {
    $cell = "";
    for ($j = 0; $j < $bytes; $j++) {
        $cell .= $chars[mt_rand(0, count($chars) - 1)];
    }
    return $cell;
}

$k1row = generateCell(1000, $chars);
$k10row = generateCell(10000, $chars);
$k100row = generateCell(100000, $chars);

$matrix = $config["matrix"];
foreach ($matrix as $key => $matrixItem) {
    $newRow = [];
    foreach ($matrixItem["row"] as $rowItem) {
        switch($rowItem) {
            case "k1row":
                $newRow[] = $k1row;
                break;
            case "k10row":
                $newRow[] = $k10row;
                break;
            case "k100row":
                $newRow[] = $k100row;
                break;
            default:
                throw new \Exception("invalid row identifier");
                break;
        }
    }
    $matrix[$key]["row"] = $newRow;
}

foreach($matrix as $parameters) {
    $temp = new Keboola\Temp\Temp();
    $source = $temp->createFile('source.csv');
    $csv = new Keboola\Csv\CsvFile($source->getPathname());

    $rowSize = 0;
    foreach ($parameters["row"] as $cell) {
        $rowSize += strlen($cell);
    }
    $sizeMB = round($parameters["rows"] * $rowSize / 1024**2);

    $time = microtime(true);
    $csvFileSplit->generateFile($csv, $parameters["rows"], $parameters["row"], $chars);
    $duration = microtime(true) - $time;

    print "{$parameters["rows"]} rows with " . count($parameters["row"]) . " columns by {$rowSize} bytes ($sizeMB MB) generated in $duration seconds\n";

    $csvFileSplitFiles = [];
    for ($i = 0; $i < $parameters["splitFiles"]; $i++) {
        $csvFileSplitFiles[] = new Keboola\Csv\CsvFile($temp->getTmpFolder() . "/csvfile_part_{$i}.csv");
    }
    $csv->rewind();

    $time = microtime(true);
    $csvFileSplit->split($csv, $csvFileSplitFiles);
    $duration = microtime(true) - $time;

    print "{$parameters["rows"]} rows with " . count($parameters["row"]) . " columns by {$rowSize} bytes ($sizeMB MB) split into {$parameters["splitFiles"]} files using CsvFile in $duration seconds\n";

    $csv->rewind();

    // upload files to S3

    $credentials = new \Aws\Credentials\Credentials(
        $config['AWS_ACCESS_KEY_ID'],
        $config['#AWS_SECRET_ACCESS_KEY']
    );
    $s3client = new \Aws\S3\S3Client(
        [
            "credentials" => $credentials,
            "region" => $config['AWS_REGION'],
            "version" => "2006-03-01"
        ]
    );

    $time = microtime(true);
    do {
        try {
            $result = $s3client->upload(
                $config['AWS_S3_BUCKET'],
                $config['S3_KEY_PREFIX'] . "/" . $csv->getBasename(),
                fopen($csv->getPathname(), "r")
            );
        } catch (\Aws\Exception\MultipartUploadException $e) {
            print "Retrying upload: " . $e->getMessage();
        }
    } while (!isset($result));
    $duration = microtime(true) - $time;
    print "$sizeMB MB file uploaded to S3 using 'upload' method in $duration seconds\n";

    $time = microtime(true);
    $result = $s3client->putObject(
        [
            'Bucket' => $config['AWS_S3_BUCKET'],
            'Key' => $config['S3_KEY_PREFIX'] . "/" . $csv->getBasename(),
            'Body' => fopen($csv->getPathname(), "r+"),
        ]
    );
    $duration = microtime(true) - $time;
    print "$sizeMB MB file uploaded to S3 using 'putObject' method in $duration seconds\n";

    /**
     * @var $splitFile \Keboola\Csv\CsvFile
     */
    $time = microtime(true);
    // well, i have to rerun the whole thing again, as i have no idea which slices are done and slice failed
    do {
        try {
            $handles = [];
            $promises = [];
            foreach ($csvFileSplitFiles as $key => $splitFile) {
                $promises[] = $s3client->uploadAsync(
                    $config['AWS_S3_BUCKET'],
                    $config['S3_KEY_PREFIX'] . "/" . $splitFile->getBasename(),
                    fopen($splitFile->getPathname(), "r")
                );
            }
            $results = GuzzleHttp\Promise\unwrap($promises);
            $finished = true;
        } catch (\Aws\Exception\MultipartUploadException $e) {
            print "Retrying upload: " . $e->getMessage();
        }
    } while (!isset($finished));
    $duration = microtime(true) - $time;
    print "$sizeMB MB split into {$parameters["splitFiles"]} files uploaded to S3 using 'uploadAsync' method in $duration seconds\n";

    /**
     * @var $splitFile \Keboola\Csv\CsvFile
     */
    $time = microtime(true);
    $promises = [];
    foreach ($csvFileSplitFiles as $key => $splitFile) {
        $promises[] = $s3client->putObjectAsync(
            [
                'Bucket' => $config['AWS_S3_BUCKET'],
                'Key' => $config['S3_KEY_PREFIX'] . "/" . $splitFile->getBasename(),
                'Body' => fopen($splitFile->getPathname(), "r+"),
            ]
        );
    }
    $results = GuzzleHttp\Promise\unwrap($promises);
    $duration = microtime(true) - $time;
    print "$sizeMB MB split into {$parameters["splitFiles"]} files uploaded to S3 using 'putObjectAsync' method in $duration seconds\n";


    // cleanup
    unlink($csv->getPathname());
    foreach ($csvFileSplitFiles as $splitFile) {
        unlink($splitFile->getPathname());
    }
}

