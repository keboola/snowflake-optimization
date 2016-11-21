<?php

ini_set('display_errors', true);

require_once __DIR__ . '/vendor/autoload.php';

$arguments = getopt("d::", array("data::"));
$dataFolder = "/data";
if (isset($arguments["data"])) {
    $dataFolder = $arguments["data"];
}
$config = json_decode(file_get_contents($dataFolder . "/config.json"), true)["parameters"];

$csvSplit = new \Keboola\Snowflake\Optimization\CsvSplit();

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

$matrix = [
    [
        "rows" => 1000,
        "row" => [$k1row],
        "splitFiles" => 10
    ],
    [
        "rows" => 10000,
        "row" => [$k10row],
        "splitFiles" => 2
    ],
    [
        "rows" => 100000,
        "row" => [$k10row],
        "splitFiles" => 2
    ],
    [
        "rows" => 100000,
        "row" => [$k10row],
        "splitFiles" => 10
    ],
    [
        "rows" => 10000,
        "row" => [$k100row],
        "splitFiles" => 2
    ],
    [
        "rows" => 10000,
        "row" => [$k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row],
        "splitFiles" => 5
    ],
    [
        "rows" => 80000,
        "row" => [$k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row],
        "splitFiles" => 100
    ]
];

foreach($matrix as $parameters) {
    $temp = new Keboola\Temp\Temp();
    $source = $temp->createFile('source.csv');
    $csv = new Keboola\Csv\CsvFile($source->getPathname());
    $csvSplit->generateFile($csv, $parameters["rows"], $parameters["row"], $chars);
    $splitFiles = [];
    for ($i = 0; $i < $parameters["splitFiles"]; $i++) {
        $splitFiles[] = new Keboola\Csv\CsvFile($temp->getTmpFolder() . "/part_{$i}.csv");
    }
    $csv->rewind();

    $time = microtime(true);
    $csvSplit->split($csv, $splitFiles);
    $duration = microtime(true) - $time;

    $rowSize = 0;
    foreach ($parameters["row"] as $cell) {
        $rowSize += strlen($cell);
    }
    $sizeMB = round($parameters["rows"] * $rowSize / 1024**2);
    print "{$parameters["rows"]} rows with " . count($parameters["row"]) . " columns by {$rowSize} bytes ($sizeMB MB) split into {$parameters["splitFiles"]} files in $duration seconds\n";
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
    $s3client->upload(
        $config['AWS_S3_BUCKET'],
        $config['S3_KEY_PREFIX'] . "/" . $csv->getBasename(),
        fopen($csv->getPathname(), "r")
    );
    $duration = microtime(true) - $time;
    print "$sizeMB MB file uploaded to S3 in $duration seconds\n";

    /**
     * @var $splitFile \Keboola\Csv\CsvFile
     */
    $time = microtime(true);
    $promises = [];
    foreach ($splitFiles as $splitFile) {
        $promises[] = $s3client->uploadAsync(
            $config['AWS_S3_BUCKET'],
            $config['S3_KEY_PREFIX'] . "/" . $splitFile->getBasename(),
            fopen($splitFile->getPathname(), "r")
        )->otherwise(function($reason) {
            throw new \Exception("Upload failed: " . $reason);
        });
    }

    $results = GuzzleHttp\Promise\unwrap($promises);
    $duration = microtime(true) - $time;
    print "$sizeMB MB split into {$parameters["splitFiles"]} files uploaded to S3 in $duration seconds\n";
}

