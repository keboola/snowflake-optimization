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
    $csvToolSplitFiles = [];
    for ($i = 0; $i < $parameters["splitFiles"]; $i++) {
        $csvFileSplitFiles[] = new Keboola\Csv\CsvFile($temp->getTmpFolder() . "/csvfile_part_{$i}.csv");
        $csvToolSplitFiles[] = $temp->createFile("csvtool_part_{$i}.csv");
    }
    $csv->rewind();

    $time = microtime(true);
    $csvFileSplit->split($csv, $csvFileSplitFiles);
    $duration = microtime(true) - $time;

    print "{$parameters["rows"]} rows with " . count($parameters["row"]) . " columns by {$rowSize} bytes ($sizeMB MB) split into {$parameters["splitFiles"]} files using CsvFile in $duration seconds\n";

    $csv->rewind();

    $time = microtime(true);
    $csvToolSplit->split($csv, $csvToolSplitFiles);
    $duration = microtime(true) - $time;

    print "{$parameters["rows"]} rows with " . count($parameters["row"]) . " columns by {$rowSize} bytes ($sizeMB MB) split into {$parameters["splitFiles"]} files using CsvTool in $duration seconds\n";

    // cleanup
    unlink($csv->getPathname());
    foreach ($csvFileSplitFiles as $splitFile) {
        unlink($splitFile->getPathname());
    }
    foreach ($csvToolSplitFiles as $splitFile) {
        unlink($splitFile->getPathname());
    }

}

