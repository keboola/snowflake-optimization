<?php

ini_set('display_errors', true);

require_once __DIR__ . '/vendor/autoload.php';

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
        "rows" => 100000,
        "row" => [$k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row, $k10row],
        "splitFiles" => 5
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
}

