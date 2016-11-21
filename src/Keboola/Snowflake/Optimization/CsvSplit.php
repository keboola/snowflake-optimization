<?php
/**
 * Created by PhpStorm.
 * User: ondra
 * Date: 14/11/16
 * Time: 13:05
 */

namespace Keboola\Snowflake\Optimization;

use Keboola\Csv\CsvFile;

class CsvSplit
{
    public function generateFile(CsvFile $csv, $rows, $row) {
        for ($i = 0; $i < $rows; $i++) {
            $csv->writeRow($row);
        }
    }

    public function split(CsvFile $source, array $destinations) {
        while($source->current()) {
            $destinations[mt_rand(0, count($destinations) - 1)]->writeRow($source->current());
            $source->next();
        }
    }
}