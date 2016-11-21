<?php
/**
 * Created by PhpStorm.
 * User: ondra
 * Date: 14/11/16
 * Time: 13:05
 */

namespace Keboola\Snowflake\Optimization;

use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;

class CsvToolSubSplit
{
    public function split(CsvFile $source, array $destinations) {
        $rows = trim((new Process('csvtool height ' . $source->getPathname()))->mustRun()->getOutput());
        $columns = trim((new Process('csvtool width ' . $source->getPathname()))->mustRun()->getOutput());
        $linesPerFile = round($rows / count($destinations));
        for ($i = 0; $i < count($destinations); $i++) {
            $drop = $i * $linesPerFile;
            $command = "csvtool -z sub {$drop} 0 {$linesPerFile} {$columns} " . $source->getPathname() . " > " . $destinations[$i]->getPathname();
            (new Process($command))->mustRun();
        }
    }
}
