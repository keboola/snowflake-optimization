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

class CsvToolSplit
{
    public function split(CsvFile $source, array $destinations) {
        $process = new Process('csvtool height ' . $source->getPathname());
        $process->mustRun();
        $rows = $process->getOutput();
        $linesPerFile = round($rows / count($destinations));
        for ($i = 0; $i < count($destinations); $i++) {
            $drop = $i * $linesPerFile;
            $command = "csvtool drop {$drop} " . escapeshellarg($source->getPathname()) . " | csvtool take {$linesPerFile} - > " . escapeshellarg($destinations[$i]->getPathname());
            (new Process($command))->mustRun();
        }
    }
}
