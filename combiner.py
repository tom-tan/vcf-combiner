#!/usr/bin/env python
import hail as hl

import os
import sys

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
        conf = {
                'spark.executor.memory': '42g',
                'spark.driver.memory': '50g'
        }

        paths = []
        workdir = os.getcwd()
        hl.init(spark_conf=conf, log=workdir+'/combiner.log')

        args = sys.argv

        inputs = []
        if len(args) == 1:
                inputs = paths
        else:
                path_to_input_list = args[1]

#               with hl.hadoop_open(path_to_input_list, 'r') as f:
                with open(path_to_input_list, 'r') as f:
                    for line in f:
                        inputs.append(line.strip())

        output_file = 'output.mt'
        temp_path = 'temp'
        hl.experimental.run_combiner(inputs, out_file=output_file, tmp_path=temp_path,
                                     use_genome_default_intervals=True)
