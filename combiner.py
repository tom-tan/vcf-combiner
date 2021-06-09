#!/usr/bin/env python
import hail as hl

from itertools import islice
from argparse import ArgumentParser

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(metavar='input-file', dest='path_to_input_list',
                        help='a file that lists VCFs (i.e., `*.vcf` or `*.vcf.gz`)')
    parser.add_argument('-n', type=str,
                        help='only use first n items from the input file (default: unlimited)')
    parser.add_argument('--batch-size', dest='batch_size', type=str, default='100',
                        help='batch size of the backend Spark cluster')
    parser.add_argument('--log', dest='logfile', default='output.log',
                        help='log output file name')
    parser.add_argument('-o', '--output', default='output.mt',
                        help='output directory name')
    parser.add_argument('--executor-memory', dest='executor_memory', default='42g',
                        help='memory allocation for spark executor')
    parser.add_argument('--driver-memory', dest='driver_memory', default='50g',
                        help='memory allocation for spark driver')
    parser.add_argument('--executor-cores', dest='executor_cores', type=str,
                        help='#cores for each executor')
    args = parser.parse_args()

    conf = {
        'spark.executor.memory': args.executor_memory,
        'spark.driver.memory': args.driver_memory,
    }
    if args.executor_cores is not None:
        conf['spark.executor.cores'] = args.executor_cores

    inputs = []
    with hl.hadoop_open(args.path_to_input_list, 'r') as f:
    # with open(args.path_to_input_list, 'r') as f:
        if args.n is None:
            inputs = [line.strip() for line in f]
        else:
            inputs = list(islice((line.strip() for line in f), 0, args.n))

    temp_path = 'temp'

    hl.init(spark_conf=conf, log=args.logfile)
    hl.experimental.run_combiner(inputs, out_file=args.output, tmp_path=temp_path,
                                 use_genome_default_intervals=True)
