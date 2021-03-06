#!/usr/bin/env python
import hail as hl

from itertools import islice
from argparse import ArgumentParser
from os import getcwd, makedirs
from os.path import join, realpath

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(metavar='input-file', dest='path_to_input_list',
                        help='a file that lists VCFs (i.e., `*.vcf` or `*.vcf.gz`)')
    parser.add_argument('-n', type=int,
                        help='only use first n items from the input file (default: unlimited)')
    parser.add_argument('--master', dest='master', type=str,
                        help='Spark leader node')
    parser.add_argument('--log', dest='logfile', default='output.log',
                        help='log output file name')
    parser.add_argument('-o', '--output', default='output.mt',
                        help='output directory name')
    parser.add_argument('--tmpdir', default='temp', type=str,
                        help='directory for intermediate output. It must be network visible path.')
    parser.add_argument('--executor-memory', dest='executor_memory', default='42g',
                        help='memory allocation for spark executor')
    parser.add_argument('--driver-memory', dest='driver_memory', default='50g',
                        help='memory allocation for spark driver')
    parser.add_argument('--executor-cores', dest='executor_cores', type=str,
                        help='#cores for each executor')
    parser.add_argument('--branch-factor', dest='branch_factor', type=int,
                        help='branch factor to hierarchically merge GVCFs')
    parser.add_argument('--batch-size', dest='batch_size', type=int,
                        help='batch size')
    # parser.add_argument('--force', help='force output to the existing directory')
    args = parser.parse_args()

    pwd = getcwd()

    conf = {
        'spark.executor.memory': args.executor_memory,
        'spark.driver.memory': args.driver_memory,
    }
    if args.executor_cores is not None:
        conf['spark.executor.cores'] = args.executor_cores

    if args.master is None:
        sc = None
    else:
        con = SparkConf()
        con.setMaster(args.master)
        sc = SparkContext(con)
    
    if args.tmpdir is None:
        base_tmpdir = join(realpath(pwd), 'tmp')
    else:
        base_tmpdir = realpath(args.tmpdir)

    network_tmpdir = join(base_tmpdir, 'network-tmp')
    makedirs(network_tmpdir)
    combiner_tmpdir = join(base_tmpdir, 'combiner-tmp')
    makedirs(combiner_tmpdir)

    inputs = []
    hl.init(sc=sc, spark_conf=conf, log=realpath(args.logfile), tmp_dir=network_tmpdir)
    with hl.hadoop_open(realpath(args.path_to_input_list), 'r') as f:
        if args.n is None:
            inputs = [line.strip() for line in f]
        else:
            inputs = list(islice((line.strip() for line in f), 0, args.n))

    if args.branch_factor:
        factor = args.branch_factor
    else:
        factor = 100

    if args.batch_size:
        bsize = args.batch_size
    else:
        bsize = 100

    hl.experimental.run_combiner(inputs, out_file=realpath(args.output), tmp_path=combiner_tmpdir,
                                 use_genome_default_intervals=True, branch_factor=factor, batch_size=bsize,
                                 reference_genome='GRCh38')
