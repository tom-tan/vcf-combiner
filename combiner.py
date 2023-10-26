#!/usr/bin/env python
import hail as hl

from itertools import islice
from argparse import ArgumentParser
from os import makedirs, environ
from os.path import exists, expanduser, join, realpath
from shutil import rmtree
import logging

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(metavar='input-file', dest='path_to_input_list',
                        help='a file that lists VCFs (i.e., `*.vcf` or `*.vcf.gz`)')
    parser.add_argument('--vds', type=str, action='append',
                        help='a path to the VDS directory')
    parser.add_argument('-n', type=int,
                        help='only use first n items from the input file (default: unlimited)')
    parser.add_argument('--master', dest='master', type=str,
                        help='Spark leader node')
    parser.add_argument('--output-base', dest='out_base', type=str,
                        help='base output directory that stores logs and outputs')
    # parser.add_argument('--log', dest='logfile', default='output.log',
    #                     help='log output file name')
    # parser.add_argument('-o', '--output', default='output.mt',
    #                     help='output directory name')
    # parser.add_argument('--tmpdir', default='temp', type=str,
    #                     help='directory for intermediate output. It must be network visible path.')
    parser.add_argument('--executor-memory', dest='executor_memory', default='42g',
                        help='memory allocation for spark executor')
    parser.add_argument('--driver-memory', dest='driver_memory', default='50g',
                        help='memory allocation for spark driver')
    parser.add_argument('--executor-instances', dest='executor_instances', type=int,
                        help='#instances for each node')
    parser.add_argument('--executor-cores', dest='executor_cores', type=str,
                        help='#cores for each executor')
    parser.add_argument('--driver-cores', dest='driver_cores', type=str,
                        help='#cores for each driver')
    parser.add_argument('--branch-factor', dest='branch_factor', type=int, default=100,
                        help='branch factor to hierarchically merge GVCFs')
    # parser.add_argument('--batch-size', dest='batch_size', type=int,
    #                     help='batch size')
    # parser.add_argument('--force', help='force output to the existing directory')
    args = parser.parse_args()

    if args.vds is None:
        input_vdses = None
    else:
        input_vdses = [realpath(expanduser(v)) for v in args.vds]

    FORMAT = '%(asctime)-15s: %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)

    conf = {
        'spark.executor.memory': args.executor_memory,
        'spark.driver.memory': args.driver_memory,
        # 'spark.yarn.am.memory': args.driver_memory,
        # 'spark.jars': join(environ['HAIL_DIR'], 'hail-all-spark.jar'),
        # 'spark.execuor.extraClassPath': './hail-all-spark.jar',
        # 'spark.driver.extraClassPath': join(environ['HAIL_DIR'], 'hail-all-spark.jar'),
        # 'spark.yarn.stagingDir': 'lustre:///lustre8/home/tanjo-pg',
    }
    if args.executor_instances is not None:
        conf['spark.executor.instances'] = str(args.executor_instances)
    if args.executor_cores is not None:
        conf['spark.executor.cores'] = str(args.executor_cores)
    if args.driver_cores is not None:
        conf['spark.driver.cores'] = str(args.driver_cores)
        # conf['spark.yarn.am.cores'] = str(args.driver_cores)

    if args.master is None:
        sc = None
        spark_conf = conf
    else:
        con = SparkConf()
        con.setMaster(args.master)
        con.set('spark.submit.deployMode', 'client')
        # con.set('spark.yarn.stagingDir', 'lustre:///lustre8/home/tanjo-pg')
        con.set('spark.jars', join(environ['HAIL_DIR'], 'hail-all-spark.jar'))
        # con.set('spark.files', join(environ['HAIL_DIR'], 'hail-all-spark.jar'))
        # con.set('spark.execuor.extraLibraryPath', '/home/tanjo-pg/miniconda3/envs/hail-env/lib')
        # con.set('spark.execuor.extraClassPath', './hail-all-spark.jar')
        # con.set('spark.driver.extraClassPath', join(environ['HAIL_DIR'], 'hail-all-spark.jar'))
        con.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        con.set('spark.kryo.registrator', 'is.hail.kryo.HailKryoRegistrator')
        for k, v in conf.items():
            con.set(k, v)
        sc = SparkContext(conf=con)
        spark_conf = None
    
    # if args.tmpdir is None:
    #     base_tmpdir = realpath(expanduser('~/.tmp'))
    # else:
    #     base_tmpdir = realpath(args.tmpdir)

    out_base = realpath(expanduser(args.out_base))
    makedirs(out_base)
    logfile = join(out_base, 'output.log')

    base_tmpdir = join(out_base, 'tmp')
    network_tmpdir = join(base_tmpdir, 'network-tmp')
    makedirs(network_tmpdir)
    combiner_tmpdir = join(base_tmpdir, 'combiner-tmp')
    makedirs(combiner_tmpdir)

    try:
        inputs = []
        hl.init(sc=sc, spark_conf=spark_conf, log=logfile, tmp_dir=network_tmpdir)
        with open(realpath(expanduser(args.path_to_input_list)), 'r') as f:
            if args.n is None:
                inputs = [line.strip() for line in f]
            else:
                inputs = list(islice((line.strip() for line in f), 0, args.n))

        output = join(out_base, 'output.vds')
        rg = hl.get_reference('GRCh38')
        combiner = hl.vds.new_combiner(
            output_path=output,
            temp_path=combiner_tmpdir,
            gvcf_paths=inputs,
            vds_paths=input_vdses,
            use_genome_default_intervals=True,
            reference_genome=rg,
            branch_factor=args.branch_factor,
        )

        combiner.run()

        logging.info("Combiner completed")
        vds = hl.vds.read_vds(output)
        logging.info("Exporting VCF...")
        hl.methods.export_vcf(vds, join(out_base, 'output.vcf.bgz'))
        logging.info("Exporting VCF...: done!")
    except Exception as e:
        logging.warning(f"Caught exception: {type(e).__name__}({e})")
        if exists(out_base):
            logging.info(f"In exception: remove {out_base}")
            rmtree(out_base)
    finally:
        if exists(base_tmpdir):
            logging.info(f"Clean up: remove {base_tmpdir}")
            rmtree(base_tmpdir)
