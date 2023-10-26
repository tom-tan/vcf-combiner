# vcf-combiner

## Requirement
- SingularityCE or Apptainer
  - You can configure it via the `RUNTIME` environment variable
  - It must work with `fakeroot` to build a SIF container
- jq for `make status` (optional)
- Each worker node must have a pair of public and private keys in `~/.ssh` with standard file names such as `id_ecdsa` and `id_ecdsa.pub`.

## How to use this repository for joint calling

- Building a SIF image
  ```console
  $ make build
  ```
  - It build a SIF image `spark.sif` with spark runtime and hail.

- Make the `workers` file that enumerates the worker nodes.
  - See `workers.sample` for details.
  - The rest of the instructions uses `master-node` as a host name of the master node.

- Start a spark cluster
  ```console
  master-node $ make start
  ```
  - It starts daemons with the `$RUNTIME instance` command and starts the culster with `start-all.sh`.
  - It generates the `log` directory for worker logs and the `work` directory.
  - You can use `$EXTRA_ARGS` to pass extra arguments such as mount points. For example:
    ```console
    master-node $ make start EXTRA_ARGS="-B /path/to/extra/dir:/path/to/extra/dir"
    ```
  - You can see the status of the cluster in Web UI via port 8080.

- Run joint calling
  ```console
  master-node $ $RUNTIME exec instance://spark $PWD/combiner.py samples.txt --master spark://master-node:7077 --output-base=~/hail-logs/n105-d_16-e_32 --driver-memory=16g --executor-memory=32g
  ```
  - Run `$RUNTIME exec instance://spark $PWD/combiner.py -h` for other options.
  - You can run the joint calling in other nodes with the following cammand: `$RUNTIME exec spark.sif $PWD/combiner.py`. Note that make sure the required directories (e.g., a directory that stores VCF files) are specified with `-B` option. 

- Stop the cluster
  ```console
  master-node $ make stop
  ```

## Appendix
- `make help` shows other subcommands to operate the cluster

