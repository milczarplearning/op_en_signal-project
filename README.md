
## Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- opensignal_config.json
 |-- jobs/
 |   |-- etl_job.py
 |-- tests/
 |   |-- test_data/
 |   |-- test_opensignal_job.py
 |   Pipfile
 |   Pipfile.lock
```

## Running the ETL job

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--files configs/opensignal_config.json \
jobs/opensignal_config.py
```

Briefly, the options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--files configs/etl_config.json` - the (optional) path to any config file that may be required by the ETL job;
- `jobs/opensignal_job.py` - the Python module file containing the ETL job to execute.

Full details of all possible options can be found [here](http://spark.apache.org/docs/latest/submitting-applications.html). Note, that we have left some options to be defined within the job (which is actually a Spark application) - e.g. `spark.cores.max` and `spark.executor.memory` are defined in the Python script as it is felt that the job should explicitly contain the requests for the required cluster resources.


## Automated Testing

In order to test with Spark, we use the `pyspark` Python package, which is bundled with the Spark JARs required to programmatically start-up and tear-down a local Spark instance, on a per-test-suite basis (we recommend using the `setUp` and `tearDown` methods in `unittest.TestCase` to do this once per test-suite). Note, that using `pyspark` to run Spark is an alternative way of developing with Spark as opposed to using the PySpark shell or `spark-submit`.

Given that we have chosen to structure our ETL jobs in such a way as to isolate the 'Transformation' step into its own function (see 'Structure of an ETL job' above), we are free to feed it a small slice of 'real-world' production data that has been persisted locally - e.g. in `tests/test_data` or some easily accessible network directory - and check it against known results (e.g. computed manually or interactively within a Python interactive console session).

To execute the example unit test for this project run,

```bash
pipenv run python -m unittest tests/test_*.py
```

If you're wondering what the `pipenv` command is, then read the next section.

## Managing Project Dependencies using Pipenv

We use [pipenv](https://docs.pipenv.org) for managing project dependencies and Python environments (i.e. virtual environments). All direct packages dependencies (e.g. NumPy may be used in a User Defined Function), as well as all the packages used during development (e.g. PySpark, flake8 for code linting, IPython for interactive console sessions, etc.), are described in the `Pipfile`. Their **precise** downstream dependencies are described in `Pipfile.lock`.

### Installing Pipenv

To get started with Pipenv, first of all download it - assuming that there is a global version of Python available on your system and on the PATH, then this can be achieved by running the following command,

```bash
pip3 install pipenv
```

Pipenv is also available to install from many non-Python package managers. For example, on OS X it can be installed using the [Homebrew](https://brew.sh) package manager, with the following terminal command,

```bash
brew install pipenv
```

For more information, including advanced configuration options, see the [official pipenv documentation](https://docs.pipenv.org).

### Installing this Projects' Dependencies

Make sure that you're in the project's root directory (the same one in which the `Pipfile` resides), and then run,

```bash
pipenv install --dev
```

This will install all of the direct project dependencies as well as the development dependencies (the latter a consequence of the `--dev` flag).
