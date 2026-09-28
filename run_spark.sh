#!/bin/bash
export JAVA_HOME=$(pwd)/.java17/Contents/Home
export PYTHONPATH=.
export PYSPARK_PYTHON=$(pwd)/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=$(pwd)/.venv/bin/python
.venv/bin/python spark/spark_etl.py
