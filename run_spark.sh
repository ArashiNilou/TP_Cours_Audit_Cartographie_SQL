#!/bin/bash
export PYTHONPATH=.

# Utilise l'environnement virtuel local si existant, sinon prévient l'utilisateur
if [ -d ".venv" ]; then
    export PYSPARK_PYTHON=$(pwd)/.venv/bin/python
    export PYSPARK_DRIVER_PYTHON=$(pwd)/.venv/bin/python
    PYTHON_CMD=".venv/bin/python"
else
    echo "️ Attention : L'environnement .venv n'a pas été trouvé. Veuillez exécuter 'python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt'."
    exit 1
fi

# Configuration Java : on utilise la version locale .java17 SEULEMENT si elle existe (macOS), sinon on utilise le JAVA_HOME du système
if [ -d ".java17/Contents/Home" ]; then
    export JAVA_HOME=$(pwd)/.java17/Contents/Home
elif [ -z "$JAVA_HOME" ]; then
    echo "️ Attention : JAVA_HOME n'est pas défini. PySpark nécessite Java 11 ou 17."
fi

echo " Lancement du pipeline PySpark..."
$PYTHON_CMD spark/spark_etl.py
