"""Configuración de Spark para ejecutar el procesamiento localmente."""

import os
import sys

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Crear una sesión local con dos hilos y fechas interpretadas en UTC."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("BD3 - Procesamiento de telemetría")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
