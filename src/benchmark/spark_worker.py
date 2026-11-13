"""Entrada local para SPARK-53759 en PySpark 4.0.1 / Python 3.12+ / Windows.

El worker original descarta la referencia al socket y no cierra explícitamente
su búfer. Se conserva la conexión hasta vaciar/cerrar el stream, como la solución
upstream: https://issues.apache.org/jira/browse/SPARK-53759
No se modifica la instalación de PySpark ni el worker de la demo habitual.
"""

import os

from pyspark.serializers import write_int
from pyspark.util import local_connect_and_auth
from pyspark.worker import main


def run():
    stream, connection = local_connect_and_auth(int(os.environ["PYTHON_WORKER_FACTORY_PORT"]),
                                                 os.environ["PYTHON_WORKER_FACTORY_SECRET"])
    try:
        write_int(os.getpid(), stream)
        stream.flush()
        main(stream, stream)
    finally:
        try:
            stream.close()  # flush explícito antes de cerrar el socket subyacente.
        finally:
            connection.close()


if __name__ == "__main__":
    run()
