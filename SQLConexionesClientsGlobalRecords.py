import os
import numpy as np
import math as mt
import pandas as pd
import oracledb as cx_Oracle
import logging

# Configuración de logging
logging.basicConfig(
    filename='/ruta/a/tu/logfile.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def download_digital_records():
    """
    Descarga el histórico de registros digitales desde Oracle y guarda los datos en un archivo CSV.
    """
    logging.info("Se leen credenciales para descarga del histórico de registros digitales")
    
    # Uso de variables de entorno para credenciales
    USER = os.environ.get('DB_USER')
    PASSWORD = os.environ.get('DB_PASS')
    
    if not USER or not PASSWORD:
        logging.error("Credenciales no definidas en las variables de entorno.")
        return

    try:
        # Inicializar cliente Oracle y definir DSN con parámetros genéricos
        cx_Oracle.init_oracle_client()
        dsn_tns = cx_Oracle.makedsn('DB_HOST', 'DB_PORT', service_name='DB_SERVICE')
        
        logging.info("Inicia la descarga del histórico de registros digitales")
        
        # Conectar a la base de datos
        conn = cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn_tns)
        
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return

    try:
        # Consulta SQL genérica, con alias para no revelar información sensible
        query = """
        SELECT DISTINCT
            CLIENT_ID AS id_cliente,
            OFFICE_ID AS id_oficina,
            NAME AS nombre,
            LAST_NAME AS apellido_paterno,
            SECOND_LAST_NAME AS apellido_materno,
            EMAIL AS correo
        FROM
            schema_reports.view_digital_clients
        WHERE
            EMAIL IS NOT NULL
        ORDER BY
            CLIENT_ID
        """
        # Lectura de datos mediante pandas
        dfDigitalRecords = pd.read_sql_query(query, conn)
        logging.info("Consulta ejecutada exitosamente")
        
    except Exception as e:
        logging.error(f"Error al ejecutar la consulta o procesar los datos: {e}")
        return
    finally:
        try:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión: {e}")
    
    try:
        # Guardar el DataFrame en un archivo CSV
        output_file = '/ruta/a/tu/dfHistoricoDigitalRecords.csv'
        dfDigitalRecords.to_csv(output_file, header=True, index=False)
        logging.info("Información guardada correctamente en el CSV")
        
    except Exception as e:
        logging.error(f"Error al guardar el archivo CSV: {e}")