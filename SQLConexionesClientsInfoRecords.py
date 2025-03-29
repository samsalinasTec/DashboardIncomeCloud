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

def download_special_records():
    logging.info("Se leen credenciales para descarga del histórico de registros especiales")
    
    # Uso de variables de entorno para usuario y contraseña
    USER = os.environ.get('DB_USER')
    PASSWORD = os.environ.get('DB_PASS')
    
    if not USER or not PASSWORD:
        logging.error("Las credenciales no están definidas en las variables de entorno.")
        return

    try:
        # Inicialización del cliente Oracle y configuración del DSN (parámetros genéricos)
        cx_Oracle.init_oracle_client()
        dsn_tns = cx_Oracle.makedsn('DB_HOST', 'DB_PORT', service_name='DB_SERVICE')
        
        # Establecer conexión a la base de datos
        conn = cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn_tns)
        cursor = conn.cursor()
        logging.info("Inicia la descarga del histórico de registros especiales")
        
        # Intentar eliminar la tabla genérica (si existe) para luego crearla de nuevo
        try:
            cursor.execute("DROP TABLE generic_table PURGE")
            logging.info("Tabla generic_table eliminada exitosamente")
        except Exception as drop_err:
            logging.warning(f"No se pudo eliminar la tabla generic_table (posiblemente no existe): {drop_err}")
        
        # Creación de la tabla usando nombres genéricos y sin revelar detalles del negocio
        create_query = """
        CREATE TABLE generic_table AS
        SELECT /*+ PARALLEL(8) */ DISTINCT
               a.col1,
               a.col2,
               a.col3,
               a.col4
        FROM schema_a.table_a a
        WHERE a.fecha_cierre >= SYSDATE - INTERVAL '3' YEAR
        """
        cursor.execute(create_query)
        logging.info("Tabla generic_table creada exitosamente")
        
        # Inserción de datos complementarios en la tabla genérica
        insert_query = """
        INSERT INTO generic_table (
            SELECT /*+ PARALLEL(8) */ DISTINCT
                   b.col5 AS col1,
                   b.col6 AS col2,
                   b.col7 AS col3,
                   b.col8 AS col4
            FROM schema_b.table_b b
            WHERE NOT EXISTS (
                SELECT 1 FROM generic_table g WHERE g.col1 = b.col5
            )
        )
        """
        cursor.execute(insert_query)
        cursor.execute("COMMIT")
        logging.info("Datos insertados y comprometidos en generic_table")
        
        # Lectura de datos usando pandas (antes de cerrar la conexión)
        dfSpecialRecords = pd.read_sql_query("SELECT * FROM generic_table", conn)
        logging.info("Descarga de datos completada")
        
        # Guardar el DataFrame en un archivo CSV
        output_file = '/ruta/a/tu/dfHistoricoCatalogoSpecialRecords.csv'
        dfSpecialRecords.to_csv(output_file, header=True, index=False)
        logging.info("Información guardada correctamente en el CSV")
        
    except Exception as e:
        logging.error(f"Error durante el proceso: {e}")
    finally:
        try:
            cursor.close()
        except Exception as e_cursor:
            logging.error(f"Error al cerrar el cursor: {e_cursor}")
        try:
            conn.close()
        except Exception as e_conn:
            logging.error(f"Error al cerrar la conexión: {e_conn}")