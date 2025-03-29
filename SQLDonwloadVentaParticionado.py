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

def download_contract_history():
    """
    Descarga el histórico de contratos desde la base de datos Oracle
    y guarda la información en un archivo CSV.
    """
    logging.info("Se leen credenciales para descarga del histórico de contratos")
    
    # Uso de variables de entorno para usuario y contraseña
    USER = os.environ.get('DB_USER')
    PASSWORD = os.environ.get('DB_PASS')
    
    if not USER or not PASSWORD:
        logging.error("Las credenciales no están definidas en las variables de entorno.")
        return

    try:
        # Inicialización del cliente Oracle
        cx_Oracle.init_oracle_client()
        # Configuración de DSN (nombres de host, puerto y servicio genéricos)
        dsn_tns = cx_Oracle.makedsn('DB_HOST', 'DB_PORT', service_name='DB_SERVICE')
        
        logging.info("Inicia la descarga del histórico de contratos")
        FechaCierre = "SYSDATE - INTERVAL '3' YEAR"
        
        # Establecer conexión a la base de datos
        conn = cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn_tns)
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return

    try:
        # Consulta SQL con nombres genéricos para esquemas, tablas y columnas
        query = """
        WITH Producto_ AS (
            SELECT col1, col2, col3, col4
            FROM   SCHEMA1.TABLE1
            WHERE  fecha_cierre >= {fecha}
        ),
        Cliente_Con_Contrato AS (
            SELECT  
                s.col1 AS id_producto,
                b.col2 AS id_cliente,
                TRUNC(c.col3) AS fecha_registro,  
                COUNT(b.col4) AS cantidad_contratos,
                COUNT(b.col4) * s.col4 AS ingreso_por_contratos
            FROM   Producto_ s 
            INNER JOIN SCHEMA2.TABLE2 b ON s.col5 = b.col5
            LEFT JOIN SCHEMA2.TABLE3 c ON c.col6 = b.col6 AND c.col7 = b.col7
            WHERE  b.col8 IN (2,6) AND c.col9 != 34
            GROUP BY s.col1, s.col4, b.col2, TRUNC(c.col3)
    
            UNION ALL
    
            SELECT  
                s.col1 AS id_producto,
                b.col2 AS id_cliente,
                TRUNC(c.col3) AS fecha_registro,  
                COUNT(b.col4) AS cantidad_contratos,
                COUNT(b.col4) * s.col4 AS ingreso_por_contratos
            FROM   Producto_ s 
            INNER JOIN SCHEMA3.TABLE2 b ON s.col10 = b.col10
            LEFT JOIN SCHEMA3.TABLE3 c ON c.col11 = b.col11 AND c.col12 = b.col12
            WHERE  b.col13 IN (2,6) AND c.col14 != 34
            GROUP BY s.col1, s.col4, b.col2, TRUNC(c.col3)
        )
        SELECT * FROM Cliente_Con_Contrato
        """.format(fecha=str(FechaCierre))
    
        # Lectura de datos a través de pandas
        dfVentaGlobal = pd.read_sql_query(query, conn)
    
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
        output_file = '/ruta/a/tu/dfHistoricoVentaGlobal.csv'
        dfVentaGlobal.to_csv(output_file, header=True, index=False)
        logging.info("Información guardada correctamente en el histórico de contratos")
    except Exception as e:
        logging.error(f"Error al guardar el archivo CSV: {e}")