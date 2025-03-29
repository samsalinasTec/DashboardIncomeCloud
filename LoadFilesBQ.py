import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import logging



def main():

    try:
        DeploymentAnalytics = pd.read_csv("./DeploymentAnalytics.csv")
        FCUsageMovements = pd.read_csv("./FCUsageMovements.csv")
        TeamDeployment = pd.read_csv("./TeamDeployment.csv")
        logging.info("CSV leídos correctamente.")
    except FileNotFoundError as e:
        logging.error(f"No se encontró alguno de los archivos CSV: {e}")
        return
    except pd.errors.EmptyDataError as e:
        logging.error(f"Alguno de los CSV está vacío o es inválido: {e}")
        return
    except Exception as e:
        logging.error(f"Error desconocido al leer los CSV: {e}")
        return

    try:
        # Ajusta el formato si difiere en cada CSV
        DeploymentAnalytics["REGISTRATION_DATE"] = pd.to_datetime(
            DeploymentAnalytics["REGISTRATION_DATE"], format='%Y-%m-%d'
        )
        FCUsageMovements["REGISTRATION_DATE"] = pd.to_datetime(
            FCUsageMovements["REGISTRATION_DATE"], format='%Y-%m-%d'
        )
        # Caso distinto en team_deployment, que venía con '%d/%m/%Y' en el script original
        TeamDeployment["REGISTRATION_DATE"] = pd.to_datetime(
            TeamDeployment["REGISTRATION_DATE"], format='%d/%m/%Y'
        )
        logging.info("Columnas de fecha convertidas correctamente.")
    except Exception as e:
        logging.error(f"Error al convertir columnas de fecha: {e}")
        return


    # Credenciales para las consultas a BigQuery
    KEY_FILE_LOCATION = "./BQProjectKey.json"
    try:
        credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logging.info("Conexión a BigQuery establecida.")
    except FileNotFoundError as e:
        logging.error(f"No se encontró el archivo de credenciales: {e}")
        return
    except Exception as e:
        logging.error(f"Error desconocido al conectar con BigQuery: {e}")
        return 

    tablesToDelete = [
        "sorteostec-analytics360.PruebasDashboardNacional.DMDeploymentAnalytics",
        "sorteostec-analytics360.PruebasDashboardNacional.FCUsageMovements",
        "sorteostec-analytics360.PruebasDashboardNacional.DMTeamDeployment"
    ]

    for table in tablesToDelete:
        try:
            client.delete_table(table)
            logging.info(f"Table {table} eliminada correctamente.")
        except Exception as e:
            logging.warning(f"No se pudo elminar la tabla {table} o no existe: {e}")





        # 4. Definir los esquemas de las nuevas tablas con los NUEVOS nombres de columna
        job_config_deployment_analytics = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("DEPLOYMENT_DAY_ID", "INTEGER"),
                bigquery.SchemaField("DEPLOYMENT_TEAM_ID", "INTEGER"),
                bigquery.SchemaField("DEPLOYMENT_ID", "INTEGER"),
                bigquery.SchemaField("TEAM_MEMBER_ID", "INTEGER"),
                bigquery.SchemaField("REGISTRATION_DATE", "DATE"),
                bigquery.SchemaField("VM_INSTANCES", "INTEGER"),
                bigquery.SchemaField("INSTANCE_REVENUE", "FLOAT")
            ]
        )

        job_config_fc_usage_movements = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("CLOUD_RETAILER", "STRING"),
                bigquery.SchemaField("DIGITAL_CHANNEL", "STRING"),
                bigquery.SchemaField("CLIENT_ID", "INTEGER"),
                bigquery.SchemaField("REGISTRATION_DATE", "DATE"),
                bigquery.SchemaField("VM_INSTANCES", "INTEGER"),
                bigquery.SchemaField("INSTANCE_REVENUE", "FLOAT"),
                bigquery.SchemaField("DEPLOYMENT_ID", "INTEGER"),
                bigquery.SchemaField("DEPLOYMENT_DAY_ID", "INTEGER"),
                bigquery.SchemaField("DEPLOYMENT_CLIENT_ID", "INTEGER")
            ]
        )

        job_config_team_deployment = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("REGISTRATION_DATE", "DATE"),
                bigquery.SchemaField("CLIENT_ID", "INTEGER"),
                bigquery.SchemaField("OFFICE_ID", "INTEGER"),
                bigquery.SchemaField("EMAIL", "STRING"),
                bigquery.SchemaField("FULL_NAME", "STRING")
            ]
        )

        # 5. Cargar dataframes a BigQuery
        try:
            client.load_table_from_dataframe(
                FCUsageMovements,
                "mycloud-analytics.ProjectData.FCUsageMovements",
                job_config=job_config_fc_usage_movements
            ).result()
            logging.info("FCUsageMovements subido correctamente a BigQuery.")

            client.load_table_from_dataframe(
                DeploymentAnalytics,
                "mycloud-analytics.ProjectData.DMDeploymentAnalytics",
                job_config=job_config_deployment_analytics
            ).result()
            logging.info("DeploymentAnalytics subido correctamente a BigQuery.")

            client.load_table_from_dataframe(
                TeamDeployment,
                "mycloud-analytics.ProjectData.DMTeamDeployment",
                job_config=job_config_team_deployment
            ).result()
            logging.info("TeamDeployment subido correctamente a BigQuery.")

        except Exception as e:
            logging.error(f"Ocurrió un error al cargar los datos a BigQuery: {e}")


# Punto de entrada principal
if __name__ == "__main__":
    main()