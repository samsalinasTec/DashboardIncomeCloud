# deployment_analytics.py
import numpy as np
import pandas as pd
from datetime import timedelta, datetime
import math as mt

def process_deployment_analytics():
    """
    Procesa datos históricos de uso y despliegues para generar un DataFrame
    que relaciona fechas con un identificador único para cada día de despliegue.
    """
    # Cargar archivos de datos históricos
    dfHistoricalUsagePhysical = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoVentaParticionado.csv")
    dfServiceInfo = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoProducto.csv")
    dfHistoricalAnalytics = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoDLAS.csv")


    # Convertir la fecha de registro a tipo fecha (solo la parte de la fecha)
    dfHistoricalUsagePhysical["REGISTRATION_DATE"] = pd.to_datetime(dfHistoricalUsagePhysical["REGISTRATION_DATE"], format='%Y-%m-%d').dt.date

    # Obtener la fecha mínima de registro por despliegue
    dfMinDatesDeployment = dfHistoricalUsagePhysical.groupby("DEPLOYMENT_ID")["REGISTRATION_DATE"].min().reset_index()

    # Fusionar con la información de cierre de despliegue
    dfMinDatesDeployment = pd.merge(dfMinDatesDeployment, dfServiceInfo[["DEPLOYMENT_ID", "CLOSURE_DATE"]], on="DEPLOYMENT_ID", how="left")
    dfMinDatesDeployment["CLOSURE_DATE"] = pd.to_datetime(dfMinDatesDeployment["CLOSURE_DATE"], format='%Y-%m-%d')

    # Función para crear un rango de fechas desde la fecha de registro hasta la fecha de cierre
    def create_date_range(row):
        return pd.date_range(start=row['REGISTRATION_DATE'], end=row['CLOSURE_DATE'])

    # Aplicar la función y guardar en la columna DATE_RANGE
    dfMinDatesDeployment['DATE_RANGE'] = dfMinDatesDeployment.apply(create_date_range, axis=1)
    dfMinDatesDeployment = dfMinDatesDeployment.explode('DATE_RANGE')
    dfMinDatesDeployment["DATE_RANGE"] = pd.to_datetime(dfMinDatesDeployment["DATE_RANGE"], format='%Y-%m-%d')

    # Calcular el offset en días (DAY_OFFSET) entre la fecha de cierre y cada fecha del rango
    dfMinDatesDeployment["DAY_OFFSET"] = (dfMinDatesDeployment["CLOSURE_DATE"] - dfMinDatesDeployment["DATE_RANGE"]).dt.days

    # Convertir la fecha (del rango) a un valor numérico (días desde 1899-12-30)
    dfMinDatesDeployment["REGISTRATION_DATE_NUM"] = (pd.to_datetime(dfMinDatesDeployment["DATE_RANGE"], format='%d/%m/%Y') - 
                                                     pd.Timestamp('1899-12-30')).dt.days

    # Convertir la columna de fecha de registro en el histórico de analytics a valor numérico
    dfHistoricalAnalytics["REGISTRATION_DATE_NUM"] = (pd.to_datetime(dfHistoricalAnalytics["FECHA_REGISTRO"], format='%Y-%m-%d') - 
                                                      pd.Timestamp('1899-12-30')).dt.days

    # Fusionar la data de fechas con los datos históricos de analytics
    dfHistoricalAnalytics = pd.merge(dfHistoricalAnalytics, 
                                     dfServiceInfo[["PRODUCT_ID", "EDITION_NUMBER", "DEPLOYMENT_ID"]],
                                     left_on=["PRODUCT_TYPE", "DEPLOYMENT_NUMBER"],
                                     right_on=["PRODUCT_ID", "EDITION_NUMBER"])
    dfDeploymentAnalytics = pd.merge(dfMinDatesDeployment, dfHistoricalAnalytics, 
                                     left_on=["REGISTRATION_DATE_NUM", "DEPLOYMENT_ID"],
                                     right_on=["REGISTRATION_DATE_NUM", "DEPLOYMENT_ID"], how="left")

    # Crear identificador único para cada día de despliegue
    dfDeploymentAnalytics["DEPLOYMENT_DAY_ID"] = (dfDeploymentAnalytics["REGISTRATION_DATE_NUM"].astype(str) + 
                                                  dfDeploymentAnalytics["DEPLOYMENT_ID"].astype(str)).astype(np.int64)

    # Eliminar columnas no necesarias
    dfDeploymentAnalytics = dfDeploymentAnalytics.drop(["REGISTRATION_DATE_NUM", "CLOSURE_DATE", "DATE_RANGE", 
                                                        "PRODUCT_ID", "EDITION_NUMBER", "PRODUCT_TYPE", "DEPLOYMENT_NUMBER"], axis=1)

    # Verificar unicidad de los identificadores
    if dfDeploymentAnalytics['DEPLOYMENT_DAY_ID'].nunique() == len(dfDeploymentAnalytics):
        print("Todos los IDs de despliegue son únicos.")
    else:
        raise Warning("¡IDs duplicados, cuidado!")

    # Guardar el DataFrame resultante a CSV
    dfDeploymentAnalytics.to_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/TablasDMyFC/DeploymentAnalytics.csv", header=True, index=False)


# team_members.py

def process_team_members():
    """
    Une datos de equipo provenientes de diferentes fuentes y genera un consolidado de clientes.
    """
    dfTeamOffices = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfColabsOficina.csv")
    dfTeamSpecial = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoColabsEspeciales.csv")
    dfTeamCatalog = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoCatalogoColabs.csv")

    # Se asume que en estos archivos se han renombrado:
    # "ID_COLAB" → "TEAM_MEMBER_ID" y "TIPO_COLABORADOR" → "TEAM_MEMBER_TYPE"
    dfTeamMembers = pd.merge(dfTeamCatalog, dfTeamOffices, on="TEAM_MEMBER_ID", how="left")
    dfTeamMembers = pd.merge(dfTeamMembers, dfTeamSpecial, on="TEAM_MEMBER_ID", how="left")
    dfTeamMembers["TEAM_MEMBER_TYPE"] = dfTeamMembers["TEAM_MEMBER_TYPE"].fillna("TEAM_MEMBER")

    # Guardar la información consolidada de miembros de equipo
    dfTeamMembers.to_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/TablasDMyFC/TeamMembers.csv", header=True, index=False)


# fc_usage_movements.py
import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta

def process_usage_movements():
    """
    Transforma y actualiza los datos de movimientos de uso (movimientos de despliegue)
    y los concatena con la data histórica.
    """
    print("Iniciando transformación de FCUsageMovements")

    dfHistoricalMovements = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoMovimientosContrat.csv")
    FCUsageMovements = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/TablasDMyFC/FCMovimientos_contratos.csv")

    # Cargar la última fecha de captura (si existe) desde un archivo JSON
    try:
        with open(r'/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/VariablesEntorno/ultimaFechaRegistroAnterior.json', 'r') as f:
            lastCaptureDate = json.load(f)
    except FileNotFoundError:
        print("Archivo de fecha no encontrado. No se agregará ningún registro nuevo de movimientos")
        lastCaptureDate = 0

    if lastCaptureDate != 0:
        lastCaptureDateParsed = pd.to_datetime(lastCaptureDate, format='%Y-%m-%d')
        # Excluir un registro particular
        dfHistoricalMovements = dfHistoricalMovements.loc[dfHistoricalMovements["USAGE_ID"] != 270407]
        dfHistoricalMovements["CAPTURE_DATE"] = pd.to_datetime(dfHistoricalMovements["FECHACAPTURA"], format='%Y-%m-%d')
        dfMovementsUpdate = dfHistoricalMovements.loc[dfHistoricalMovements["CAPTURE_DATE"] == lastCaptureDateParsed]

        # Convertir la fecha de captura a valor numérico
        dfMovementsUpdate["SUPPORT_DATE_NUM"] = (dfMovementsUpdate['CAPTURE_DATE'] - pd.Timestamp('1899-12-30')).dt.days

        # Fusionar con información de despliegues
        dfServiceInfo = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardNacionalAnalytics/dataframesPreparacion/dfHistoricoSorteos.csv")
        dfServiceInfo.rename(columns={"ID_SORTEO": "DEPLOYMENT_ID", "FECHA_CIERRE": "CLOSURE_DATE",
                                      "ID_PRODUCTO": "PRODUCT_ID", "NUMERO_EDICION": "EDITION_NUMBER"}, inplace=True)
        dfMovementsUpdate = pd.merge(dfMovementsUpdate, 
                                     dfServiceInfo[["PRODUCT_ID", "EDITION_NUMBER", "DEPLOYMENT_ID"]],
                                     on=["PRODUCT_ID", "EDITION_NUMBER"], how="left")
        dfMovementsUpdate["DEPLOYMENT_DAY_ID"] = (dfMovementsUpdate["SUPPORT_DATE_NUM"].astype(str) + 
                                                  dfMovementsUpdate["DEPLOYMENT_ID"].astype(str)).astype(np.int64)
        dfMovementsUpdate["DEPLOYMENT_TEAM_ID"] = (dfMovementsUpdate["TEAM_MEMBER_ID"].astype(str) + 
                                                   dfMovementsUpdate["DEPLOYMENT_ID"].astype(str)).astype(np.int64)
        dfMovementsUpdate = dfMovementsUpdate.drop(["SUPPORT_DATE_NUM", "PRODUCT_ID", "EDITION_NUMBER"], axis=1)

        dfMovementsUpdate = dfMovementsUpdate.sort_values(["USAGE_ID", "DEPLOYMENT_ID", "CAPTURE_DATE"])

        # Identificar reubicaciones
        dfMovementsUpdate['Is_Reallocation'] = (
            (dfMovementsUpdate['MOVEMENT_TYPE_ID'] == 1) &
            (dfMovementsUpdate['MOVEMENT_TYPE_ID'].shift() == 2) &
            (dfMovementsUpdate['USAGE_ID'] == dfMovementsUpdate['USAGE_ID'].shift()) &
            (dfMovementsUpdate['DEPLOYMENT_ID'] == dfMovementsUpdate['DEPLOYMENT_ID'].shift())
        )

        dfMovementsUpdate['MOVEMENT_DESCRIPTION'] = np.where(
            dfMovementsUpdate['Is_Reallocation'], 'REALLOCATION',
            np.where(dfMovementsUpdate['MOVEMENT_TYPE_ID'] == 1, 'ALLOCATION', 'RETURN')
        )
        dfMovementsUpdate = dfMovementsUpdate.drop("Is_Reallocation", axis=1)

        FCUsageMovements["CAPTURE_DATE"] = pd.to_datetime(FCUsageMovements["FECHACAPTURA"], format='%Y-%m-%d')
        FCUsageMovements = pd.concat([FCUsageMovements, dfMovementsUpdate])

        FCUsageMovements.to_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/TablasDMyFC/FCUsageMovements.csv", header=True, index=False)

        # Reiniciar la última fecha de captura
        lastCaptureDate = 0
        with open(r'/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/VariablesEntorno/ultimaFechaRegistroAnterior.json', 'w') as f:
            json.dump(lastCaptureDate, f)
    else: 
        print("El archivo de fecha indica 0. Aún no se ejecutó la consulta de actualización.")


# team_deployment.py
import numpy as np
import pandas as pd

def process_team_deployment():
    """
    Consolida información de miembros de equipo asociados a despliegues,
    utilizando la salida de FCUsageMovements y otros datos históricos.
    """
    dfHistoricalUsagePhysical = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoVentaFisi.csv")
    dfServiceInfo = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoSorteos.csv")
    dfHistoricalSelfUsage = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoAutoconsumoFisi.csv")

    # Se asume que en dfHistoricalUsagePhysical y dfServiceInfo ya se han renombrado las columnas correspondientes
    dfServiceInfo.rename(columns={"ID_SORTEO": "DEPLOYMENT_ID", "FECHA_CIERRE": "CLOSURE_DATE",
                                  "ID_PRODUCTO": "PRODUCT_ID", "NUMERO_EDICION": "EDITION_NUMBER"}, inplace=True)
    dfHistoricalUsagePhysical.rename(columns={"FECHAREGISTRO": "REGISTRATION_DATE"}, inplace=True)

    dfHistoricalMovements = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/dataframesPreparacion/dfHistoricoMovimientosTal.csv")
    dfHistoricalMovements = pd.merge(dfHistoricalMovements, 
                                     dfServiceInfo[["DEPLOYMENT_ID", "PRODUCT_ID", "EDITION_NUMBER"]],
                                     on=["PRODUCT_ID", "EDITION_NUMBER"], how="left")
    dfHistoricalMovementsGrouped = dfHistoricalMovements.groupby(["DEPLOYMENT_ID", "TEAM_MEMBER_ID", "MOVEMENT_TYPE_ID"]).size().reset_index(name="GROUP_SIZE")
    dfHistoricalMovementsGrouped = dfHistoricalMovementsGrouped.pivot_table(index=["DEPLOYMENT_ID", "TEAM_MEMBER_ID"],
                                                                            columns="MOVEMENT_TYPE_ID",
                                                                            values="GROUP_SIZE",
                                                                            fill_value=0).reset_index().rename_axis(None, axis=1)
    dfHistoricalUsagePhysicalUniqTeam = dfHistoricalUsagePhysical[["DEPLOYMENT_ID", "TEAM_MEMBER_ID"]].drop_duplicates()
    dfHistoricalMovementsGrouped = pd.merge(dfHistoricalMovementsGrouped, dfHistoricalUsagePhysicalUniqTeam,
                                            on=["DEPLOYMENT_ID", "TEAM_MEMBER_ID"], how="left", indicator=True).rename(columns={"_merge": "RETURN_INTEGRAL"})
    dfHistoricalMovementsGrouped["RETURN_INTEGRAL"] = dfHistoricalMovementsGrouped["RETURN_INTEGRAL"].replace({'left_only': 'Yes', 'both': 'No'})
    dfHistoricalMovementsGrouped["TEAM_MEMBER_3_USAGE"] = np.where((dfHistoricalMovementsGrouped[1] >= 3), 'Yes', 'No')
    dfHistoricalMovementsGrouped = dfHistoricalMovementsGrouped.rename({1: "NUM_ALLOCATIONS", 2: "NUM_RETURNS"}, axis=1)

    dfHistoricalMovementsDroDuplic = pd.read_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/TablasDMyFC/FCUsageMovements.csv")[["DEPLOYMENT_ID", "TEAM_MEMBER_ID", "MOVEMENT_DESCRIPTION"]].drop_duplicates()
    dfHistoricalMovementsDroDuplic = dfHistoricalMovementsDroDuplic.loc[dfHistoricalMovementsDroDuplic["MOVEMENT_DESCRIPTION"] == "REALLOCATION"].rename(columns={"MOVEMENT_DESCRIPTION": "REALLOCATION"})
    dfHistoricalMovementsGrouped = pd.merge(dfHistoricalMovementsGrouped, dfHistoricalMovementsDroDuplic, on=["DEPLOYMENT_ID", "TEAM_MEMBER_ID"], how="left").fillna("No")
    dfHistoricalMovementsGrouped["REALLOCATION"] = dfHistoricalMovementsGrouped["REALLOCATION"].replace({"REALLOCATION": "Yes"})

    dfHistoricalUsagePhysical["REGISTRATION_DATE"] = pd.to_datetime(dfHistoricalUsagePhysical["REGISTRATION_DATE"], format='%Y-%m-%d').dt.date
    dfHistoricalUsagePhysical = dfHistoricalUsagePhysical.dropna(subset=["REGISTRATION_DATE"])
    dfHistoricalUsagePhysical = dfHistoricalUsagePhysical.drop_duplicates(subset=["DEPLOYMENT_ID", "TEAM_MEMBER_ID"]).reset_index(drop=True)
    dfHistoricalUsagePhysical = pd.merge(dfHistoricalUsagePhysical, dfServiceInfo[["DEPLOYMENT_ID", "PRODUCT_ID", "EDITION_NUMBER"]],
                                         on="DEPLOYMENT_ID", how="left")

    dfHistoricalRetention = dfHistoricalUsagePhysical[["DEPLOYMENT_ID", "PRODUCT_ID", "EDITION_NUMBER", "REGISTRATION_DATE", "TEAM_MEMBER_ID"]].sort_values(["TEAM_MEMBER_ID", "PRODUCT_ID", "EDITION_NUMBER"])
    dfHistoricalRetention['EDITION_DIFF'] = dfHistoricalRetention.groupby('TEAM_MEMBER_ID')['EDITION_NUMBER'].diff()
    dfHistoricalRetention['SAME_PRODUCT'] = dfHistoricalRetention['PRODUCT_ID'].eq(dfHistoricalRetention.groupby('TEAM_MEMBER_ID')['PRODUCT_ID'].shift(1))

    def categorize_client(row):
        if pd.isna(row['EDITION_DIFF']):
            return 'New'
        elif row['SAME_PRODUCT']:
            if row['EDITION_DIFF'] == 1:
                return 'Retained'
            elif 1 < row['EDITION_DIFF'] <= 10:
                return 'Reactivated'
            else:
                return 'New'
        else:
            return 'New'

    dfHistoricalRetention['RETENTION'] = dfHistoricalRetention.apply(categorize_client, axis=1)
    dfHistoricalRetention = pd.merge(dfHistoricalMovementsGrouped, dfHistoricalRetention, on=["DEPLOYMENT_ID", "TEAM_MEMBER_ID"], how="outer")
    dfHistoricalRetention = dfHistoricalRetention.drop(["REGISTRATION_DATE", "EDITION_DIFF", "SAME_PRODUCT", "PRODUCT_ID", "EDITION_NUMBER"], axis=1)

    dfTeamDeployment = pd.merge(dfHistoricalRetention, dfHistoricalSelfUsage[["DEPLOYMENT_ID", "TEAM_MEMBER_ID", "AUTOCONSUMO"]],
                                on=["DEPLOYMENT_ID", "TEAM_MEMBER_ID"], how="outer")
    dfTeamDeployment["DEPLOYMENT_TEAM_ID"] = (dfTeamDeployment["TEAM_MEMBER_ID"].astype(str) + dfTeamDeployment["DEPLOYMENT_ID"].astype(str)).astype(np.int64)
    dfTeamDeployment = dfTeamDeployment.drop(["TEAM_MEMBER_ID", "DEPLOYMENT_ID"], axis=1)

    if dfTeamDeployment['DEPLOYMENT_TEAM_ID'].nunique() == len(dfTeamDeployment):
        print("Todos los IDs de despliegue-equipo son únicos.")
    else:
        raise Warning("¡IDs duplicados, cuidado!")

    dfTeamDeployment.to_csv("/home/stadmin/AutomatizacionScripts/DashboardIncomeCloud/TablasDMyFC/TeamDeployment.csv", header=True, index=False)




