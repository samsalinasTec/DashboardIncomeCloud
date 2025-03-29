
# Importa las funciones de cada uno de los archivos
from TablesTransformModules import process_deployment_analytics
from TablesTransformModules import process_team_members
from TablesTransformModules import process_usage_movements
from TablesTransformModules import process_team_deployment


def main():
    try:
        print("[INFO] Iniciando proceso de deployment_analytics...")
        process_deployment_analytics()
    except Exception as e:
        print(f"[ERROR] Ocurrió un error en process_deployment_analytics: {e}")

    try:
        print("[INFO] Iniciando proceso de team_members...")
        process_team_members()
    except Exception as e:
        print(f"[ERROR] Ocurrió un error en process_team_members: {e}")

    try:
        print("[INFO] Iniciando proceso de usage_movements...")
        process_usage_movements()
    except Exception as e:
        print(f"[ERROR] Ocurrió un error en process_usage_movements: {e}")

    try:
        print("[INFO] Iniciando proceso de team_deployment...")
        process_team_deployment()
    except Exception as e:
        print(f"[ERROR] Ocurrió un error en process_team_deployment: {e}")

if __name__ == "__main__":
    main()