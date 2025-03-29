# main.py

# 1. Importa las funciones o clases que necesites de cada archivo.
from SQLConexionesClientesCatalogoRecords import download_special_records
from SQLConexionesClientesDigRecords import download_digital_records
from SQLConexionesVentaFisi import download_contract_history

def main():
    """
    Función principal que orquesta la ejecución de los procesos 
    definidos en los tres módulos SQLConexiones...
    """
    try:
        print("[INFO] Iniciando proceso de Catalogo Records...")
        download_special_records()
    except Exception as e:
        print(f"[ERROR] Ocurrió un error en run_catalogo_records: {e}")

    try:
        print("[INFO] Iniciando proceso de Dig Records...")
        download_digital_records()
    except Exception as e:
        print(f"[ERROR] Ocurrió un error en run_dig_records: {e}")

    try:
        print("[INFO] Iniciando proceso de Venta Fisi...")
        download_contract_history()
    except Exception as e:
        print(f"[ERROR] Ocurrió un error en run_venta_fisi: {e}")

if __name__ == "__main__":
    main()