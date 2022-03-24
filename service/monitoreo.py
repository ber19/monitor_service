import time
from datetime import datetime
from googleapiclient.errors import HttpError
from scraping.scheduling_scraping import main_activo
from ops import set_dfs, data_ops
from datos import datos
from socket import timeout


def monitoreo(sheets, bot):
    try:
        while True:
            main_func(sheets, bot)
    except ConnectionResetError:
        print("- Ocurrio un error en la conexión con el servidor\n" +
        "\tIntentando de nuevo en 2 mins...")
        time.sleep(2*60)
        monitoreo(sheets, bot)
    except timeout:
        print("- Ocurrio un error al hacer una operacion para el drive\n" +
        "\tIntentando de nuevo en 35 segs...")
        time.sleep(35)
        monitoreo(sheets, bot)
    except HttpError: ## Por si ocurre un error al hacer una operacion del drive
        print("- Ocurrio un error al hacer una operacion para el drive\n" +
        "\tIntentando de nuevo en 2 mins...")
        time.sleep(2*60)
        monitoreo(sheets, bot)


def main_func(sheets, bot):
    '''Obtiene los datos del activo'''
    main_activo()
    '''Convierte los datos del activo a Dataframes'''
    set_dfs.set_dfs_a()
    set_dfs.set_df_total_activo()
    '''Obtiene los valores de la hoja negra y lo convierte a un DataFrame'''
    sheets.get_sheet_monitor()
    sheets.get_sheet_monitor_p()     
    set_dfs.set_df_monitor()
    set_dfs.set_df_monitor_p()     
    """Obtiene los valores de la hoja de cancelaciones y lo convierte a un DataFrame"""
    sheets.get_sheet_cancelados()
    set_dfs.set_df_cancelaciones()
    '''Obtiene los cancelados del activo'''
    data_ops.get_notoks()
    '''Obtiene los cancelados que son nuestros
        los coloca en la hoja de cancelaciones y
        envia las alertas por Telegram'''
    data_ops.get_delegated_notoks(sheets, bot)
    '''Envia la alerta si es que esta cargada la malla temporal'''
    # data_ops.get_temporal()     ##### DESCOMENTAR CUANDO SEA DIA DE MALLA TEMPORAL--------------
    '''Obtiene los jobs que son nuestros de la consulta'''
    data_ops.activo_set_date()
    '''Se envia un mensaje indicando que el bot esta listo'''
    data_ops.bot_ready()
    '''Monitoreo de bloques'''
    data_ops.set_bloques()
    data_ops.jobs_aviso()
    ######
    '''Revisa las etiquetas borradas y establece la variable de las tablas'''
    data_ops.check_removed()
    data_ops.set_tablas()
    data_ops.set_status_tablas()
    ######
    '''Establece los dias segun la consulta'''
    data_ops.set_days_a(datetime.today())    
    '''Reconoce las columnas del calendario (los dias)'''
    sheets.get_days_monitor()
    sheets.get_days_monitor_p()
    '''Obtiene los valores anteriores de los dias'''
    sheets.get_vals_cal()
    sheets.get_vals_cal_p()
    '''Establece los valores de las ejecuciones de los jobs
        para que funcionen con la hoja negra'''
    data_ops.ejec_by_day_a()
    '''Inserta las ejecuciones del activo en la hoja negra'''
    for m, n_day in datos.days_ejec_a:
        if m == "a":
            col_index = datos.days_monitor[n_day]
            sheets.set_ejec_by_days_a(
                col_index = col_index,
                n_day = n_day,
                mes = m
            )
        if m == "p":
            col_index = datos.days_monitor_p[n_day]
            sheets.set_ejec_by_days_a(
                col_index = col_index,
                n_day = n_day,
                mes = m
            )
    '''Coloca la fecha y hora de la ejecucion'''
    sheets.set_datetime_ejec()
    '''Se inicia un temporizador de 5 mins'''
    time.sleep(4 * 60)
    # time.sleep(10) # Para pruebas
