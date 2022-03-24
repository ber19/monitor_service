import sys
from googleapiclient.errors import HttpError
from ops.sheets_services import set_spreadsheet
import traceback
import time
from service.monitoreo import monitoreo
from ops.bot import Bot
from telegram.error import NetworkError
import socket
from requests import exceptions


def main(sheets=None, bot=None):
    try:
        if not sheets:
            '''Establece los servicios con las Google Spreadsheets'''
            sheets = set_spreadsheet()
            '''Verifica si la hoja negra (la del monitor) existe'''
            sheets.check_sheet_monitor()
            '''Verifica si la hoja de cancelaciones existe'''
            sheets.check_sheet_cancelados()
        if not bot:
            '''Se inician los servicios para el bot de Telegram'''
            bot = Bot()
        '''Inicio del servicio de monitoreo'''
        monitoreo(sheets, bot)
    except HttpError as err: ## Por si ocurre un error al hacer la instancia del drive
        print("- Ocurrio un error la instancia del drive\n"+
        "\tPor favor\nrevise la conexion a internet del servidor y"+
        "el drive")
        sys.exit()
    except NetworkError as err:
        print("- Ocurrio un error de conexion del bot con Telegram\n"+
        "\tIntentando de nuevo en 2 mins")
        time.sleep(2 * 60)
        main(sheets=sheets) ## Se vuelve a instanciar al bot
    except ConnectionAbortedError:
        print("- Ocurrio un error de conexion\n"+
        "\tIntentando de nuevo en 2 mins")
        time.sleep(2 * 60)
        main(sheets=sheets, bot=bot)
    except socket.gaierror:
        print("- Ocurrio un error de conexion\n"+
        "\tIntentando de nuevo en 2 mins")
        time.sleep(2 * 60)
        main(sheets=sheets, bot=bot)
    except exceptions.ConnectionError:
        print("- Ocurrio un error de conexion del bot con Telegram\n"+
        "\tIntentando de nuevo en 2 mins")
        time.sleep(2 * 60)
        main(sheets=sheets, bot=bot)
    except ConnectionResetError:
        print("- Ocurrio un error de conexion\n"+
        "\tIntentando de nuevo en 2 mins")
        time.sleep(2 * 60)
        main(sheets=sheets, bot=bot)
    except Exception as err:
        if bot:
            bot.updater.stop()
        print("- Error inesperado.\n- Info:\n", err,
        "\n- Cerrando ...")
        traceback.print_exc() # Comentar en produccion
        sys.exit()
    except KeyboardInterrupt:
        if bot:
            bot.updater.stop()
        print("- Ejecucion interrumpida")
        sys.exit()
    
if __name__ == "__main__":
    main()
