import requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
import vars.vars as vars
from datetime import datetime, time
from datos import datos
from requests.exceptions import ConnectionError, ChunkedEncodingError
import time as tm

def main_activo():
    print("{}".format(datetime.now()))
    # print("- Obteniendo datos (del activo)...")
    try:
        for uuaa, str_uuaa in vars.UUAAS_ACTIVO.items():
            datos.data_activo[uuaa] = activo(str_uuaa)
            print("- {0} del activo OK".format(uuaa))
        datos.last_update_activo = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except ConnectionError as err:
        print("- No se ha podido establecer conexion con el Scheduling\n"\
            "Revisar: Conexión a internet y conexión a la VPN\nIntentando de nuevo en 2 mins")
        tm.sleep(2 * 60)
        main_activo()
    except Timeout as err:
        print("- No se ha podido establecer conexion con el Scheduling\n"\
            "Revisar: Conexión a internet y conexión a la VPN\nIntentando de nuevo en 2 mins")
        tm.sleep(2 * 60)
        main_activo()
    except ChunkedEncodingError as err:
        print("- La conexión con el Scheduling se rompió\n"\
            "Revisar_ Conexión a internet y conexión a la VPN\nIntentando de nuevo en 2 mins")
        tm.sleep(2 * 60)
        main_activo()
def activo(uuaa):
    data = []
    url = vars.URL_JOBS_EN_EL_AJF + "{0}".format(uuaa)
    res = requests.get(url=url, timeout=10)
    soup = BeautifulSoup(res.content, "html.parser")
    table = soup.find("table", attrs= {"id":"tblEjec"})
    for row in table.find_all("tr")[1:]:
        data.append([value.getText() for value in row.find_all("td")])
    return data