from dateutil.relativedelta import relativedelta
from datetime import datetime


#############################
reqs = []
def body_reqs(reqs):
    return {
        "requests": reqs
    }
#############################
def insert_empty_row(sheetId):
    return {
        "insertDimension":{
            "range": {
                "sheetId": sheetId,
                "dimension": "ROWS",
                "startIndex": 1,
                "endIndex": 2
            },
            "inheritFromBefore": False
        }
    }

def insert_data(data):
    return {
        "value_input_option": "USER_ENTERED",
        "data": [
            {
                "range": "CANCELACIONES RIESGOS!A2",
                "majorDimension": "ROWS",
                "values": [
                    data
                ]
            }
        ],
        "includeValuesInResponse": True
    }

def set_ejec_day(fecha, col_index, datos, n_day, mes):
    if mes == "a":
        return {
            "value_input_option": "USER_ENTERED",
            "data": [
                {
                    "range": "Calendario Ejec {0}!{1}4".format(
                        fecha.strftime("%Y%m"),
                        col_index # col_index -> string
                        ),
                    "majorDimension": "ROWS",
                    "values": datos[(mes, n_day)] # n_day -> int
                }
            ],
            "includeValuesInResponse": True
        }
    elif mes == "p":
        return {
            "value_input_option": "USER_ENTERED",
            "data": [
                {
                    "range": "Calendario Ejec {0}!{1}4".format(
                        (fecha - relativedelta(months=1)).strftime("%Y%m"),
                        col_index # col_index -> string
                        ),
                    "majorDimension": "ROWS",
                    "values": datos[(mes, n_day)] # n_day -> int
                }
            ],
            "includeValuesInResponse": True
        }

def set_datetime_ejec(dt):
    return {
        "value_input_option": "USER_ENTERED",
        "data": [
            {
                "range": "Calendario Ejec {}!A1".format(
                    dt.strftime("%Y%m")
                ),
                "majorDimension": "COLUMNS",
                "values": [
                    ["Ultima actualización: "],
                    [],
                    [datetime.now().strftime("%d-%m-%Y %H:%M:%S")]
                ]
            }
        ],
        "includeValuesInResponse": True
    }