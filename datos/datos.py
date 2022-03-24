import pandas as pd

data_activo = {
    "ras": None,
    "deo": None,
    "ctk": None,
    "rig": None,
    "dco": None,
    "alm": None
}

###############################################################
dfs_activo = {
    "ras": None,
    "deo": None,
    "ctk": None,
    "rig": None,
    "dco": None,
    "alm": None
}
###
meses = {
    "01": "Enero",
    "02": "Febrero",
    "03": "Marzo",
    "04": "Abril",
    "05": "Mayo",
    "06": "Junio",
    "07": "Julio",
    "08": "Agosto",
    "09": "Septiembre",
    "10": "Octubre",
    "11": "Noviembre",
    "12": "Diciembre"
}
periodicidad = {
    "DIA" : "DIARIA",
    "MEN" : "MENSUAL",
    "SEM" : "SEMANAL"
}
def get_per(per):
    if per in periodicidad.keys():
        return periodicidad[per]
    else: return ""
###
###############################################################
data_monitor = None
df_monitor = None

data_monitor_p = None
df_monitor_p = None
####
spreadsheet_dict = None
sheets_id = {}
####
data_sheet_cancelaciones = None
df_sheet_cancelaciones = None
###############################
total_not_oks = pd.DataFrame() ##
notoks = pd.DataFrame()  ##

days_monitor = None
days_monitor_p = None     #############

jobs_activo = pd.DataFrame() ##
days_ejec_a = set()     
ejec_by_day_a = {}     ############# key ejemplos: ("a",4) o ("p", 11)
################################
days_values = None
days_values_p = None     #############
####
total_activo = pd.DataFrame() ##
####
delegated_notoks = pd.DataFrame()
####
last_update_activo = None
####
bot_ready = False
####
dfs_bloques = None  # Sera un diccionario
                    # key -> bloque (string)
                    # value -> filas de la hoja negra (dataframe)
status_bloques = {} # key -> bloque (string)
                    # value -> diccionario
                             # key -> job (string)
                             # value -> tag, status, tabla (list)
tablas = {} # key -> bloque (string)
            # value -> diccionario
                     # key -> tabla (string)
                     # value -> filas del monitor (dataframe)
                     #          jobs de la tabla  
status_tablas = {} # key -> bloque (string)
                   # value -> diccionario
                            # key -> tabla (string)
                            # value -> estados (lista de strings)
