import string

## VARS PARA schedulingScraping
URL_JOBS_EN_EL_AJF = "http://150.100.216.64:8080/scheduling/ajF?filtro="

UUAAS_ACTIVO = {
    "ras": "RAS-MX-DATIO",
    "deo": "DEO-MX-DATIO",
    "ctk": "CTK-MX-DATIO",
    "rig": "RIG-MX-DATIO",
    "dco": "DCO-MX-DATIO",
    "alm": "MALM-MX-DATIO"
}

## VARS PARA GOOGLE SHEETS
VALUE_INPUT_OPTION = "USER_ENTERED"

COLUMNS = {
    "activo": [[
        "ORDER ID", "JOBNAME", "SCHEDTABLE", "APPLICATION", "SUB-APPLICATION",
        "ODATE", "START_TIME", "END_TIME", "HOST", "RUS AS", "STATUS"
    ]]
}

COLUMNS_MONITOR = [[      ## SI SE AGREGAN NUEVAS COLUMNAS EN LA HOJA NEGRA, REVISAR QUE COINCIDAN EN ESTA VARIABLE
    "JOB", "Responsable", "DESCRIPCION", "Persistencia Ctrl-M", "SubAplicativo", "Origen",
    "Periodicidad BUI 2.0", "Periodicidad Sofia", "Skynet", "Tipo", "Tabla Master", "Tabla Raw",
    "ESTATUS", "COMENTARIO", "Malla", "JobName", "Periodicidad", "Orden Ejecucion", "Campos Sensibles",
    "Tokenizada", "Mig-Nex-Art", "Fecha de aceptacion", "TIPO", "Entradas CdU", "Salidas CdU", 
    "Dependencia de Salida", "Insumos desde Legacy", "Modulo Legacy", "Tabla Delegada?", "Particion", "Ruta Master",
    "KEEP ACTIVE", "JOBTYPE", "CONDITIONS", 
    "IDC", "#", "TAG", "Contacto origen", "Contacto Destino", "Link documentacion", "Disparador msj"
]]

COLUMNS_CANCELACIONES = [[   ## SI SE AGREGAN NUEVAS COLUMNAS EN LA HOJA DE CANCELACIONES, REVISAR QUE COINCIDAN EN ESTA VARIABLE
    "OrderId", "BEX", "UUAA", "MALLA", "SUBAPLICATIVO", "JOBNAME", "STATUS", "odate", "Fecha Cancelacion",
    "Delegado?", "TIPO JOB", "MES", "Tabla", "PERIODICIDAD", "ERROR", "SOLUCION TACTICA", "SOLUCION ESTRATEGICA",
    "ESTATUS", "FECHA ATENCION", "Registrado PIBEX?", "MOTIVO", "DESCRIPCION JOB"
]]

NOTOKS_OPTIONS = ["Ended Not OK", "Ended Not OK/En Hold"]

def columns_index():
    cols = []
    for letra0 in string.ascii_uppercase:
        cols.append(letra0)
    for letra1 in string.ascii_uppercase:
            for letra2 in string.ascii_uppercase:
                cols.append(letra1+letra2)
    return cols

## VARS PARA EL BOT
token = "2023546848:AAF1GhAE8z1rEJKqTHJZ5oY163fEalTsJu4"
chat_mio = 1511338459
# chat_mio = 2041153332  ## De JuanMa
# chat_prueba = -542496411
chat_prueba = -1001544285071
timeout = 3.0
def send_message(chat_id, texto):
    return "https://api.telegram.org/bot2023546848:AAF1GhAE8z1rEJKqTHJZ5oY163fEalTsJu4/sendMessage?"+\
        "chat_id={}&text={}".format(
            chat_id, texto
        )
comandos = {
    "/last":  ##
    "Fecha y hora de la ultima actualizacion de los datos",
    "/cancelados": ##
    "Jobs delegados, cancelados, en el activo",
    "/activo <job>": ##
    "Datos del job, en el activo",
    "/monitor <job>": ##
    "Datos del job, en el monitor",
    "/temporal <status>": ##
    "Jobs con el status indicado, en malla temporal, en el activo\n"+\
        "<status> aceptados:\n" +\
            "cancelados\nnotok\nok\nwait\nex",
    "/bloques":  ##
    "Bloques registrados en el monitor",
    "/bloque <bloque>": ##
    "Informacion sobre el bloque indicado\n"+\
        "Para busqueda por mas de una palabra clave, tienen que ir\n"+\
            "separadas por ', ' (coma y un espacio)",
    "/bloqs":  ##
    "Bloques registrados en el monitor, en el activo",
    "/bloq <bloque>":  ##
    "Estatus en el activo de las tablas del bloque\n"+\
        "Para busqueda por mas de una palabra clave, tienen que ir\n"+\
            "separadas por ', ' (coma y un espacio)"
}

flag_temporal = False