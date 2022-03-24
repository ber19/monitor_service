import pandas as pd
import datos.datos as datos
import vars.vars as vars

def set_dfs_a():
    for uuaa in datos.data_activo:
        datos.dfs_activo[uuaa] = pd.DataFrame(
            datos.data_activo[uuaa],
            columns=vars.COLUMNS["activo"][0]
        )

def set_df_total_activo():
    datos.total_activo = pd.DataFrame()
    for values in datos.dfs_activo.values():
        datos.total_activo = datos.total_activo.append(values)

def set_df_monitor():
    datos.df_monitor = pd.DataFrame(
        datos.data_monitor,
        columns=vars.COLUMNS_MONITOR[0]
    )
    datos.df_monitor["Tabla Delegada?"] == datos.df_monitor["Tabla Delegada?"].str.strip()
    datos.df_monitor["JOB"] = datos.df_monitor["JOB"].str.strip()
    
def set_df_monitor_p():     #############
    datos.df_monitor_p = pd.DataFrame(
        datos.data_monitor_p,
        columns=vars.COLUMNS_MONITOR[0]
    )
    datos.df_monitor_p["Tabla Delegada?"] = datos.df_monitor_p["Tabla Delegada?"].str.strip()
    datos.df_monitor_p["JOB"] = datos.df_monitor_p["JOB"].str.strip()

def set_df_cancelaciones():  
    datos.df_sheet_cancelaciones = pd.DataFrame(
        datos.data_sheet_cancelaciones,
        columns=vars.COLUMNS_CANCELACIONES[0]
    )
    datos.df_sheet_cancelaciones["JOBNAME"] = datos.df_sheet_cancelaciones["JOBNAME"].str.strip()
    datos.df_sheet_cancelaciones["odate"] = datos.df_sheet_cancelaciones["odate"].str.strip()
    datos.df_sheet_cancelaciones["Fecha Cancelacion"] = datos.df_sheet_cancelaciones["Fecha Cancelacion"].str.strip()

