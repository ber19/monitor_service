from datos import datos
from vars import vars
import pandas as pd
from dateutil.relativedelta import relativedelta
import requests
from copy import deepcopy


def get_notoks():
    datos.total_not_oks = pd.DataFrame()
    for uuaa_dfs in datos.dfs_activo.values():
        local_notoks = uuaa_dfs[uuaa_dfs["STATUS"].isin(vars.NOTOKS_OPTIONS)]
        datos.total_not_oks = datos.total_not_oks.append(local_notoks)
    datos.total_not_oks["ODATE"] = datos.total_not_oks["ODATE"].astype("int64")
    datos.total_not_oks["END_TIME"] = datos.total_not_oks["END_TIME"].astype("int64")
    datos.total_not_oks.sort_values(by=["ODATE","END_TIME"], inplace=True)


''''''
def get_delegated_notoks(sheets, bot):
    datos.notoks = pd.DataFrame()
    datos.delegated_notoks = pd.DataFrame()
    for index in range(len(datos.total_not_oks)):
        job_not_ok = datos.total_not_oks.iloc[index]
        ## El row monitor se saca de la hoja del mes actual (mas actualizada)
        row_monitor = datos.df_monitor.loc[datos.df_monitor["JOB"] == job_not_ok.JOBNAME]
        if row_monitor.empty == False:
            var1 = row_monitor.iloc[0]
            if var1["Tabla Delegada?"] == "DELEGADA" or var1["Tabla Delegada?"] == "EN PROCESO":
                datos.notoks = datos.notoks.append(job_not_ok)
                if not check_cancelado_exists(job_not_ok):  ## Verifica si el cancelado ya esta registrado en la hoja de cancelaciones
                    job_monitor = row_monitor[["JOB", "DESCRIPCION", "Malla", "Tabla Master", "TIPO"]]
                    job_activo = pd.DataFrame().append(job_not_ok)
                    sheets.insert_empty_row()
                    cancelados_sheet_insert(sheets, row_monitor, job_activo)
            if var1["Tabla Delegada?"] == "DELEGADA":
                datos.delegated_notoks = datos.delegated_notoks.append(job_not_ok)  ## Para el comando /cancelados
                if not check_cancelado_exists(job_not_ok):  ## Verifica si el cancelado ya esta registrado en la hoja de cancelaciones
                    job_activo["START_TIME"] = int(job_activo["START_TIME"]) ## Para el problema de los auto floats de pandas
                    job_activo["ODATE"] = int(job_activo["ODATE"])
                    job_activo["END_TIME"] = int(job_activo["END_TIME"])
                    r_monitor = row_monitor.iloc[0]
                    r_activo = job_activo.iloc[0] 
                    texto = ("*** NUEVO CANCELADO ***\n*Status:\n{0}\n*Job:\n{1}\n*Odate:\n{2}\n" + \
                            "*Descripcion:\n{3}\n*Start time:\n{4}\n*End time:\n{5}").format(
                                                                        r_activo["STATUS"],
                                                                        r_monitor["JOB"],
                                                                        r_activo["ODATE"],
                                                                        r_monitor["DESCRIPCION"],
                                                                        r_activo["START_TIME"],
                                                                        r_activo["END_TIME"]
                                                                    )
                    requests.get(vars.send_message(
                        vars.chat_prueba, texto))

def get_temporal():
    if veces_temporal() and not vars.flag_temporal:
        texto = "Se encontraron jobs\nde la malla CR-MXBEXTMP-T01\ncargados en el activo\n"
        requests.get(vars.send_message(
            vars.chat_prueba, texto
        ))
        vars.flag_temporal = True
    elif not veces_temporal() and vars.flag_temporal:
        vars.flag_temporal = False
    else: pass

##$$$$$$$$$$$$$
def set_bloques():
    datos.dfs_bloques = {}  ## En cada ciclo del monitoreo, se actualiza completamente
    for index in range(len(datos.df_monitor)):
        row_monitor = datos.df_monitor.iloc[index]
        if not row_monitor["Disparador msj"] == None or row_monitor["Disparador msj"] == "" or row_monitor["Disparador msj"] == " ":
            if not job_in_block_exists(row_monitor):
                try: 
                    datos.dfs_bloques[row_monitor["Disparador msj"]] =\
                    datos.dfs_bloques[row_monitor["Disparador msj"]].append(row_monitor)
                except KeyError:
                    datos.dfs_bloques[row_monitor["Disparador msj"].strip()] = \
                        pd.DataFrame().append(row_monitor)
                try:
                    #                   |          bloque             |     job          |
                    datos.status_bloques[row_monitor["Disparador msj"]][row_monitor["JOB"]] = \
                    datos.status_bloques[row_monitor["Disparador msj"]][row_monitor["JOB"]]
                except KeyError:
                    if not row_monitor["TAG"] == "" or row_monitor["TAG"] == " ":
                        try:
                            datos.status_bloques[row_monitor["Disparador msj"]][row_monitor["JOB"]] = [row_monitor["TAG"] ,"Sin estatus", row_monitor["Tabla Master"]] 
                        except KeyError:
                            datos.status_bloques[row_monitor["Disparador msj"].strip()] = {}
                            datos.status_bloques[row_monitor["Disparador msj"].strip()][row_monitor["JOB"]] = [row_monitor["TAG"], "Sin estatus", row_monitor["Tabla Master"]] 
                    else:
                        try:
                            datos.status_bloques[row_monitor["Disparador msj"]][row_monitor["JOB"]] = [row_monitor["JOB"], "Sin estatus", row_monitor["Tabla Master"]] 
                        except KeyError:
                            datos.status_bloques[row_monitor["Disparador msj"].strip()] = {}
                            datos.status_bloques[row_monitor["Disparador msj"].strip()][row_monitor["JOB"]] = [row_monitor["JOB"], "Sin estatus", row_monitor["Tabla Master"]] 
                    

def check_removed():
    for i in range(len(datos.df_monitor)):
        row_monitor = datos.df_monitor.iloc[i]
        for bloque_str, jobs in deepcopy(datos.status_bloques).items():
            if row_monitor["JOB"] in jobs.keys():
                if row_monitor["Disparador msj"] == None or row_monitor["Disparador msj"] == "" or row_monitor["Disparador msj"] == " ":
                    del datos.status_bloques[bloque_str][row_monitor["JOB"]]
                    if len(datos.status_bloques[bloque_str]) == 0:
                        del datos.status_bloques[bloque_str]


def set_tablas():
    datos.tablas = {}
    for i in range(len(datos.df_monitor)):
        row_monitor = datos.df_monitor.iloc[i]
        for bloque, jobs in datos.status_bloques.items():
            for job_list_data in jobs.values():
                if row_monitor["Tabla Master"] == job_list_data[2]:
                    try: 
                    #   |     bloque        |      tabla     |
                        datos.tablas[bloque][job_list_data[2]] = datos.tablas[bloque][job_list_data[2]].append(row_monitor)
                    except KeyError:
                        try:
                            datos.tablas[bloque][job_list_data[2]] = pd.DataFrame().append(row_monitor)
                        except KeyError:
                            datos.tablas[bloque] = {}
                            datos.tablas[bloque][job_list_data[2]] = pd.DataFrame().append(row_monitor)

def set_status_tablas():
    datos.status_tablas = {}
    for bloque, tablas in  datos.tablas.items():
        for tabla_str, tabla_df in tablas.items():
            for i in range(len(tabla_df)):
                row_monitor = tabla_df.iloc[i]
                job_activo = datos.total_activo.loc[
                    datos.total_activo["JOBNAME"] == row_monitor["JOB"]
                ]
                if not job_activo.empty:
                    var1 = job_activo.iloc[0]
                    try:
                        datos.status_tablas[bloque][tabla_str].append(var1["STATUS"])
                    except KeyError:
                        try:
                            datos.status_tablas[bloque][tabla_str] = []
                            datos.status_tablas[bloque][tabla_str].append(var1["STATUS"])
                        except KeyError:
                            datos.status_tablas[bloque] = {}
                            datos.status_tablas[bloque][tabla_str] = []
                            datos.status_tablas[bloque][tabla_str].append(var1["STATUS"])

def job_in_block_exists(row_monitor):
    if row_monitor["Disparador msj"] not in datos.dfs_bloques.keys():
        return False
    else:
        row = datos.dfs_bloques[row_monitor["Disparador msj"]].loc[
            datos.dfs_bloques[row_monitor["Disparador msj"]]["JOB"] == row_monitor["JOB"] 
        ]
        if row.empty:
            return False
        else: return True

def jobs_aviso():
    for bloque, filas in datos.dfs_bloques.items(): 
        for i in range(len(filas)):
            row = filas.iloc[i]     ## fila de la hoja negra dentro de un bloque
            if row["Tabla Delegada?"] == "DELEGADA":
                job_aviso_ok = datos.total_activo.loc[    ### BUSCA OK
                    (datos.total_activo["JOBNAME"] == row["JOB"]) &
                    (datos.total_activo["STATUS"] == "Ended OK")
                ]
                if not job_aviso_ok.empty:
                    var1 = job_aviso_ok.iloc[0]
                    if var1["JOBNAME"] in datos.status_bloques[bloque].keys():
                        if datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Sin estatus": # Por si el programa inicia cuando ya esta en "OK"
                            datos.status_bloques[bloque][var1["JOBNAME"]][1] = var1["STATUS"] # no envia mensaje
                        if datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Wait" or \
                        datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Executing":   ### AVISARA CUANDO PASE DE "Wait" o "Executing" a "Ok" 
                            datos.status_bloques[bloque][var1["JOBNAME"]][1] = var1["STATUS"]   
                            requests.get(vars.send_message(
                                vars.chat_prueba, texto_aviso(bloque)
                            ))
                    continue
                job_aviso_notok = datos.total_activo.loc[    ## BUSCA NOTOK
                    (datos.total_activo["JOBNAME"] == row["JOB"]) &
                    (datos.total_activo["STATUS"] == "Ended Not OK")
                ]
                if not job_aviso_notok.empty:
                    var1 = job_aviso_notok.iloc[0]
                    if var1["JOBNAME"] in datos.status_bloques[bloque].keys():
                        if datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Sin estatus": # Por si el programa inicia cuando ya esta en "Not OK"
                            datos.status_bloques[bloque][var1["JOBNAME"]][1] = var1["STATUS"] # no envia mensaje
                        if datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Wait" or \
                        datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Executing":   ### AVISARA CUANDO PASE DE "Wait" o "Executing" a "Ended Not OK"
                            datos.status_bloques[bloque][var1["JOBNAME"]][1] = var1["STATUS"]  
                            requests.get(vars.send_message(
                                vars.chat_prueba, texto_aviso(bloque)
                            ))
                    continue
                job_aviso_wait = datos.total_activo.loc[    ## BUSCA WAIT
                    (datos.total_activo["JOBNAME"] == row["JOB"]) &
                    (datos.total_activo["STATUS"].str.contains("Wait Condition ¿Que espera para ejecutar?"))
                ]
                if not job_aviso_wait.empty:   #######
                    var1 = job_aviso_wait.iloc[0]
                    if var1["JOBNAME"] in datos.status_bloques[bloque].keys():
                        if datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Sin estatus":
                            datos.status_bloques[bloque][var1["JOBNAME"]][1] = "Wait"
                    continue
                job_aviso_ex = datos.total_activo.loc[   ## BUSCA EXECUTING
                    (datos.total_activo["JOBNAME"] == row["JOB"]) &
                    (datos.total_activo["STATUS"].str.contains("Executing"))
                ]
                if not job_aviso_ex.empty:
                    var1 = job_aviso_ex.iloc[0]
                    if var1["JOBNAME"] in datos.status_bloques[bloque].keys():
                        if datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Sin estatus" or \
                        datos.status_bloques[bloque][var1["JOBNAME"]][1] == "Wait":  ### TIENE QUE PASAR DE "Wait" a "Executing"
                            datos.status_bloques[bloque][var1["JOBNAME"]][1] = "Executing" ## o de "Sin estatus" a "Executing"
                                                                                            ## por si el programa inicia cuando ya esta en Executing
                    continue
            

def texto_aviso(bloque):
    texto = f"*Ejec Bloque {bloque}*\n\n"
    for jobs in datos.status_bloques[bloque].values():
        texto += f"{jobs[0]}    {jobs[1]}\n"
    return texto

''''''
# Poner bien los datos que se van a poner en los registros 
#  en la hoja de cancelaciones
##OrderId,	BEX,	UUAA,	MALLA,	
# SUBAPLICATIVO,	JOBNAME,	STATUS,	
# odate,	Fecha Cancelación,	
# Delegado?,	TIPO JOB,	MES,	Tabla,	
# PERIODICIDAD,	ERROR, 	SOLUCIÓN TÁCTICA,	
# SOLUCIÓN ESTRATÉGICA,	ESTATUS,	FECHA ATENCIÓN,			
# "Registrado  PIBEX?",	MOTIVO
def cancelados_sheet_insert(sheets,r_monitor, r_activo):
    row_monitor = r_monitor.iloc[0]
    row_activo = r_activo.iloc[0]
    sheets.insert_data_cancelados(
        [row_activo["ORDER ID"], "Riesgos", 
        row_activo["JOBNAME"][:4], row_monitor["Malla"],
        row_monitor["SubAplicativo"], row_activo["JOBNAME"],
        row_activo["STATUS"], row_activo["ODATE"], row_activo["END_TIME"],
        row_monitor["Tabla Delegada?"], row_monitor["TIPO"], datos.meses[str(row_activo["END_TIME"])[4:6]],
        row_monitor["Tabla Master"], datos.get_per(row_monitor["Malla"][8:11]), "", "", "", "", "",
        "", ""
        ]
    )

def check_cancelado_exists(job_not_ok):
    job_s = datos.df_sheet_cancelaciones.loc[
        (datos.df_sheet_cancelaciones["JOBNAME"] == str(job_not_ok["JOBNAME"])) &
        (datos.df_sheet_cancelaciones["Fecha Cancelacion"] == str(job_not_ok["END_TIME"]))
    ]
    if job_s.empty:
        return False
    else:
        return True
                

###########
def activo_set_date():
    datos.jobs_activo = pd.DataFrame()
    for uuaa_dfs in datos.dfs_activo.values():
        for index in range(len(uuaa_dfs)):
            dele_job = uuaa_dfs.iloc[index]
            row_monitor = datos.df_monitor.loc[datos.df_monitor["JOB"] == dele_job.JOBNAME]
            if row_monitor.empty == False:
                var1 = row_monitor.iloc[0]
                if var1["Tabla Delegada?"] == "DELEGADA" or var1["Tabla Delegada?"] == "EN PROCESO":
                    datos.jobs_activo = datos.jobs_activo.append(dele_job)
    datos.jobs_activo["ODATE"] = datos.jobs_activo["ODATE"].str.strip()
    datos.jobs_activo["JOBNAME"] = datos.jobs_activo["JOBNAME"].str.strip()
    datos.jobs_activo["STATUS"] = datos.jobs_activo["STATUS"].str.strip()
    datos.jobs_activo.sort_values(by=["ODATE"], inplace=True, ascending=False)

def set_days_a(today):
    for index in range(len(datos.jobs_activo)):
        job = datos.jobs_activo.iloc[index]
        if job["ODATE"][2:4][0] == "0":
            if job["ODATE"][3:4] == str(today.month):
                datos.days_ejec_a.add(("a", int(job["ODATE"][4:6])))
            elif job["ODATE"][3:4] == str((today - relativedelta(months=1)).month):
                datos.days_ejec_a.add(("p", int(job["ODATE"][4:6])))
        if job["ODATE"].strip()[2:4] == str(today.month):
            datos.days_ejec_a.add(("a", int(job["ODATE"][4:6])))    
        elif job["ODATE"].strip()[2:4] == str((today - relativedelta(months=1)).month):
            datos.days_ejec_a.add(("p", int(job["ODATE"][4:6])))
    for m, day in datos.days_ejec_a:
        datos.ejec_by_day_a[(m, day)] = []    ############### Buscar aqui si hay cancelados del mes pasado

def ejec_by_day_a():
    def get_day(str_day):
        if len(str_day) == 1: return "0" + str_day
        else: return str_day
    for m, day in datos.ejec_by_day_a:
        if m == "a":
            for index in range(len(datos.df_monitor)):
                row_monitor = datos.df_monitor.iloc[index]     # Va iterando en la hoja del monitor
                job = datos.jobs_activo.loc[
                    (datos.jobs_activo["JOBNAME"] == row_monitor["JOB"]) &
                    (datos.jobs_activo["ODATE"].str.slice(4,6) == get_day(str(day)))
                ]
                if job.empty:
                    datos.ejec_by_day_a[(m, day)].append([])
                    continue
                else:
                    var1 = job.iloc[0]
                    col_index = day - 1
                    if datos.days_values:   ## Por si la hoja es nueva y no hay datos aun
                        # if datos.days_values[index]:  ## Aveces Google Sheets no devuelve todos los datos
                        try:
                            row_ejec_prev = datos.days_values[index]
                            if len(row_ejec_prev) <= col_index: 
                                dif = (col_index - len(row_ejec_prev)) + 1
                                for i in range(0, dif):
                                    row_ejec_prev.append("")  ## Se llenan los espacios vacios que puedan haber hasta la fecha actual
                            prev_status = row_ejec_prev[col_index]  ## Se obtiene la ejecucion del dia (columna)
                            curr_status = var1["STATUS"]
                            ## m servira para que despues se vayan colocando los valores en las hojas correspondientes (mes pasado o mes actual) 
                            if (prev_status == "C" and curr_status == "Ended OK") or \
                                (prev_status == "REX" and curr_status == "Ended OK") or \
                                    (prev_status == "ROK"): datos.ejec_by_day_a[(m, day)].append(["ROK"])
                            elif prev_status == "C" and curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["REX"])
                            elif (prev_status == "BAC" and curr_status == "Ended OK") or \
                                (prev_status == "BA" and curr_status == "Ended OK"):
                                    datos.ejec_by_day_a[(m, day)].append(["EVEOK"])
                            elif curr_status == "Ended OK": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Ended OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Wait Condition ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WAIT"])
                            elif curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["EX"])
                            elif curr_status == "Ended Not OK": datos.ejec_by_day_a[(m, day)].append(["C"])
                            elif curr_status == "Wait Workload/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WWH"])
                            elif curr_status == "Wait Condition/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BA"])
                            elif curr_status == "Ended Not OK/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BAC"])
                            elif curr_status == "Wait Condition/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WH"])
                            elif curr_status == "Executing/En Hold": datos.ejec_by_day_a[(m, day)].append(["EH"])
                            elif curr_status == "Ended Not OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["CH"])
                            else: datos.ejec_by_day_a[(m, day)].append([])
                        # else:
                        except IndexError:
                            curr_status = var1["STATUS"]
                            if curr_status == "Ended OK": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Ended OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Wait Condition ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WAIT"])
                            elif curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["EX"])
                            elif curr_status == "Ended Not OK": datos.ejec_by_day_a[(m, day)].append(["C"])
                            elif curr_status == "Wait Workload/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WWH"])
                            elif curr_status == "Wait Condition/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BA"])
                            elif curr_status == "Ended Not OK/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BAC"])
                            elif curr_status == "Wait Condition/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WH"])
                            elif curr_status == "Executing/En Hold": datos.ejec_by_day_a[(m, day)].append(["EH"])
                            elif curr_status == "Ended Not OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["CH"])
                            else: datos.ejec_by_day_a[(m, day)].append([])
                    else:
                        curr_status = var1["STATUS"]
                        if curr_status == "Ended OK": datos.ejec_by_day_a[(m, day)].append(["O"])
                        elif curr_status == "Ended OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["O"])
                        elif curr_status == "Wait Condition ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WAIT"])
                        elif curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["EX"])
                        elif curr_status == "Ended Not OK": datos.ejec_by_day_a[(m, day)].append(["C"])
                        elif curr_status == "Wait Workload/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WWH"])
                        elif curr_status == "Wait Condition/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BA"])
                        elif curr_status == "Ended Not OK/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BAC"])
                        elif curr_status == "Wait Condition/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WH"])
                        elif curr_status == "Executing/En Hold": datos.ejec_by_day_a[(m, day)].append(["EH"])
                        elif curr_status == "Ended Not OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["CH"])
                        else: datos.ejec_by_day_a[(m, day)].append([])
        elif m == "p":
            for index in range(len(datos.df_monitor_p)):
                row_monitor = datos.df_monitor_p.iloc[index]     # Va iterando en la hoja del monitor
                job = datos.jobs_activo.loc[
                    (datos.jobs_activo["JOBNAME"] == row_monitor["JOB"]) &
                    (datos.jobs_activo["ODATE"].str.slice(4,6) == get_day(str(day)))
                ]
                if job.empty:
                    datos.ejec_by_day_a[(m, day)].append([])
                    continue
                else:
                    var1 = job.iloc[0]
                    col_index = day - 1
                    if datos.days_values_p:   ## No tiene datos aun
                        # if datos.days_values_p[index]:  ## Si la fila tiene datos
                        try:
                            row_ejec_prev = datos.days_values_p[index]
                            if len(row_ejec_prev) <= col_index: 
                                dif = (col_index - len(row_ejec_prev)) + 1
                                for i in range(0, dif):
                                    row_ejec_prev.append("")  ## Se llenan los espacios vacios que puedan haber hasta la fecha actual
                            prev_status = row_ejec_prev[col_index]  ## Se obtiene la ejecucion del dia (columna)
                            curr_status = var1["STATUS"]
                            ## m servira para que despues se vayan colocando los valores en las hojas correspondientes (mes pasado o mes actual) 
                            if (prev_status == "C" and curr_status == "Ended OK") or \
                                (prev_status == "REX" and curr_status == "Ended OK") or \
                                    (prev_status == "ROK"): datos.ejec_by_day_a[(m, day)].append(["ROK"])
                            elif prev_status == "C" and curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["REX"])
                            elif (prev_status == "BAC" and curr_status == "Ended OK") or \
                                (prev_status == "BA" and curr_status == "Ended OK"):
                                    datos.ejec_by_day_a[(m, day)].append(["EVEOK"])
                            elif curr_status == "Ended OK": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Ended OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Wait Condition ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WAIT"])
                            elif curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["EX"])
                            elif curr_status == "Ended Not OK": datos.ejec_by_day_a[(m, day)].append(["C"])
                            elif curr_status == "Wait Workload/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WWH"])
                            elif curr_status == "Wait Condition/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BA"])
                            elif curr_status == "Ended Not OK/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BAC"])
                            elif curr_status == "Wait Condition/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WH"])
                            elif curr_status == "Executing/En Hold": datos.ejec_by_day_a[(m, day)].append(["EH"])
                            elif curr_status == "Ended Not OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["CH"])
                            else: datos.ejec_by_day_a[(m, day)].append([])
                        # else:
                        except IndexError:
                            curr_status = var1["STATUS"]
                            if curr_status == "Ended OK": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Ended OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["O"])
                            elif curr_status == "Wait Condition ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WAIT"])
                            elif curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["EX"])
                            elif curr_status == "Ended Not OK": datos.ejec_by_day_a[(m, day)].append(["C"])
                            elif curr_status == "Wait Workload/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WWH"])
                            elif curr_status == "Wait Condition/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BA"])
                            elif curr_status == "Ended Not OK/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BAC"])
                            elif curr_status == "Wait Condition/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WH"])
                            elif curr_status == "Executing/En Hold": datos.ejec_by_day_a[(m, day)].append(["EH"])
                            elif curr_status == "Ended Not OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["CH"])
                            else: datos.ejec_by_day_a[(m, day)].append([])
                    else:
                        curr_status = var1["STATUS"]
                        if curr_status == "Ended OK": datos.ejec_by_day_a[(m, day)].append(["O"])
                        elif curr_status == "Ended OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["O"])
                        elif curr_status == "Wait Condition ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WAIT"])
                        elif curr_status == "Executing": datos.ejec_by_day_a[(m, day)].append(["EX"])
                        elif curr_status == "Ended Not OK": datos.ejec_by_day_a[(m, day)].append(["C"])
                        elif curr_status == "Wait Workload/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WWH"])
                        elif curr_status == "Wait Condition/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BA"])
                        elif curr_status == "Ended Not OK/Eliminado del Activo": datos.ejec_by_day_a[(m, day)].append(["BAC"])
                        elif curr_status == "Wait Condition/En Hold ¿Que espera para ejecutar?": datos.ejec_by_day_a[(m, day)].append(["WH"])
                        elif curr_status == "Executing/En Hold": datos.ejec_by_day_a[(m, day)].append(["EH"])
                        elif curr_status == "Ended Not OK/En Hold": datos.ejec_by_day_a[(m, day)].append(["CH"])
                        else: datos.ejec_by_day_a[(m, day)].append([])
        else: raise LookupError



################################ FUNCIONES BOT
def bot_search_monitor(job):
    job_monitor = datos.df_monitor.loc[  ## Suponiendo que la hoja mas reciente del monitor siempre es la actualizada
        datos.df_monitor["JOB"] == job
    ]
    if job_monitor.empty:
        return "No se encontro el job en el monitor (hoja negra)"
    else:
        var1 = job_monitor.iloc[0]
        texto = ("*** /monitor {11} ****\n*Job:\n{0}\n*Descripcion:\n{1}\n*Persistencia Ctrl-M:\n{2}\n*Subaplicativo:\n{3}\n" + \
                    "*Origen:\n{4}\n*Tabla raw:\n{5}\n*Tabla master:\n{6}\n*Malla:\n{7}\n*Jobname:\n{8}\n" + \
                    "*Tipo:\n{9}\n*Delegada?:\n{10}").format(
                                                    var1["JOB"], var1["DESCRIPCION"], var1["Persistencia Ctrl-M"], var1["SubAplicativo"],
                                                    var1["Origen"], var1["Tabla Raw"], var1["Tabla Master"], var1["Malla"], var1["JobName"],
                                                    var1["TIPO"], var1["Tabla Delegada?"], job
                                                    )
        return texto
###
def veces_en_activo(job):
    job_en_activo = datos.total_activo.loc[
        datos.total_activo["JOBNAME"] == job
    ]
    if job_en_activo.empty: return False
    else: return (len(job_en_activo), job_en_activo)
def bot_search_activo(job, i):
    var1 = job.iloc[i]
    if not var1["START_TIME"].isnumeric():
        return ("*** /activo {5} ***\n*Job:\n{0}\n*Status:\n{1}\n*Odate:\n{2}\n*Start time:\n{3}\n*End time:\n{4}").format(
            var1["JOBNAME"], var1["STATUS"], var1["ODATE"], "No ha iniciado", "No ha terminado", var1["JOBNAME"]
        )
    elif var1["START_TIME"].isnumeric() and not var1["END_TIME"].isnumeric():
        return ("*** /activo {5} ***\n*Job:\n{0}\n*Status:\n{1}\n*Odate:\n{2}\n*Start time:\n{3}\n*End time:\n{4}").format(
            var1["JOBNAME"], var1["STATUS"], var1["ODATE"], var1["START_TIME"], "No ha terminado", var1["JOBNAME"]
        )
    else:
        return ("*** /activo {5} ***\n*Job:\n{0}\n*Status:\n{1}\n*Odate:\n{2}\n*Start time:\n{3}\n*End time:\n{4}").format(
            var1["JOBNAME"], var1["STATUS"], var1["ODATE"], var1["START_TIME"], var1["END_TIME"], var1["JOBNAME"]
        )
###
def return_bloque(bloque):
    try:
        block = datos.status_bloques[bloque]
        texto = f"* Bloque: {bloque} *\n\n"
        for job, data in block.items():
            texto += "*{}:\n  {}\n  {}\n".format(
                job, data[0], data[1]
            )
        return texto
    except KeyError:
        try:
            list_of_str = bloque.split(", ") ## El formato tiene que ser
                                                ## <palabra>, <palabra>, <palabra>, ...
            for bloq in datos.status_bloques.keys():
                if all(str1 in bloq for str1 in list_of_str):
                    block = datos.status_bloques[bloq]
                    texto = f"* Bloque: {bloq} *\n\n"
                    for job, data in block.items():
                        texto += "*{}:\n  {}\n  {}\n".format(
                            job, data[0], data[1]
                        )
                    return texto
            raise KeyError
        except KeyError:
            return "No hay datos sobre ese bloque"
###$$$$$$$$$$$$$$$$$$$$
def return_bloqp(bloque):
    try:
        block = datos.status_tablas[bloque]
        texto = f"* Bloque: {bloque} *\n\n"
        for tabla_str, status_list in block.items():
            if all("Ended OK" in status for status in status_list):
                texto += "* {}\n{}\n".format(
                    "OK", tabla_str
                )
            elif any("Ended Not" in status for status in status_list):
                texto += "* {}\n{}\n".format(
                    "NOTOK", tabla_str
                )
            elif any("Exe" in status for status in status_list):
                texto += "* {}\n{}\n".format(
                    "EXEC", tabla_str
                )
            elif any("Wait" in status for status in status_list):
                texto += "* {}\n{}\n".format(
                    "WAIT", tabla_str
                )
        return texto
    except KeyError:
        try:
            list_of_str = bloque.split(", ")
            for bloq in datos.status_tablas.keys():
                if all(str1 in bloq for str1 in list_of_str):
                    block = datos.status_tablas[bloq]
                    texto = f"* Bloque: {bloq} *\n\n"
                    for tabla_str, status_list in block.items():
                        if all("Ended OK" in status for status in status_list):
                            texto += "* {}\n{}\n".format(
                                "OK", tabla_str
                            )
                        elif any("Ended Not" in status for status in status_list):
                            texto += "* {}\n{}\n".format(
                                "NOTOK", tabla_str
                            )
                        elif any("Exe" in status for status in status_list):
                            texto += "* {}\n{}\n".format(
                                "EXEC", tabla_str
                            )
                        elif any("Wait" in status for status in status_list):
                            texto += "* {}\n{}\n".format(
                                "WAIT", tabla_str
                            )
                    return texto
            raise KeyError
        except KeyError:
            return "No hay datos sobre ese bloque"
###
def return_bloques():
    texto = f"* Bloques registrados *\n\n"
    for block in datos.status_bloques.keys():
        texto += "{}\n\n".format(
            block
        )
    return texto        

def return_bloqs():
    texto = f"* Bloques en el activo *\n\n"
    for block in datos.status_tablas.keys():
        texto += "{}\n\n".format(
            block
        )
    return texto
###
def veces_temporal():
    temporal_en_activo = datos.total_activo.loc[
        datos.total_activo["SCHEDTABLE"] == "CR-MXBEXTMP-T01"
    ]
    if temporal_en_activo.empty: return False
    else: return True
def veces_temporal_status(status):
    temporal_en_activo_status = datos.total_activo.loc[
        (datos.total_activo["SCHEDTABLE"] == "CR-MXBEXTMP-T01") &
        (datos.total_activo["STATUS"] == status)
    ]
    if temporal_en_activo_status.empty: return False
    else: return (len(temporal_en_activo_status), temporal_en_activo_status)
def bot_search_temporal(job, i, status_arg):
    var1 = job.iloc[i]
    if status_arg == "wait":
        return ("*** \\temporal {0} ***\n*Job:\n{1}\n*Start time:\n{2}\n*End time:\n{3}\n*Status:\n{4}\n").format(
            status_arg, var1["JOBNAME"], "Aun no ha iniciado", "Aun no ha terminado", var1["STATUS"]
        )
    elif status_arg == "Executing":
        return ("*** \\temporal {0} ***\n*Job:\n{1}\n*Start time:\n{2}\n*End time:\n{3}\n*Status:\n{4}\n").format(
            status_arg, var1["JOBNAME"], var1["START_TIME"], "Aun no ha terminado", var1["STATUS"]
        )
    else:
        return ("*** \\temporal {0} ***\n*Job:\n{1}\n*Start time:\n{2}\n*End time:\n{3}\n*Status:\n{4}\n").format(
            status_arg, var1["JOBNAME"], var1["START_TIME"], var1["END_TIME"], var1["STATUS"]
        )
###
def veces_cancelados():
    if len(datos.delegated_notoks) == 0:
        return False
    else: return True
def bot_search_cancelado(index):
    # Con el copy() se indica explicitamente que se hace una copia del subset seleccionado por el iloc
    cancel = datos.delegated_notoks.iloc[index].copy()
    cancel_monitor = datos.df_monitor.loc[
        datos.df_monitor["JOB"] == cancel["JOBNAME"]
    ]
    cancel["ODATE"] = int(cancel["ODATE"])  ## Para el error del auto float de pandas
    cancel["START_TIME"] = int(cancel["START_TIME"])
    cancel["END_TIME"] = int(cancel["END_TIME"])
    row_monitor = cancel_monitor.iloc[0]
    return ("*** /cancelados ***\n*Job:\n{0}\n*Descripcion:\n{1}\n*Status:\n{2}\n" + \
                "*Odate:\n{3}\n*Start time:\n{4}\n*End time\n{5}\n").format(
                    cancel["JOBNAME"], row_monitor["DESCRIPCION"], cancel["STATUS"],
                    cancel["ODATE"], cancel["START_TIME"], cancel["END_TIME"]
                )
###
def bot_ready():
    if not datos.bot_ready:
        requests.get(vars.send_message(
                        vars.chat_mio, "*** BEXy con datos listos ***"))
        datos.bot_ready = True
    else: pass

