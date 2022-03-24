from telegram.ext import Updater, CommandHandler
from vars import vars
from ops.data_ops import bot_search_monitor, bot_search_activo, veces_en_activo, \
    bot_search_temporal, bot_search_cancelado, veces_cancelados, veces_temporal, \
        veces_temporal_status, return_bloque, return_bloques, return_bloqp, return_bloqs
from datos import datos
from telegram.error import TimedOut, NetworkError

#######
def send_message(update, context, texto):
    try:
        # print(update.effective_chat.id)
        context.bot.send_message(chat_id=update.effective_chat.id, text=texto, timeout=vars.timeout)
    except TimedOut as err:
        try:
            context.bot.send_message(chat_id=update.effective_chat.id, text=texto, timeout=vars.timeout)  ## *VUELVE A INTENTARLO*
        except TimedOut as err:
            text_error = "* ERROR DE CONEXION *\n\nEl servidor donde corre BEXy esta\nsufriendo problemas de conexión\n\n"+\
                "La respuesta anterior seguramente salio incompleta\nINTENTE DE NUEVO por favor"
            context.bot.send_message(chat_id=update.effective_chat.id, text=text_error, timeout=vars.timeout)
    except NetworkError as err:   ## Excepcion que le aparece a JuanMa
        try: 
            context.bot.send_message(chat_id=update.effective_chat.id, text=texto, timeout=vars.timeout)  ## *VUELVE A INTENTARLO*
        except NetworkError as err:
            text_error = "* ERROR DE CONEXION *\n\nEl servidor donde corre BEXy esta\nsufriendo problemas de conexión\n\n"+\
                "La respuesta anterior seguramente salio incompleta\nINTENTE DE NUEVO por favor"
            context.bot.send_message(chat_id=update.effective_chat.id, text=text_error, timeout=vars.timeout)   
#######
def monitor(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) == 1:
        texto_en_comando = list_of_str[0]
        send_message(update, context, texto=bot_search_monitor(texto_en_comando))
    elif list_of_str and len(list_of_str) > 1:
        send_message(update, context, texto="El comando solo puede aceptar una palabra")
    else:
        send_message(update, context, texto="El comando se recibio sin argumentos")

def activo(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) == 1:
        texto_en_comando = list_of_str[0]
        resp = veces_en_activo(texto_en_comando)
        if not resp:
            send_message(update, context, texto="El job no se encuentra cargado en el activo " + \
            "o no se encuentra dentro de la  UUAAs: RAS, DEO, CTK, RIG, DCO o ALM")
        elif resp[0] >= 1:
            for i in range(resp[0]):
                send_message(update, context, texto=bot_search_activo(resp[1], i))
    elif list_of_str and len(list_of_str) > 1:
        send_message(update, context, texto="El comando solo puede aceptar una palabra")
    else:
        send_message(update, context, texto="El comando se recibio sin argumentos")

def temporal(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) == 1:
        texto_en_comando = list_of_str[0]
        resp = veces_temporal()
        if not resp:
            send_message(update, context, texto="No hay jobs de malla temporal en el activo")
        else:
            if texto_en_comando == "cancelados" or texto_en_comando == "notok":
                status = "Ended Not OK"
                resp1 = veces_temporal_status(status)
                if not resp1:
                    send_message(update, context, texto="No hay cancelados de la malla temporal")
                else:
                    for i in range(resp1[0]):
                        send_message(update, context, texto=bot_search_temporal(resp1[1], i, texto_en_comando))
            elif texto_en_comando == "ok":
                status = "Ended OK"
                resp1 = veces_temporal_status(status)
                if not resp1:
                    send_message(update, context, texto="No hay OKs de la malla temporal")
                else:
                    for i in range(resp1[0]):
                        send_message(update, context, texto=bot_search_temporal(resp1[1], i, texto_en_comando))
            elif texto_en_comando == "wait":
                status = "Wait Condition ¿Que espera para ejecutar?"
                resp1 = veces_temporal_status(status)
                if not resp1:
                    send_message(update, context, texto="No hay jobs en wait de la malla temporal")
                else:
                    for i in range(resp1[0]):
                        send_message(update, context, texto=bot_search_temporal(resp1[1], i, texto_en_comando))
            elif texto_en_comando == "ex":
                status = "Executing"
                resp1 = veces_temporal_status(status)
                if not resp1:
                    send_message(update, context, texto="No hay jobs ejecutando")
                else:
                    for i in range(resp1[0]):
                        send_message(update, context, texto=bot_search_temporal(resp1[1], i, status))
            else:
                send_message(update, context, texto="Argumento no identificado como status aceptable")            
    elif list_of_str and len(list_of_str) > 1:
        send_message(update, context, texto="El comando solo puede aceptar una palabra")
    else:
        send_message(update, context, texto="El comando se recibio sin argumentos")

def bloque(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) >= 1:
        texto_en_comando = " ".join(list_of_str)
        send_message(update, context, texto=return_bloque(texto_en_comando))
    else:
        send_message(update, context, texto="El comando se recibio sin argumentos")

def bloques(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) >= 1:
        send_message(update, context, texto="El comando no acepta argumentos")
    if not list_of_str and len(list_of_str) == 0:
        send_message(update, context, texto=return_bloques())

def help(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) >= 1:
        send_message(update, context, texto="El comando no acepta argumentos")
    if not list_of_str and len(list_of_str) == 0:
        texto = "*** AYUDA ***\n\n*Comandos reconocidos por BEXy:\n"
        for comando, info in vars.comandos.items():
            texto += "{}\n{}\n".format(
                comando, info
            )
        send_message(update, context, texto=texto)
        mas_texto = "Tambien, BEXy avisa automaticamente:\n-Cuando hay un cancelado\n"+\
            "-Los cambios de estado en\nlos bloques de ejecuciones"
        send_message(update, context, texto=mas_texto)

def cancelados(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) >= 1:
        send_message(update, context, texto="El comando no acepta argumentos")
    if not list_of_str and len(list_of_str) == 0:
        resp = veces_cancelados()
        if not resp:
            if datos.last_update_activo:
                send_message(update, context, texto="No tenemos cancelados :D !!!")
                texto = "* ULTIMA ACTUALIZACION DE DATOS *\n"
                texto += ' ' * 13 + datos.last_update_activo
                send_message(update, context, texto=texto)
            else:
                send_message(update, context, texto="Aun no termina la primera carga de datos")
        else:
            for i in range(len(datos.delegated_notoks)):
                send_message(update, context, texto=bot_search_cancelado(i))
            texto = "* ULTIMA ACTUALIZACION DE DATOS *\n"
            texto += ' ' * 13 + datos.last_update_activo
            send_message(update, context, texto=texto)            

def last(update, context):
    list_of_str = context.args
    if list_of_str and len(list_of_str) >= 1:
        send_message(update, context, texto="El comando no acepta argumentos")
    if not list_of_str and len(list_of_str) == 0:
        texto = "* ULTIMA ACTUALIZACION DE DATOS *\n"
        if datos.last_update_activo:
            texto += ' ' * 13 + datos.last_update_activo
            send_message(update, context, texto=texto)
        else:
            send_message(update, context, texto="Aun no termina la primera carga de datos")
    

def bloq(update, context):   
    list_of_str = context.args
    if list_of_str and len(list_of_str) >= 1:
        texto_en_comando = " ".join(list_of_str)
        send_message(update, context, texto=return_bloqp(texto_en_comando))
    else:
        send_message(update, context, texto="El comando se recibio sin argumentos")

def bloqs(update, context): 
    list_of_str = context.args
    if list_of_str and len(list_of_str) >= 1:
        send_message(update, context, texto="El comando no acepta argumentos")
    if not list_of_str and len(list_of_str) == 0:
        send_message(update, context, texto=return_bloqs())

#############################################################################################################

class Bot:
    def __init__(self):
        self.updater = Updater(token=vars.token, use_context=True)
        self.dispatcher = self.updater.dispatcher

        monitor_handler = CommandHandler("monitor", monitor)
        self.dispatcher.add_handler(monitor_handler)
        
        job_handler = CommandHandler("activo", activo)
        self.dispatcher.add_handler(job_handler)

        temporal_handler = CommandHandler("temporal", temporal)
        self.dispatcher.add_handler(temporal_handler)

        bloque_handler = CommandHandler("bloque", bloque)
        self.dispatcher.add_handler(bloque_handler)

        bloques_handler = CommandHandler("bloques", bloques)
        self.dispatcher.add_handler(bloques_handler)

        bloqp_handler = CommandHandler("bloq", bloq) ##### PROBANDO
        self.dispatcher.add_handler(bloqp_handler)

        bloqs_handler = CommandHandler("bloqs", bloqs) ##### PROBANDO
        self.dispatcher.add_handler(bloqs_handler)

        help_handler = CommandHandler("help", help)
        self.dispatcher.add_handler(help_handler)

        cancelados_handler = CommandHandler("cancelados", cancelados)
        self.dispatcher.add_handler(cancelados_handler)

        last_handler = CommandHandler("last", last)
        self.dispatcher.add_handler(last_handler)

        self.updater.start_polling(drop_pending_updates=True)

