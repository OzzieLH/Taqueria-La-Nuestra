import boto3
import json
from datetime import datetime
import random
import time
import threading
from threading import Thread
import queue
import copy
import simplejson as json
sqs = boto3.client('sqs')
WHO = 'DANIEL'
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/292274580527/sqs_cc106_team_5"
oid = 0

def create_queue():
    response = sqs.create_queue(
        QueueName="my-new-queue",
        Attributes={
            "DelaySeconds": "0",
            "VisibilityTimeout": "60", 
        }
    )
    print(response)


def get_queue_url(_name):
    response = sqs.get_queue_url(
        QueueName=_name,
    )
    return response["QueueUrl"]


def get_number_messages():
    queue_attr = sqs.get_queue_attributes(
        QueueUrl=QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(queue_attr['Attributes']['ApproximateNumberOfMessages'])


def read_message():
    response = sqs.receive_message(QueueUrl=QUEUE_URL)
    if 'Messages' in response:
        message = response['Messages']
        orden = json.loads(message[0]['Body'])
        print(
            f"Atendiendo orden {orden['request_id']} Leyendo mensaje del queue")
        return [message,orden]


def delete_message(message, orden, complete):
    if complete:
        #orden["end_datetime"] = str(datetime.now().timestamp())
        #print(
            #f"Orden {orden['request_id']} Terminada. Mensaje sera borrado del queue.")
        sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=message["ReceiptHandle"])
        #print(orden)
        return print('MENSAJE BORRADO')
    else:
        print(
            f"Orden {orden['request_id']} Pendiente. Regresando mensaje del queue")


def send_message(outputqueue,mensaje, orden):
    delete_message(mensaje, orden, True)
    response = sqs.send_message(
        QueueUrl=outputqueue,
        MessageBody=(json.dumps(orden))
    )
    print(response)


def round_robin():
    while get_number_messages() > 0:
        print(f"Ordenes pendientes:{get_number_messages()}")
        # time.sleep(2)
        mensaje, orden = read_message()
        orden['tiempo_pendiente'] = 0  
        orden["process"].append(
            {"who": WHO, "new_tiempo_pendiente": orden["tiempo_pendiente"]})
        if orden["tiempo_pendiente"] <= 0:
            delete_message(mensaje, orden, True)
        else:
            send_message(mensaje, orden)


def purge_queue():
    response = sqs.purge_queue(
        QueueUrl=QUEUE_URL,
    )
    print(f'Purged queue: {QUEUE_URL}')
    print(response)


def init():
    total = 6
    print(f'Agregando {total} ordenes a SQS')
    for index in range(total):
        orden = {
        "datetime": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        "request_id": index,
        "status": "open",
        "orden": [
            {
                "part_id": "2-0",
                "type": "quesadilla",
                "meat": "suadero",
                "status": "open",
                "quantity": 69,
                "ingredients": []
            },
            {
                "part_id": "2-1",
                "type": "taco",
                "meat": "suadero",
                "status": "open",
                "quantity": 35,
                "ingredients": [
                    "cebolla",
                    "salsa"
                ]
            },
            {
                "part_id": "2-2",
                "type": "taco",
                "meat": "asada",
                "status": "open",
                "quantity": 69,
                "ingredients": [
                    "cebolla",
                    "cilantro"
                ]
            },
            {
                "part_id": "2-3",
                "type": "taco",
                "meat": "tripa",
                "status": "open",
                "quantity": 17,
                "ingredients": [
                    "salsa",
                    "cilantro",
                    "guacamole"
                ]
            },
            {
                "part_id": "2-4",
                "type": "taco",
                "meat": "cabeza",
                "status": "open",
                "quantity": 91,
                "ingredients": [
                    "guacamole",
                    "cebolla",
                    "cilantro"
                ]
            }
        ]
    }
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=(json.dumps(orden))
    )
    print(response)

def buscador(oid):
    while True:
        temp = False
        response = sqs.receive_message(QueueUrl=QUEUE_URL)
        if("Messages") in response:
            message = response['Messages'][0]['Body']
            respuesta = json.loads(message)
            orde = read_message()
            message = response["Messages"][0]
            try:
                orden = respuesta['orden']
                #monitor(orden,chalanA)
                temp = True
            except:
                pass
#message, orden, complete
            if temp == True:
                oid += 1
                mo = Thread(target=monitor,args=(orden,chalanA,oid))
                mo.start()
                print(orden)
                delete_message(message,orde,True)
                print("ORDEN RECIBIDA")




  

def generate_tacos():
    tacos = []
    type = ["taco", "quesadilla"]
    meat = ["asada", "adobada", "suadero", "tripa", "cabeza"]
    fillings = ["cebolla", "cilantro", "salsa", "guacamole"]
    for x in range(random.randrange(100)):
        taco = {
            "datetime": str(datetime.now()), 
            "request_id": x, 
            "status": "open", 
            "orden":[ ] 
        }
        for y in range(random.randrange(10)):
            taco["orden"].append(
                {   
                    "part_id": "{0}-{1}".format(x, y), 
                    "type": random.choice(type), 
                    "meat": random.choice(meat), 
                    "status":"open", 
                    "quantity": random.randrange(100), 
                    "ingredients":[ ] 
                }
            )
            local_fillings = copy.deepcopy(fillings)
            for z in range(random.randrange(len(local_fillings))):
                ind_filling = random.choice(local_fillings)
                taco["orden"][y]["ingredients"].append(ind_filling)
                local_fillings.remove(ind_filling)

        tacos.append(taco)  

    tacos_file = open("tacos.json", "w")
    tacos_file.write(json.dumps(tacos))
    tacos_file.close()


def ventilador(taquero):
    txt = "taquero de {tq} enciende ventilador!"
    print(txt.format(tq = taquero))
    time.sleep(60)
    txt = "taquero de {tq} apaga ventilador!"
    print(txt.format(tq = taquero))
    
def verificador(orden):
    for i in orden:
        if i['status'] == "open":
            return False
    return True
            
def chalanA(ingredientes,ingrediente):
    CA.acquire()
    
    if ingrediente == "tortillas":
        print("rellenando tortillas")
        time.sleep(5)
        ingredientes[4] = 50
        
    elif ingrediente == "guacamole":
        print("rellenando guacamole")
        time.sleep(20)
        ingredientes[1] = 100

    elif ingrediente == "cebolla":
        print("rellenando cebolla")
        time.sleep(10)
        ingredientes[2] = 200
        
    elif ingrediente == "cilantro":
        print('rellenando cilantro')
        time.sleep(10)
        ingredientes[2] = 200
        
    elif ingrediente == "salsa":
        print("rellenando salsa")
        time.sleep(15)
        ingredientes[0] = 150
        
    CA.release()

def chalanB(ingredientes,ingrediente):
    CB.acquire()
    
    if ingrediente == "tortillas":
        print("rellenando tortillas")
        time.sleep(5)
        ingredientes[4] = 50
        
    elif ingrediente == "guacamole":
        print("rellenando guacamole")
        time.sleep(20)
        ingredientes[1] = 100

    elif ingrediente == "cebolla":
        print("rellenando cebolla")
        time.sleep(10)
        ingredientes[2] = 200
        
    elif ingrediente == "cilantro":
        print('rellenando cilantro')
        time.sleep(10)
        ingredientes[2] = 200
        
    elif ingrediente == "salsa":
        print("rellenando salsa")
        time.sleep(15)
        ingredientes[0] = 150
    CB.release()
    
def prep_quesadilla(ingredientes):
    print("preparando quesadillas")
    quesa.acquire()
    time.sleep(100)
    ingredientes[7] = 5
    quesa.release()
        

def taquero_adobada(orden,i,ingredientes,chalanA,oid):
    adobada.acquire()
    qty = orden[i]["quantity"]
    ingredients = orden[i]['ingredients']
    if orden[i]['type'] == "quesadilla":
        for l in range(qty):
            ingredientes_adobada[7] -= 1
            if ingredientes_adobada[7] == 0:
                qa = Thread(target=prep_quesadilla,args=(ingredientes_adobada,))
                qa.start()
            
    else:
        print(i,orden[i])
        for l in range(qty):
            ingredientes_adobada[4]-= 1
            time.sleep(1)

            if ingredientes_adobada[4] == 0:
                chalanA(ingredientes_adobada,"tortillas")

            if "guacamole" in ingredients:
                time.sleep(.5)
                ingredientes_adobada[1] -= 1
                if ingredientes_adobada[1] == 0:
                    chalanA(ingredientes_adobada,"guacamole")

            if "salsa" in ingredients:
                time.sleep(.5)
                ingredientes_adobada[0] -= 1
                if ingredientes_adobada[0] == 0:
                    chalanA(ingredientes_adobada,"salsa")

            if "cilantro" in ingredients:
                time.sleep(.5)
                ingredientes_adobada[2] -= 1
                if ingredientes_adobada[2] == 0:
                    chalanA(ingredientes_adobada,"cilantro")

            if "cebolla" in ingredients:
                time.sleep(.5)
                ingredientes_adobada[3] -= 1
                if ingredientes_adobada[3] == 0:
                    chalanA(ingredientes_adobada,"cebolla")

            ingredientes_adobada[5] -= 1
            ingredientes_adobada[6] += 1
            if ingredientes_adobada[6] == 600:
                va = Thread(target=ventilador,args=("adobada",))
                va.start()
            
    orden[i]['status'] = "complete"
    if verificador(orden) == True:
        txt = "Orden {o} completada!"
        print(txt.format(o = str(oid)))
    else:
        print("orden aun no se termina")
    adobada.release()

def taquero_asada(orden,i,ingredientes,chalanA,oid):
    asada.acquire()
    qty = orden[i]["quantity"]
    ingredients = orden[i]['ingredients']
    if orden[i]['type'] == "quesadilla":
        for k in range(qty):
            ingredientes_asada[7] -= 1
            if ingredientes_asada[7] == 0:
                qa = Thread(target=prep_quesadilla,args=(ingredientes_asada,))
                qa.start()
    else:
        for k in range(qty):
            ingredientes_asada[4]-= 1
            time.sleep(1)

            if ingredientes_asada[4] == 0:
                chalanA(ingredientes_asada,"tortillas")

            if "guacamole" in ingredients:
                time.sleep(.5)
                ingredientes_asada[1] -= 1
                if ingredientes_asada[1] == 0:
                    chalanA(ingredientes_asada,"guacamole")

            if "salsa" in ingredients:
                time.sleep(.5)
                ingredientes_asada[0] -= 1
                if ingredientes_asada[0] == 0:
                    chalanA(ingredientes_asada,"salsa")

            if "cilantro" in ingredients:
                time.sleep(.5)
                ingredientes_asada[2] -= 1
                if ingredientes_asada[2] == 0:
                    chalanA(ingredientes_asada,"cilantro")

            if "cebolla" in ingredients:
                time.sleep(.5)
                ingredientes_asada[3] -= 1
                if ingredientes_asada[3] == 0:
                    chalanA(ingredientes_asada,"cebolla")

            ingredientes_asada[5] -= 1
            ingredientes_asada[6] += 1
            if ingredientes_asada[6] == 600:
                vas = Thread(target=ventilador,args=("asada",))
                vas.start()
                ingredientes_asada[6] = 0
                
    orden[i]['status'] = "complete"
    if verificador(orden) == True:
        txt = "Orden {o} completada!"
        print(txt.format(o = str(oid)))
    else:
        print("orden aun no se termina")    
    asada.release()

def taquero_cabeza(orden,i,ingredientes,chalanB,oid):
    cabeza.acquire()
    qty = orden[i]["quantity"]
    ingredients = orden[i]['ingredients']
    if orden[i]['type'] == "quesadilla":
        for k in range(qty):
            ingredientes_cabeza[7] -= 1
            if ingredientes_cabeza[7] == 0:
                qa = Thread(target=prep_quesadilla,args=(ingredientes_cabeza,))
                qa.start()
    else:
        for j in range(qty):
            print(j)
            ingredientes_cabeza[4]-= 1
            time.sleep(1)

            if ingredientes_cabeza[4] == 0:
                chalanA(ingredientes_cabeza,"tortillas")

            if "guacamole" in ingredients:
                time.sleep(.5)
                ingredientes_cabeza[1] -= 1
                if ingredientes_cabeza[1] == 0:
                    chalanA(ingredientes_cabeza,"guacamole")

            if "salsa" in ingredients:
                time.sleep(.5)
                ingredientes_cabeza[0] -= 1
                if ingredientes_cabeza[0] == 0:
                    chalanA(ingredientes_cabeza,"salsa")

            if "cilantro" in ingredients:
                time.sleep(.5)
                ingredientes_cabeza[2] -= 1
                if ingredientes_cabeza[2] == 0:
                    chalanA(ingredientes_cabeza,"cilantro")

            if "cebolla" in ingredients:
                time.sleep(.5)
                ingredientes_cabeza[3] -= 1
                if ingredientes_cabeza[3] == 0:
                    chalanA(ingredientes_cabeza,"cebolla")

            ingredientes_cabeza[5] -= 1
            ingredientes_cabeza[6] += 1
            if ingredientes_cabeza[6] == 600:
                vca = Thread(target=ventilador,args=('cabeza',))
                vca.start()
                ingredientes_cabeza[6] = 0
    orden[i]['status'] = "complete"
    if verificador(orden) == True:
        txt = "Orden {o} completada!"
        print(txt.format(o = str(oid)))
    else:
        print("orden aun no se termina")    
    cabeza.release()
        
def monitor(order,chalanA,oid):
    le = len(order)-1
    for i in range(le):
        if orden[i]['meat']=="asada" or orden[i]['meat']=="suadero":
            asada1 = Thread(target=taquero_asada, args=(orden,i,ingredientes_asada,chalanA,oid))
            asada1.start()

        elif orden[i]['meat']=="adobada":
            adobada1 = Thread(target=taquero_adobada, args=(orden,i,ingredientes_adobada,chalanA,oid))
            adobada1.start()

        elif orden[i]['meat']=="cabeza" or orden[i]['meat']=="lengua" or orden[i]['meat']=="tripa":
            cabeza = Thread(target=taquero_cabeza, args=(orden,i,ingredientes_cabeza,chalanA,oid))
            cabeza.start()
ingredientes_asada = [150,100,200,200,50,1000,0,5,'asada'] #salsa[0], guacamole[1], cilantro[2], cebolla[3], tottillas[4]
ingredientes_adobada = [150,100,200,200,50,1000,0,5,'adobada'] #salsa[0], guacamole[1], cilantro[2], cebolla[3], tottillas[4]
ingredientes_cabeza = [150,20,200,200,50,1000,0,5,'cabeza'] #salsa[0], guacamole[1], cilantro[2], cebolla[3], tottillas[4]
CA = threading.Lock()
CB = threading.Lock()
adobada = threading.Lock()
asada = threading.Lock()
cabeza = threading.Lock()
quesa = threading.Lock()

init()
ini = Thread(target=buscador,args=(oid,))
ini.start()

generate_tacos()
with open('/Users/danielvelazquez/tacos.json') as f:
  data = json.load(f)
prueba = data[0]
orden = data[3]['orden']
print(orden)


