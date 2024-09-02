from pymongo import MongoClient
import threading #lib pra usar threads
import time #libs pra colocar delay
import random

client = MongoClient ('mongodb://localhost:27017')

db = client['bancoiot']

medicoes = db.sensores


#Função que será chamada pela Thread
def randomTemp(nome, intervalo):
    #Update do sensor alarmado como false no início do programa
    result = medicoes.update_one({"nomeSensor": nome}, {"$set": {'sensorAlarmado': False}})
    temp=random.randint(30,40)
    while temp <= 38:
        #Atualiza o bd com a primeira temperatura gerada, e depois vai gerando mais
        result = medicoes.update_one({"nomeSensor":nome},{"$set":{'valorSensor':temp}})
        time.sleep(intervalo)
        temp = random.randint(30, 40)
        print("Temperatura do sensor " + nome + " atualizada. Nova temperatura: " + str(temp) + "\n")

    #Sai do loop while e atualiza os campos do bd e indica que o sensor está com temperatura acima do previsto
    print("Atenção! Temperatura muito alta, verificar sensor " + nome + "\n")
    result = medicoes.update_one({"nomeSensor": nome}, {"$set": {'valorSensor': temp}})
    result = medicoes.update_one({"nomeSensor": nome}, {"$set": {'sensorAlarmado': True}})


x = threading.Thread(target=randomTemp, args=('Temp1', 2))
y = threading.Thread(target=randomTemp, args=('Temp2', 4))
z = threading.Thread(target=randomTemp, args=('Temp3', 6))
x.start()
y.start()
z.start()



