from pymongo import MongoClient

client = MongoClient ('mongodb://localhost:27017')

db = client['bancoiot']

medicoes = db.sensores

#Insere um novo documento na collection
newdoc = {
    "nomeSensor" : "Temp3",
    "valorSensor" : 2,
    "unidadeMedida" : "ºC",
    "sensorAlarmado" : False
}

result = db.sensores.insert_one(newdoc)

if result.acknowledged:
    print("Doc add")