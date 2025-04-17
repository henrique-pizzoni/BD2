
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory


class CassandraConnector:
    def __init__(self):
        self.cassandra_session = None

    def get_cassandra_connector(self):
        if self.cassandra_session is None:
            cloud_config = {
                "secure_connect_bundle": "secure-connect-dbatividade.zip"
            }

            with open("dbatividade-token.json") as f:
                secrets = json.load(f)

            CLIENT_ID = secrets["clientId"]
            CLIENT_SECRET = secrets["secret"]

            auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.cassandra_session = cluster.connect()
            self.cassandra_session.row_factory = dict_factory

            self.cassandra_session.set_keyspace('ksatv')
        return self.cassandra_session


class AutoPart:
    def __init__(self, id, name, car, shelf, level, amount):
        self.id = id
        self.name = name
        self.car = car
        self.shelf = shelf
        self.level = level
        self.amount = amount

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "car": self.car,
            "shelf": self.shelf,
            "level": self.level,
            "amount": self.amount
        }


class AutoPartDAO:
    def __init__(self):
        connector = CassandraConnector()
        self.cassandra_session = connector.get_cassandra_connector()

    def create_table(self):
        self.cassandra_session.execute(
            'CREATE TABLE estoque (id int, nome text, carro text, estante int, nivel int, quantidade int, PRIMARY KEY ((carro), nome));'
        )

    def add_part(self):
        self.cassandra_session.execute('BEGIN BATCH ' +
            'INSERT INTO estoque(id, nome, carro, estante, nivel, quantidade) values (5, \'Pistao\', \'Mustang\', 4, 1, 167);' +
            'INSERT INTO estoque(id, nome, carro, estante, nivel, quantidade) values (4, \'Suspensao\', \'Argo\', 1, 1, 3500);' +
            'APPLY BATCH;'
        )

    def get_parts_of_car(self, carro):
        prepared = self.cassandra_session.prepare("SELECT nome, estante, quantidade FROM estoque WHERE carro = ? ALLOW FILTERING")
        rows = self.cassandra_session.execute(prepared, [carro])
        return rows

    # Métodos adicionais ainda a implementar


if __name__ == "__main__":
    a1 = AutoPartDAO()
    #a1.create_table()
    #a1.add_part()

    # Questão 3
    car = input("\nModelo do carro: ")
    print(f"Partes do carro: {car}:")
    for row in a1.get_parts_of_car(car):
        print(row)