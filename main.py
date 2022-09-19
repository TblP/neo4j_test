import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import pandas as pd
import openpyxl
class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    """PIPEINF Подает информацию из файла построчно """
    def pipeinf(self,url):
        #чтение excel
        db = pd.read_excel(url)
        db2 = pd.read_excel(url,sheet_name="edges")
        #подача данных на сервер (создание нод)
        for i in range(db.shape[0]):
            self._create_statement(db['node'][i], db['ntype'][i],db['region'][i])
        # подача данных на сервер (соединение нод)
        for i in range(db2.shape[0]):
            self.create_friendship(db2['from'][i], db2['to'][i], db2['from_to'][i], int(db2['length'][i]), db2['status'][i],db2['line'][i], int(db2['nfiber'][i]),db2['step'][i])

    # _create_statement отправляет данные на сервере в execute_write подается методы обработки + данные(Переменные)
    def _create_statement(self, node_name,ntype,region):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.execute_write(
                self.create_statement, node_name,ntype,region)

    #перед методами обработки нужно писать @staticmethod
    @staticmethod
    def create_statement(tx,node_name,ntype,region):
        # query писать код для обработки как в нео4ж (для разделения команд нужно делать в конце строки пробел)
        query = (
            "CREATE (n1:NODE2 { name: $node_name, ntype: $ntype, region: $region}) "
        )
        # в tx.run нужно подавать команды (query) и данные которые нужно обработать
        result = tx.run(query, node_name=node_name,ntype=ntype,region=region)
        try:
            return [{"n1": record["n1"]["name"]["ntype"]["region"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    # create_friendship Создает соединения на сервере
    def create_friendship(self, node1_name, node2_name,fromto,lng,stat,line,nf,step):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.execute_write(
                self._create_and_return_friendship, node1_name, node2_name,fromto,lng,stat,line,nf,step)

        """если print работает то все хорошо если нет то result пустой и значит неправильно составлена query  
           for row in result:
                print("Created friendship between: {p1}, {p2} from {knows_from}".format(p1=row["n1"],p2=row["n2"],knows_from=row["k"]))"""


    #{ FromTo: $fromto, Length: $leng, Status: $status, Line: $line, nfiber: $nf, step: $step }

    #, leng=str(lng),status=stat, line=line, nf=str(nf), step=step
    #:NODE12 {name:$node_name,ntype:$node_name,region:$node_name}
    #"CREATE (n1)-[k:CONTACT { from: $fromto, Length: $leng, Status: $status, Line: $line, nfiber: $nf, step: $step}]->(n2) "
    @staticmethod
    def _create_and_return_friendship(tx, node1_name, node2_name,fromto,lng,stat,line,nf,step):
        # query писать код для обработки как в нео4ж (для разделения команд нужно делать в конце строки пробел)

        query = (
            "MATCH (n1:NODE1),(n2:NODE1) "
            "WHERE (n1.name=$node_name AND n2.name = $node_name2) "
            "CREATE (n1)-[k:PYKMIK { from: $fromto, Length: $leng, Status: $status, Line: $line, nfiber: $nf, step: $step}]->(n2) "
            "RETURN n1, k, n2"
        )
        #в tx.run нужно подавать команды (query) и данные которые нужно обработать
        result = tx.run(query, node_name=node1_name, node_name2=node2_name,fromto=fromto,leng=str(lng),status=stat, line=line, nf=str(nf), step=step)
        try:
            d = []
            for row in result:
                print(row)
                #добавление атрибутов
                d.append({
                    "n1": row["n1"],
                    "n2": row["n2"],
                    "fromto": row["k"]["from"],
                    "leng": row["k"]["Length"],
                    "status": row["k"]["Status"],
                    "line": row["k"]["Line"],
                    "nf": row["k"]["nfiber"],
                    "step": row["k"]["step"]
                })

            return d
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    """find_node - Поиск ноды в бд подаем 'название' ноды и в каком лейбле она должна находиться
    #with self.driver.session() as session: result = session.read_transaction данная конструкция делает подключение 
    к серверу бд и записывает результат  
    более подробно https://neo4j.com/docs/api/python-driver/4.4/api.html?highlight=session+write_transaction#neo4j.Session.write_transaction"""
    def find_node(self, node_name,graph_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_node, node_name,graph_name)
            for record in result:
                print("Found node: {record}".format(record=record))


    @staticmethod
    def _find_and_return_node(tx, node_name1,graph_name1):
        # query писать код для обработки как в нео4ж (для разделения команд нужно делать в конце строки пробел)
        query = (
            "MATCH (p:$graph_name) "
            "WHERE p.name = $node_name "
            "RETURN p.name AS name"
        )
        # в tx.run нужно подавать команды (query) и данные которые нужно обработать
        result = tx.run(query, node_name=node_name1,graph_name=graph_name1)
        return [record["name"] for record in result]



if __name__ == "__main__":
    # See https://neo4j.com/developer/aura-connect-driver/ for Aura specific connection URL.
    scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
    host_name = "localhost"
    port = 7687
    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = "neo4j"
    password = "qwerty123"
    app = App(url, user, password)
    app.pipeinf(r'C:\Users\vczyp\Desktop\test\NET example.xlsx')
    app.close()

