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


    def pipeinf(self,url):
        db = pd.read_excel(url)
        db2 = pd.read_excel(url,sheet_name="edges")
        """for i in range(db.shape[0]):
            self._create_statement(db['node'][i], db['ntype'][i],db['region'][i])"""
        for i in range(db2.shape[0]):
            self.create_friendship(db2['from'][i], db2['to'][i], db2['from_to'][i], int(db2['length'][i]), db2['status'][i],db2['line'][i], int(db2['nfiber'][i]),db2['step'][i])
    def _create_statement(self, node_name,ntype,region):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.execute_write(
                self.create_statement, node_name,ntype,region)

    @staticmethod
    def create_statement(tx,node_name,ntype,region):
        query = (
            "CREATE (n1:NODE2 { name: $node_name, ntype: $ntype, region: $region}) "
        )
        result = tx.run(query, node_name=node_name,ntype=ntype,region=region)
        try:
            return [{"n1": record["n1"]["name"]["ntype"]["region"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_friendship(self, node1_name, node2_name,fromto,lng,stat,line,nf,step):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.execute_write(
                self._create_and_return_friendship, node1_name, node2_name,fromto,lng,stat,line,nf,step)
            for row in result:
                print("Created friendship between: {p1}, {p2} from {knows_from}")


    #{ FromTo: $fromto, Length: $leng, Status: $status, Line: $line, nfiber: $nf, step: $step }

    #, leng=str(lng),status=stat, line=line, nf=str(nf), step=step
    #:NODE12 {name:$node_name,ntype:$node_name,region:$node_name}
    #"CREATE (n1)-[k:CONTACT { from: $fromto, Length: $leng, Status: $status, Line: $line, nfiber: $nf, step: $step}]->(n2) "
    @staticmethod
    def _create_and_return_friendship(tx, node1_name, node2_name,fromto,lng,stat,line,nf,step):
        # node_name2=str(node2_name),fromto=fromto, leng=str(lng),status=stat, line=line, nf=str(nf), step=step
        query = (
            "MATCH (n1:NODE1),(n2:NODE1) "
            "WHERE (n1.name=$node_name AND n2.name = $node_name2) "
            "CREATE (n1)-[k:PYKMIK { from: $fromto, Length: $leng, Status: $status, Line: $line, nfiber: $nf, step: $step}]->(n2) "
            "RETURN n1, k, n2"
        )
        result = tx.run(query, node_name=node1_name, node_name2=node2_name,fromto=fromto,leng=str(lng),status=stat, line=line, nf=str(nf), step=step)
        try:
            d = []
            for row in result:
                print(row)
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

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for record in result:
                print("Found person: {record}".format(record=record))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
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


