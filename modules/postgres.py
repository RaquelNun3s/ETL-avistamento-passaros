import psycopg2

class Conector_postgres:
    
    def __init__ (self, host, db, user="postgres", password="postgres"):
        try:
            self.host = host
            self.db = db
            self.user = user
            self.password = password
        except Exception as e:
            print(str(e))
    
    def conectar(self):
        conn = psycopg2.connect( host=self.host, database=self.db, user=self.user, password=self.password)
        cursor = conn.cursor()
        return conn, cursor

    def desconectar(self, conn, cursor):
        conn.commit()
        cursor.close()
        conn.close()

    def inserir(self, query):
        conn, cursor = self.conectar()
        cursor.execute(query)
        self.desconectar(conn, cursor)
        
    def selecionar(self, query):
        conn, cursor = self.conectar()
        cursor.execute(query)
        dados = cursor.fetchall()
        self.desconectar(conn, cursor)
        lista_dados = []
        for dado in dados:
            lista_dados.append(dado)
        return lista_dados
    
    def deletar(self, query):
        conn, cursor = self.conectar()
        cursor.execute(query)
        self.desconectar(conn, cursor)
    
    def alterar(self, query):
        conn, cursor = self.conectar()
        cursor.execute(query)
        self.desconectar(conn, cursor)
    
    def update(self, query):
        conn, cursor = self.conectar()
        cursor.execute(query)
        self.desconectar(conn, cursor)
    
    def update(self, query):
        conn, cursor = self.conectar()
        cursor.execute(query)
        self.desconectar(conn, cursor)
    
    def criar(self,query):
        conn, cursor = self.conectar()
        cursor.execute(query)
        self.desconectar(conn, cursor)
    
    def get_user(self):
        return self.user