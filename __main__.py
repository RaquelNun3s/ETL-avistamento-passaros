'''
----------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------Atividade 17--------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
Equipe: Carlos Bahia, Jefferson Costa, Raquel Nunes.

'''
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np
from modules.postgres import Conector_postgres
try:
    # -------------------------------------------- EXTRAÇÃO DOS DADOS --------------------------------------------
    path = # Cole aqui o caminho do seu aeequivo
    df_dados = pd.read_csv(path)
    print(df_dados)
    print("--------------------------------------------------------------------------------------------------------------------")

    # ------------------------------------------- TRATAMENTO DOS DADOS -------------------------------------------
    # Checando a presença de valores nulos:
    print("Quantidade de valores nulos por coluna: ")
    print(df_dados.isna().sum()) 
    print("--------------------------------------------------------------------------------------------------------------------")
    # Tratando os valores nulos
    df_dados.dropna(inplace=True)
    
    # Checando a presença de dados duplicados:
    print("Quantidade de linhas duplicadas df_dados: ", df_dados.duplicated().sum())

    # Tratando os dados duplicados:
    df_dados.drop_duplicates(inplace = True)
    
    # Removendo colunas desnecessárias:
    df_dados.drop(columns=['Sub.cell', 'Cell.ID'], inplace=True)

    # Trocando os nomes das colunas:
    df_dados = df_dados.rename(columns={"Common.Name" : 'common_name', "n.observers": 'n_observers', "Date": 'date_data', "Time":'time_data', "County":'county', "Season": 'season', "DEM":"dem", "List.ID": 'list_id'})
    
    # ------------------------------------------ CARREGAMENTO DOS DADOS ------------------------------------------
    # Conectando com o banco de dados
    banco = Conector_postgres(host="127.0.0.1", db="atividade17")
    
    # Deletando as Views, caso existam
    banco.deletar("DROP VIEW IF EXISTS lista_data CASCADE")
    
    # Inserindo os valores no banco de dados:
    engine = create_engine('postgresql://postgres:postgres@127.0.0.1:5432/atividade17') #Insira aqui os seus dados, no seguinte formato: ('postgresql://myusername:mypassword@myhost:5432/mydatabase')
    df_dados.to_sql('dados', engine, if_exists='replace', index=False)

    # Alterando os tipos de dados:
    banco.alterar("ALTER TABLE dados ALTER COLUMN date_data TYPE date USING date_data::date;"
                  "ALTER TABLE dados ALTER COLUMN time_data TYPE time USING time_data::time without time zone;")
    
    # ------------------------------------------ LENDO OS DADOS DO BANCO -----------------------------------------
    # Criando um data frame com a quantidade de espécies avistadas para cada um dos grupos, com o número de observadores no grupo e ordenado pelo número de observadores
    df_observacoes = pd.DataFrame(banco.selecionar("SELECT list_id, COUNT(common_name) as num_especies, n_observers FROM dados GROUP BY (list_id, n_observers) ORDER BY n_observers"), columns=['Lista', 'Nº Espécies', 'Nº Observadores'])
    
    # Criando um data frame com a quantidade de espécies avistadas para cada grupo, ordenado por data e horário
    df_especies_grupos = pd.DataFrame(banco.selecionar("SELECT list_id, COUNT(common_name) as num_especies, date_data, time_data FROM dados GROUP BY (list_id, date_data, time_data) ORDER BY date_data, time_data;"), columns=['list', 'num_especies', 'Date', 'hora'])

    # Criando um data frame com a quantidade de pássaros avistados por data, ordenado pela data
    df_passaros_dia = pd.DataFrame(banco.selecionar("SELECT date_data, COUNT(common_name) as num_especies FROM dados GROUP BY date_data ORDER BY date_data;"), columns=['Data', 'Nº pássaros observados'])
    df_passaros_dia.index = pd.to_datetime(df_passaros_dia['Data'])
    df_passaros_ano = df_passaros_dia.groupby(pd.Grouper(freq = 'Y')).sum()

    # Criando um data frame com a quantidade de visitas por dia, ordenado pela data
    banco.criar("CREATE VIEW lista_data AS (SELECT DISTINCT list_id, date_data FROM dados ORDER BY date_data);")
    df_visitas_dia = pd.DataFrame(banco.selecionar("SELECT date_data, COUNT(list_id) FROM lista_data GROUP BY date_data ORDER BY date_data;"), columns= ["Data", "Nº Visitas"])
    df_visitas_dia.index = pd.to_datetime(df_visitas_dia['Data'])
    df_visitas_ano = df_visitas_dia.groupby(pd.Grouper(freq='Y')).sum()

    # ------------------------------------------- PLOTANDO OS GRÁFICOS -------------------------------------------
    # Gráfico 01:
    plt.figure(figsize=(10,5))
    plt.title("Nº de Espécies Avistadas por Nº de observadores")
    z = np.polyfit(df_observacoes["Nº Observadores"], df_observacoes["Nº Espécies"], 1)
    p = np.poly1d(z)
    plt.plot(df_observacoes["Nº Observadores"], p(df_observacoes["Nº Observadores"]), "r", label='Linha de tendência')
    plt.scatter(df_observacoes["Nº Observadores"], df_observacoes["Nº Espécies"])
    plt.xlabel("Nº de Observadores")
    plt.ylabel("Nº de Espécies")
    plt.legend()
    plt.show()
    
    #Gráfico 02:
    plt.figure(figsize=(10,10))
    plt.subplot(2,1,1)
    plt.title("Nº Registros de pássaros observados por ano")
    plt.plot(df_passaros_ano.index, df_passaros_ano["Nº pássaros observados"])
    plt.xlabel("Ano")
    plt.ylabel("Nº de Pássaros Observados")
    
    plt.subplot(2, 1, 2)
    plt.title("Nº de visitas por ano")
    plt.plot(df_visitas_ano.index , df_visitas_ano["Nº Visitas"])
    plt.xlabel("Ano")
    plt.ylabel("Nº de Visitas")
    plt.show()
    
    # Gráfico 03:
    plt.figure(figsize=(10,5))
    plt.title("Pássaros observados por visita")
    plt.xlabel("Visitas")
    plt.ylabel("Nº Espécies Observadas")
    z = np.polyfit(df_especies_grupos.index, df_especies_grupos["num_especies"], 1)
    p = np.poly1d(z)
    plt.plot(df_especies_grupos.index, p(df_especies_grupos["num_especies"]), "r", label='Linha de tendência')
    plt.scatter(df_especies_grupos.index, df_especies_grupos['num_especies'])
    plt.legend()
    plt.show()
    
    # --------------------------------------------- INSIGHTS OBTIDOS ---------------------------------------------
    print("--------------------------------------------------------------------------------------------------------------------")
    print("01) Observações relevantes: ")
    print("- Houve uma grande queda abrupta no número de visitas para observação de pássaros em 2019, que tornou-se tendência.")
    print("- Não há redução ou aumento do número de espécies observados por visita durante os cinco anos de dados coletados")
    print("- Possivelmente há uma regra limitando a quantidade de visitantes por grupo, o que pode ser percebido por terem poucos registros de grupos maiores do que 8 pessoas")
    print("02) Sugestão de ação: ")
    print("- Para maximizar a quantidade de pássaros avistados por grupo, prefira grupos de 2-5 integrantes")
except Exception as e:
    print(str(e))
