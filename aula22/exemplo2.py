from datetime import datetime
import pandas as pd
# import polars as pl # biblioteca Polars, utilizada para grandes volumes de dados

#   OBTENDO OS DADOS
try:
    ENDERECO_DADOS = r'../dados/'

    hora_inicio = datetime.now()

    lista_arquivos = ['202501_NovoBolsaFamilia.csv',
                      '202502_NovoBolsaFamilia.csv',
                      '202503_NovoBolsaFamilia.csv',
                      '202504_NovoBolsaFamilia.csv',
                      '202505_NovoBolsaFamilia.csv']

    df_bolsa_familia = None

    for arquivo in lista_arquivos:
        print(f'Carregando o arquivo {arquivo}... ')

# #       PANDAS # 5:33s
        df = pd.read_csv(ENDERECO_DADOS + arquivo, sep=';', encoding='iso-8859-1')
        if df_bolsa_familia is None:
            df_bolsa_familia = df
        else:
            df_bolsa_familia = pd.concat([df_bolsa_familia, df])

        print('\n', df)
        print(df.shape)

        del df

# #   POLARS # 40s
#         df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')
#         if df_bolsa_familia is None:
#             df_bolsa_familia = df
#         else:
#             df_bolsa_familia = pl.concat([df_bolsa_familia, df])

#         print('\n', df)
#         print(df.shape)

#         del df

    print('\nBolsa Família conectando')
    print(30*'-')
    print(df_bolsa_familia)
    print(df_bolsa_familia.shape)


    hora_final = datetime.now()
    print(f'Tempo gasto: {hora_final - hora_inicio}')


except Exception as e:
    print(f'Erro ao obter dados:, {e}')


#   paralelismo, dask, polars, pyspark: permite executar em mais de um core
#   pseudoparalelismo e multithreading: threads, asyncio. essenciais para o cientista de dados
#   pepiline de dado - coleta, pré processamento, processamento, armazenamento, visualização
#   docker, kubernetes, airflow, mlflow, git, github