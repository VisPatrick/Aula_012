from datetime import datetime
import pandas as pd
import polars as pl # biblioteca Polars, utilizada para grandes volumes de dados

#   pandas vs polars
#   pandas: 21s
#   polars:  6s

#   OBTENDO OS DADOS

try:
    ENDERECO_DADOS = r'../dados/'

    hora_inicio = datetime.now()
    print('Carregando... ')

#   PANDAS
#     df_janeiro = pd.read_csv(ENDERECO_DADOS + '202501_NovoBolsaFamilia.csv', sep=';', encoding='iso-8859-1')
#     print("\n", df_janeiro.head())

#   POLARS
    df_janeiro = pl.read_csv(ENDERECO_DADOS + '202501_NovoBolsaFamilia.csv', separator=';', encoding='iso-8859-1')
    print("\n", df_janeiro.head())

#    TEMPO FINAL
    hora_fim = datetime.now()
    print(30*'-')
    print('Tempo total:', hora_fim - hora_inicio)

except Exception as e:
    print(f'Erro ao obter dados:, {e}')
