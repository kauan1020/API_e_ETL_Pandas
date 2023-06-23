## importando bibliotecas nescessárias
import schedule
import time
from datetime import datetime
import grequests
import re
import requests
import warnings
import urllib.parse
import pandas as pd


### ingestão dados via API limitando em 1000 a quantidade de dados por mes
def coletar_dados_e_executa_ETL():
    ## definindo warnings de usuario
    warnings.simplefilter("ignore", category=UserWarning)

    ## definindo url da API
    base_endpoint = 'https://opendata.nhsbsa.net/api/3/action/'
    package_list_method = 'package_list'
    package_show_method = 'package_show?id='
    action_method = 'datastore_search_sql?'

    ## chamar api para obter data sets
    datasets_response = requests.get(base_endpoint +  package_list_method).json()

    ## chamar data set solicitado nos requisitos do projeto
    dataset_id = "english-prescribing-data-epd"

    ## extraindo os metadados
    metadata_repsonse  = requests.get(f"{base_endpoint}" \
                                      f"{package_show_method}" \
                                      f"{dataset_id}").json()

    ## chamando tabelas de recursos
    resources_table  = pd.json_normalize(metadata_repsonse['result']['resources'])

    ## selecionando meses solicitados
    resource_name_list = resources_table[resources_table['name'].str.contains('202302|202301|202212')]['name']

    ## verificar se há dados novos disponiveis

    mes_to_df = pd.DataFrame(resources_table, columns= ['name'])
    max_mes = mes_to_df['name'].str[4:].astype(int).max()
    mes = pd.DataFrame(resource_name_list, columns= ['name'])
    max_value_req = mes['name'].str[4:].astype(int).max()

    if max_mes > max_value_req:
      max_mes1 = max_mes.astype(str)
      resource_name_list = resources_table[resources_table['name'].str.contains(f'202302|202301|{max_mes1}|202212')]['name']


    ## definindo query para trazer tabelas
    def query(resource_name):
        query = "SELECT * " \
                f"FROM `{resource_name}` " \
                f"LIMIT 10000"
        return (query)

    ## criando a chamada da api
    api_calls = []
    for x in resource_name_list:
        api_calls.append(
            f"{base_endpoint}" \
            f"{action_method}" \
            "resource_id=" \
            f"{x}" \
            "&" \
            "sql=" \
            f"{urllib.parse.quote(query(x))}"  # Encode spaces in the url
        )

    ## obtendo os resultados da api
    dd = (grequests.get(u) for u in api_calls)
    res = grequests.map(dd)

    ## validação dos dados extraidos
    for x in res:
        if x.ok:
            print(True)
        else:
            print(False)

    ## concatenando resultados e colocando em um df
    df = pd.DataFrame()

    for x in res:
        # Grab the response JSON as a temporary list
        tmp_response = x.json()

        # Extract records in the response to a temporary dataframe
        tmp_df = pd.json_normalize(tmp_response['result']['result']['records'])

        # Bind the temporary data to the main dataframe
        df = df._append(tmp_df)

    df.to_csv("df.csv", index=False)

    df = pd.read_csv('df.csv', sep=',', decimal=',')

    ## separar os dados em prescribers e prescriptions e escrever em formato csv

    prescribers = df[['PRACTICE_CODE', 'PRACTICE_NAME', 'ADDRESS_1', 'ADDRESS_2', 'ADDRESS_3', 'ADDRESS_4',
                      'POSTCODE']].drop_duplicates()
    prescriptions = df.drop(columns=['PRACTICE_NAME', 'ADDRESS_1', 'ADDRESS_2', 'ADDRESS_3', 'ADDRESS_4', 'POSTCODE'])

    prescribers.to_csv('prescribers.csv', index=False)
    prescriptions.to_csv('prescriptions.csv', index=False)

    ## Crie um dataframe contendo os 10 principais produtos químicos prescritos por região

    top10_quimicos = df.groupby('REGIONAL_OFFICE_NAME')['BNF_CHEMICAL_SUBSTANCE'].value_counts().groupby(
        level=0).nlargest(10)
    top10_quimicos.head()


    ## Quais produtos químicos prescritos tiveram a maior somatória de custos por mês? Liste os 10 primeiros

    grouped = df.groupby(['YEAR_MONTH', 'BNF_CHEMICAL_SUBSTANCE'])['NIC'].sum().reset_index()

    # Ordenar os dados pela soma dos custos em ordem decrescente

    sorted_data = grouped.sort_values(by='NIC', ascending=False)

    # Selecionar os 10 primeiros produtos químicos com maior soma de custos por mês

    top_10 = sorted_data.groupby('YEAR_MONTH').head(10)

    ## Quais são as precrições mais comuns?

    top_prescicoes = df['BNF_DESCRIPTION'].value_counts().nlargest(10)
    top_prescicoes.head()

    ## Qual produto químico é mais prescrito por cada prescriber?

    produto_por_prescritor = df.groupby('PRACTICE_NAME')['BNF_CHEMICAL_SUBSTANCE'].agg(lambda x: x.value_counts().index[0])
    produto_por_prescritor.head()

    ## Quantos prescribers foram adicionados no ultimo mês?

    prescritores_ultimo_mes = df[df['YEAR_MONTH'] == df['YEAR_MONTH'].max()]['PRACTICE_NAME'].nunique()
    print(prescritores_ultimo_mes)

    ## Quais prescribers atuam em mais de uma região? Ordene por quantidade de regiões antendidas.

    prescricoes_regiao = df.groupby('PRACTICE_NAME')['REGIONAL_OFFICE_NAME'].nunique().reset_index(name='count')
    prescricoes_regiao_atendidas = prescricoes_regiao[prescricoes_regiao['count'] > 1].sort_values('count',ascending=False)
    prescricoes_regiao_atendidas.head()

    ##  Qual o preço médio dos químicos prescritos em no ultimo mês coletado?

    df['NIC'] = pd.to_numeric(df['NIC'], errors='coerce')
    media_preco_ultimo_mes = df[df['YEAR_MONTH'] == df['YEAR_MONTH'].max()]['NIC'].mean()
    print(media_preco_ultimo_mes)

    ## Gere uma tabela que contenha apenas a prescrição de maior valor de cada usuário.

    df['ACTUAL_COST'] = pd.to_numeric(df['ACTUAL_COST'], errors='coerce')
    max_indices = df.groupby('PRACTICE_NAME')['ACTUAL_COST'].idxmax()
    maior_valor_usuario = df.loc[max_indices]
    maior_valor_usuario.head()

# função para agendar a execução mensalmente
def agendar_atualizacao_mensal():
    # agendar a execução da função coletar_dados no primeiro dia de cada mês às 00:00
    schedule.every().day.at("00:00").do(coletar_dados_e_executa_ETL)

# executar imediatamente
coletar_dados_e_executa_ETL()

# agendar a execução mensalmente dia 1
if datetime.today().day == 1:
    agendar_atualizacao_mensal()

print(datetime.today().day)

# loop para manter o programa em execução
while True:
    schedule.run_pending()
    time.sleep(1)



