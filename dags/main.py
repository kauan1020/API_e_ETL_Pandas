## importando bibliotecas nescessárias
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import pandas as pd
import grequests
import re
import requests
import warnings
import urllib.parse

## definindo argumentos para criação da dag do airflow
default_args = {
    'owner': 'kauan silva',
    'depends_on_past': False,
    'email': ['kauan1020@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': False
}

## criando dag

with DAG(
    dag_id="2RP-teste-01",
    tags=['development','teste','pandas'],
    default_args=default_args,
    start_date=datetime(year=2023, month=5, day=21),
    schedule_interval='@monthly',
    catchup=False
) as dag:


    # ingestão dados via API limitando

    def ingestao_api():
        ## definindo warnings de usuario
        warnings.simplefilter("ignore", category=UserWarning)

        ## definindo url da API
        base_endpoint = 'https://opendata.nhsbsa.net/api/3/action/'
        package_list_method = 'package_list'     # List of data-sets in the portal
        package_show_method = 'package_show?id=' # List all resources of a data-set
        action_method = 'datastore_search_sql?'  # SQL action method

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

        ## concatenando resultados e colocando em um df
        df = pd.DataFrame()

        for x in res:
            # Grab the response JSON as a temporary list
            tmp_response = x.json()

            # Extract records in the response to a temporary dataframe
            tmp_df = pd.json_normalize(tmp_response['result']['result']['records'])

            # Bind the temporary data to the main dataframe
            df = df._append(tmp_df)

    ingestao_api = PythonOperator(
        task_id='ingestao_api',
        python_callable=ingestao_api(),
        dag=dag
    )

    ## separar os dados em prescribers e prescriptions e escrever em formato csv
    def separar_prescibers_prescriptions():
        prescribers = df[['PRACTICE_CODE', 'PRACTICE_NAME', 'ADDRESS_1', 'ADDRESS_2', 'ADDRESS_3', 'ADDRESS_4',
                          'POSTCODE']].drop_duplicates()
        prescriptions = df.drop(columns=['PRACTICE_NAME', 'ADDRESS_1', 'ADDRESS_2', 'ADDRESS_3', 'ADDRESS_4', 'POSTCODE'])

        prescribers.to_csv('prescribers.csv', index=False)
        prescriptions.to_csv('prescriptions.csv', index=False)

    separar_prescibers_prescriptions = PythonOperator(
        task_id='separar_prescibers_prescriptions',
        python_callable=separar_prescibers_prescriptions(),
        dag=dag
    )

    ## Crie um dataframe contendo os 10 principais produtos químicos prescritos por região
    def top10_quimicos():
        top10_quimicos = df.groupby('REGIONAL_OFFICE_NAME')['BNF_CHEMICAL_SUBSTANCE'].value_counts().groupby(
            level=0).nlargest(10)
        top10_quimicos.head()

    top10_quimicos = PythonOperator(
        task_id='top10_quimicos',
        python_callable=top10_quimicos(),
        dag=dag
    )

    ## Quais produtos químicos prescritos tiveram a maior somatória de custos por mês? Liste os 10 primeiros.

    def top_custos():
        top_custos = df.groupby(['YEAR_MONTH', 'BNF_CHEMICAL_SUBSTANCE'])['ACTUAL_COST'].sum().nlargest(10)
        top_custos.head()

    top_custos = PythonOperator(
        task_id= 'top_custos',
        python_callable=top10_quimicos(),
        dag=dag
    )

    ## Quais são as precrições mais comuns?

    def top_prescicoes():
        top_prescicoes = df['BNF_DESCRIPTION'].value_counts().nlargest(10)
        top_prescicoes.head()

    top_prescicoes = PythonOperator(
        task_id= 'top_prescicoes',
        python_callable=top_prescicoes(),
        dag=dag
    )

    ## Qual produto químico é mais prescrito por cada prescriber?

    def produto_por_prescritor():
        produto_por_prescritor = df.groupby('PRACTICE_NAME')['BNF_CHEMICAL_SUBSTANCE'].agg(lambda x: x.value_counts().index[0])
        produto_por_prescritor.head()


    produto_por_prescritor = PythonOperator(
        task_id='produto_por_prescritor',
        python_callable=produto_por_prescritor(),
        dag=dag
    )

    ## Quantos prescribers foram adicionados no ultimo mês?

    def prescritores_ultimo_mes():

        prescritores_ultimo_mes = df[df['YEAR_MONTH'] == df['YEAR_MONTH'].max()]['PRACTICE_NAME'].nunique()
        prescritores_ultimo_mes.head()


    prescritores_ultimo_mes = PythonOperator(
        task_id='prescritores_ultimo_mes',
        python_callable=prescritores_ultimo_mes,
        dag=dag
    )

    ## Quais prescribers atuam em mais de uma região? Ordene por quantidade de regiões antendidas.

    def prescricoes_regiao_atendidas():
        prescricoes_regiao = df.groupby('PRACTICE_NAME')['REGIONAL_OFFICE_NAME'].nunique().reset_index(name='count')
        prescricoes_regiao_atendidas = prescricoes_regiao[prescricoes_regiao['count'] > 1].sort_values('count',ascending=False)
        prescricoes_regiao_atendidas.head()

    prescricoes_regiao_atendidas = PythonOperator(
        task_id=' prescricoes_regiao_atendidas',
        python_callable= prescricoes_regiao_atendidas(),
        dag=dag
    )

    ##  Qual o preço médio dos químicos prescritos em no ultimo mês coletado?

    def media_preco_ultimo_mes():
        media_preco_ultimo_mes = df[df['YEAR_MONTH'] == df['YEAR_MONTH'].max()]['ACTUAL_COST'].mean()
        media_preco_ultimo_mes.head()

    media_preco_ultimo_mes = PythonOperator(
        task_id='media_preco_ultimo_mes',
        python_callable=media_preco_ultimo_mes,
        dag=dag
    )

    ## Gere uma tabela que contenha apenas a prescrição de maior valor de cada usuário.

    def maior_valor_usuario():
        max_indices = df.groupby('PRACTICE_NAME')['ACTUAL_COST'].idxmax()
        maior_valor_usuario = df.loc[max_indices]
        maior_valor_usuario.head()


    maior_valor_usuario = PythonOperator(
        task_id='maior_valor_usuario',
        python_callable=maior_valor_usuario,
        dag=dag
    )

