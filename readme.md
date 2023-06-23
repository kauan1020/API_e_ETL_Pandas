## Consumindo dados de uma API e ETL com Pandas

- A aplicação realizara o processo de ingestão dos dados da API e após isso o processo de  transformação, ao terminar continuará rodando devido ao scheduler que ficará aguardando todo dia 1 do mês para a verificação de novos dados presentes na API.

![diagram](png/diagram.png)

## Setup para executar a aplicação

1. Instale todas as bibliotecas necessárias para rodar a aplicação de ingestão com o comando:
```
pip install -r requirements.txt
```
2. Execute a aplicação `main.py`

