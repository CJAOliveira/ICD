# Bibliotecas
from azure.storage.blob import BlobClient
import pandas as pd
from io import StringIO

def start():
    # Chave de conexao
    cadeia_conexao = 'cadeia_conexao'

    # Acessando os dados a serem importados
    detalhe_exames = 'https://storageaccount1404933.blob.core.windows.net/datalake-aulas/consume/DW_Exames/dados_exames/detalhes_exames/detalhe_exames.csv'
    tipo_exames = 'https://storageaccount1404933.blob.core.windows.net/datalake-aulas/consume/DW_Exames/dados_exames/exames/tipo_exames.csv'
    pedidos_exames = 'https://storageaccount1404933.blob.core.windows.net/datalake-aulas/consume/DW_Exames/dados_exames/pedidos/pedidos_exames.csv'


    # Exportando para um CSV
    df_detalhe_exames = pd.read_csv(detalhe_exames, sep=';')
    df_tipo_exames = pd.read_csv(tipo_exames, sep=';')
    df_pedidos_exames = pd.read_csv(pedidos_exames, sep=';')

    # importar o DF sem precisar salvar ele na maquina local
    # from io import StringIO
    def csv_temp(arquivo):
        csv_data = StringIO()
        arquivo.to_csv(csv_data, index=True)
        return csv_data


    # Dados Pedidos Exames
    arquivo_blob = 'consume/DW_Exames/dados_exames/detalhes_exames/detalhe_exames.csv'
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-aulas", blob_name=arquivo_blob, timeout=300)
    blob.upload_blob(csv_temp(df_detalhe_exames).getvalue(), overwrite=True)

    arquivo_blob = 'consume/DW_Exames/dados_exames/exames/tipo_exames.csv'
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-aulas", blob_name=arquivo_blob, timeout=300)
    blob.upload_blob(csv_temp(df_tipo_exames).getvalue(), overwrite=True)

    arquivo_blob = 'consume/DW_Exames/dados_exames/pedidos/pedidos_exames.csv'
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-aulas", blob_name=arquivo_blob, timeout=300)
    blob.upload_blob(csv_temp(df_pedidos_exames).getvalue(), overwrite=True)


if __name__ == "__main__":
    start()
