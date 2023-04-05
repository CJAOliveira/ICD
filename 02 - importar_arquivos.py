# Bibliotecas
from azure.storage.blob import BlobClient
import pandas as pd
from io import StringIO

def start():
    # Chave de conexao
    cadeia_conexao = 'cadeia_conexao'

    # Acessando os dados a serem importados
    dados_exames = 'https://storageaccount1404933.blob.core.windows.net/datalake-aulas/origem_XPTO/DADOS_EXAMES/dados_exames.csv'

    # Exportando para um CSV
    df_dados_exames = pd.read_csv(dados_exames, sep=';')

    # importar o DF sem precisar salvar ele na maquina local
    # from io import StringIO
    def csv_temp(arquivo):
        csv_data = StringIO()
        arquivo.to_csv(csv_data, index=True)
        return csv_data


    # Dados Pedidos Exames
    arquivo_blob = 'raw/DADOS_EXAMES/dados_exames.csv'
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-aulas", blob_name=arquivo_blob, timeout=300)
    blob.upload_blob(csv_temp(df_dados_exames).getvalue(), overwrite=True)


if __name__ == "__main__":
    start()
