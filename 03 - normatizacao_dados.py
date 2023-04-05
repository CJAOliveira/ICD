# Bibliotecas
from azure.storage.blob import BlobClient
import pandas as pd
from io import StringIO

def start():
    # Chave de conexao
    cadeia_conexao = 'DefaultEndpointsProtocol=https;AccountName=storageaccount1404933;AccountKey=EsgiXbmU4j9C9x0VLPwTdNlflwFmYYk5U/COwnJ4NrF3iMX5zQ8SLkh3NXHRn6rDZroisJpSnuHV+AStJQJBrA==;EndpointSuffix=core.windows.net'

    # Acessando os dados da camada RAW
    dados_exames = 'https://storageaccount1404933.blob.core.windows.net/datalake-aulas/origem_XPTO/DADOS_EXAMES/dados_exames.csv'

    # Exportando para um CSV
    df_dados_exames = pd.read_csv(dados_exames, sep=';')

    # NORMALIZAR_DADOS
    df_dados_exames['IdUnidadeAtendimento'] = df_dados_exames['IdUnidadeAtendimento'].astype('int64')
    df_dados_exames['Cidade'] = df_dados_exames['Cidade'].astype('string')
    df_dados_exames['Estado'] = df_dados_exames['Estado'].astype('string')
    df_dados_exames['NumPedido'] = df_dados_exames['NumPedido'].astype('string')
    df_dados_exames['IdExame'] = df_dados_exames['IdExame'].astype('int64')
    df_dados_exames['Exame'] = df_dados_exames['Exame'].astype('string')
    df_dados_exames['SiglaExame'] = df_dados_exames['SiglaExame'].astype('string')
    df_dados_exames['Material'] = df_dados_exames['Material'].astype('string')
    df_dados_exames['SetorExame'] = df_dados_exames['SetorExame'].astype('string')
    df_dados_exames['PrecoExame'] = df_dados_exames['PrecoExame'].astype('string')
    df_dados_exames['DataPedido'] = df_dados_exames['DataPedido'].astype('datetime64')
    df_dados_exames['QtdExames'] = df_dados_exames['QtdExames'].astype('int64')
    df_dados_exames['QtdAmostrasColhidas'] = df_dados_exames['QtdAmostrasColhidas'].astype('int64')
    df_dados_exames['DataPrevistaEntregaResultado'] = df_dados_exames['DataPrevistaEntregaResultado'].astype('datetime64')
    df_dados_exames['DataLiberacaoResultado'] = df_dados_exames['DataLiberacaoResultado'].astype('datetime64')

    # Dados_Pedidos_Exames
    arquivo_pedido_exames = df_dados_exames[['NumPedido', 'Cidade', 'Estado', 'IdUnidadeAtendimento', 'IdExame']]
    arquivo_pedido_exames_distinct = arquivo_pedido_exames.drop_duplicates(subset='NumPedido')

    #Tipos de Exames
    arquivo_exames = df_dados_exames[['IdExame', 'Exame', 'SiglaExame', 'Material'
                                        ,'SetorExame', 'PrecoExame']]
    arquivo_exames_distinct = arquivo_exames.drop_duplicates(subset=['IdExame', 'PrecoExame'])

    #Detalhe Exames
    arquivo_pedidos_detalhe = df_dados_exames[['NumPedido', 'DataPedido', 'QtdExames', 'QtdAmostrasColhidas'
                                                    ,'DataPrevistaEntregaResultado', 'DataLiberacaoResultado']]

    # importar o DF sem precisar salvar ele na maquina local
    # from io import StringIO
    def csv_temp(arquivo):
        csv_data = StringIO()
        arquivo.to_csv(csv_data, index=True)
        return csv_data


    # Dados Pedidos Exames
    arquivo_blob = 'meu_Repositorio/DADOS_EXAMES/pedidos/pedidos_exames.csv'
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-aulas", blob_name=arquivo_blob, timeout=120)
    blob.upload_blob(csv_temp(arquivo_pedido_exames_distinct).getvalue(), overwrite=True)

    # Tipo Exame
    arquivo_blob = 'meu_Repositorio/DADOS_EXAMES/exames/tipo_exames.csv'
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-aulas", blob_name=arquivo_blob, timeout=120)
    blob.upload_blob(csv_temp(arquivo_exames_distinct).getvalue(), overwrite=True)

    # Detalhe Exame
    arquivo_blob = 'meu_Repositorio/DADOS_EXAMES/detalhes_exames/detalhe_exames.csv'
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-aulas", blob_name=arquivo_blob, timeout=120)
    blob.upload_blob(csv_temp(arquivo_pedidos_detalhe).getvalue(), overwrite=True)


if __name__ == "__main__":
    start()
