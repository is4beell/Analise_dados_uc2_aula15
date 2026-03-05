import polars as pl
from datetime import datetime

ENDERECO_DADOS = './../DADOS/PARQUET/NovoBolsaFamilia/'

try:
    print('Iniciando o processamento Lazy()')
    inicio = datetime.now()

    with pl.StringCache():

        # Metodo Lazy "scan_parquet" cria um plano de execução, não carregando TODOS os dados 
        #diretamente na memoria, porém o plano é implementado
        lazy_plan = (
            pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
            .select(['NOME MUNICÍPIO', 'VALOR PARCELA'])
            .with_columns([
                pl.col('NOME MUNICÍPIO').cast(pl.Categorical)
            ])
            .group_by('NOME MUNICÍPIO')
            .agg(pl.col('VALOR PARCELA').sum())
            .sort('VALOR PARCELA', descending=True)
        )

    # print(lazy_plan)
    #collect() executa o plano de execução. Ele realmente traz os dados 
    df_bolsa_familia = lazy_plan.collect()

    print(df_bolsa_familia.head(10))

    fim = datetime.now()
    print(f'Tempo de execução: {fim - inicio}')
    print('Leitura do arquivo parquet')

except Exception as e:
    print('Erro ao obter dados')    