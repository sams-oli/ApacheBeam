import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
                    "id",
                    "data_iniSE",
                    "casos",
                    "ibge_code",
                    "cidade",
                    "uf",
                    "cep",
                    "latitude",
                    "longitude"]

def lista_dicionario(elemento, colunas):
    """
    Recebe 2 listas
    Retorna 1 dicionário
    """
    return dict(zip(colunas, elemento))

def texto_lista(elemento, delimitador = '|'):
    """
    Recebe um texto e um delimitador
    e retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def tratando_data(elemento):
    """
    Recebe um dicionário
    e cria um novo campo com ANO-MÊS
    Retorna o mesmo dicionario com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_estado(elemento):
    """
    Receber um dicionário
    Retornar uma tupla com estado e o elemento
    (UF, dicionário)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('UF', [{}, {}])
    Retorna uma tupla ('UF-ANO-MES', casos)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def lista_uf_ano_mes(elemento):
    """
    Recebe uma lista de elementos
    Retorna uma tupla contendo uma chave e o valor de chuva (mm)
    ('UF-ANO-MES', X.X(mm))
    """
    data, mm, uf =  elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

def arredonda(elemento):
    """
    Recebe um tupla
    Retorna uma tupla com o valor arredondado
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtro_nulos(elemento):
    """
    Remove elementos que tenham chaves vazias
    Recebe uma tupla
    Retorna uma tupla
    """
    chave, dados =  elemento

    if all([
        dados['dengue'],
        dados['chuvas']]):
        return True
    return False

def descompactar_elementos(elemento):
    """
    Recebe uma tupla
    Retorna uma tupla
    """
    chave, dados = elemento
    dengue = dados['dengue'][0]
    chuvas = dados['chuvas'][0]
    uf, ano, mes = chave.split('-')

    return uf, ano, mes, str(dengue), str(chuvas)

def preparar_csv(elemento, delimitador = ';'):
    """
    Recebe uma tupla
    Retorna uma string delimitada
    """
    return f"{delimitador}".join(elemento)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_lista)
    | "De lista para dicionário" >> beam.Map(lista_dicionario, colunas_dengue)
    | "Criar campo ano_mês" >> beam.Map(tratando_data)
    | "Criar chave pelo estado" >> beam.Map(chave_estado)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print))

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | "De texto para lista do dataset de chuvas" >> beam.Map(texto_lista, delimitador = ',')
    | "Criando a chave UF-ANO-MES" >> beam.Map(lista_uf_ano_mes)
    | "Soma dos mm de chuva pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de mm de chuva" >> beam.Map(arredonda)
    | "Mostrar resultados de mm de chuvas" >> beam.Map(print))

resultado = (
    ({'dengue': dengue, 'chuvas': chuvas})
    | "Mesclar pcols" >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtro_nulos)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar csv" >> beam.Map(preparar_csv)
    | "Mostrar resultados da união das pcollections" >> beam.Map(print))

header = 'UF;ANO;MES;DENGUE;CHUVA'

arquivo_csv | "Criar arquivo csv" >> WriteToText('resultado', file_name_suffix = '.csv', header = 'header')

pipeline.run()
