from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Upload File to Spark
url = "./database/operacoes_comerciais_inteira.csv"
rdd3 = sc.textFile(url)

# Remove o cabeçalho do arquivo
def remove_header(rdd):
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header)
    return rdd

# Question 1 - Número de transações envolvendo o Brasil através de RDD. O retorno deverá ser
# um único valor
def number_transactions_involving_brazil(rdd):
    remove_header(rdd)
    rdd = rdd.filter(lambda x: x.split(";")[0] == "Brazil").count()
    open("output/01_number_transactions_involving_brazil.txt", "w", encoding="utf-8").write(str(rdd))
    print("\033[32mNúmero de transações envolvendo o Brasil: \033[0m", rdd)

# Question 2 - Número de transações envolvendo o Brasil utilizando PairRDD. O retorno deverá
# ser composto de chave (Brazil) e valor
def number_transactions_involving_brazil_pair_rdd(rdd):
    remove_header(rdd)
    rdd = (rdd.map(lambda x: (x.split(";")[0], 1))
           .reduceByKey(lambda a, b: a + b)
           .filter(lambda x: x[0] == "Brazil")
           .collect())
    open("output/02_number_transactions_involving_brazil_pairRdd.txt", "w", encoding="utf-8").write(str(rdd))
    print("\033[33mNúmero de transações envolvendo o Brasil: \033[0m", rdd)

# Question 3 - Número de transações envolvendo o Brazil durante 2016 utilizando RDD. O
# retorno deverá ser um único valor
def number_transactions_involving_brazil_during_2016(rdd):
    remove_header(rdd)
    rdd = rdd.filter(lambda x: x.split(";")[0] == "Brazil" and x.split(";")[1] == "2016").count()
    open("output/03_number_transactions_involving_brazil_during_2016.txt", "w", encoding="utf-8").write(str(rdd))
    print("\033[34mNúmero de transações envolvendo o Brasil durante o ano de 2016: \033[0m", rdd)

# Question 4 - Número de transações envolvendo o Brazil durante 2016 utilizando PairRDD. O
# retorno deverá ser com chave (Brasil, 2016) e valor
def number_transactions_involving_brazil_during_2016_pair_rdd(rdd):
    rdd = remove_header(rdd)
    rdd = (rdd.filter(lambda x: x.split(";")[0] == "Brazil" and x.split(";")[1] == "2016")
             .map(lambda x: (x.split(";")[0], 1))
             .reduceByKey(lambda a, b: a + b)
             .collect())
    open('output/04_number_transactions_involving_brazil_during_2016_pairRdd.txt', 'w', encoding='utf-8').write(str(rdd))
    print("\033[35mNúmero de transações envolvendo o Brasil durante o ano de 2016: \033[0m", rdd)

# Question 5 - Número de transações por flow e por ano ordenados por ano a partir de 2010,
# transformando os valores da coluna Flow em maiúsculas. O retorno será em chave (ano, flow) e valor.
def number_transactions_by_flow_and_by_year_start_2010_and_transform_column_flow_values_in_uppercase(rdd):
    rdd = remove_header(rdd)
    rdd = (rdd.filter(lambda x: x.split(";")[1] >= "2010")
             .map(lambda x: ((x.split(";")[1], x.split(";")[4].upper()), 1))
             .reduceByKey(lambda a, b: a + b)
             .sortByKey()
             .collect())
    open('output/05_number_transactions_by_flow_and_by_year_start_2010.txt', 'w', encoding='utf-8').write(str(rdd))
    print(rdd)

# Question 6 - A média da coluna Price para o ano de 2016 utilizando RDD. O retorno deverá conter apenas o valor da média.
def average_price_for_2016_using_rdd(rdd):
    rdd = remove_header(rdd)
    rdd = (rdd.filter(lambda x: x.split(";")[1] == "2016")
             .map(lambda x: float(x.split(";")[5]))
             .mean())
    open('output/06_average_price_for_2016_rdd.txt', 'w', encoding='utf-8').write(str(rdd))
    print(rdd)

# Question 7 - A média da coluna Price para o ano de 2016 utilizando PairRDD. O retorno deverá
# conter chave (ano) e valor.
def average_price_for_2016_using_pair_rdd(rdd):
    rdd = remove_header(rdd)
    # Mapeia para (ano, (preço, 1))
    pair_rdd = (rdd.filter(lambda x: x.split(";")[1] == "2016")
                  .map(lambda x: (x.split(";")[1], (float(x.split(";")[5]), 1))))
    # Soma os preços e as contagens
    sum_count = pair_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # Calcula a média
    average = sum_count.mapValues(lambda x: x[0] / x[1]).collect()
    open('output/07_average_price_for_2016_pair_rdd.txt', 'w', encoding='utf-8').write(str(average))
    print(average)

# Question 8 - O preço máximo e mínimo por categoria e por ano, ordenado por país, sendo
# que a coluna categoria deve conter valores com letra maiúsculas. Utilizar PairRDD e a saída
# esperada deve conter: chave (ano, categoria) e valor (máximo, mínimo).
# Column 0 - Country
# Column 1 - Year
# Column 5 - Price
# Column 9 - Category
def max_and_min_price_by_category_and_by_year(rdd):
    rdd = remove_header(rdd)
    # Cria o par (ano, categoria) -> preço
    pair_rdd = rdd.map(lambda x: ((x.split(";")[1], x.split(";")[9].upper()), float(x.split(";")[5])))
    # Agrupa todos os preços por (ano, categoria)
    grouped = pair_rdd.groupByKey()
    # Calcula o máximo e mínimo para cada grupo
    result = grouped.mapValues(lambda prices: (max(prices), min(prices))).sortByKey().collect()
    open('output/08_max_and_min_price_by_category_and_by_year_pair_rdd.txt', 'w', encoding='utf-8').write(str(result))
    print(result)

# Question 9 - Retornar um único valor para PairRDD contendo o país com a maior exportação
# (Flow=Export). A saída esperada é chave(país, flow) e valor.
# Column 0 - Country
# Column 4 - Flow
def country_with_the_largest_export(rdd):
    rdd = remove_header(rdd)
    export_counts = (rdd.filter(lambda x: x.split(";")[4] == "Export")
                      .map(lambda x: (x.split(";")[0], 1))
                      .reduceByKey(lambda a, b: a + b)
                      .collect())
    # Pega o país com maior exportação
    max_country = max(export_counts, key=lambda x: x[1])
    # Formata como (país, flow), valor
    result = ((max_country[0], "Export"), max_country[1])
    open('output/09_country_with_the_largest_export_pair_rdd.txt', 'w', encoding='utf-8').write(str(result))
    print(result)

# Question 10 - Buscar o preço mínimo por país e por ano, filtrado de forma crescente por ano
# e país (ordem alfabética).
def min_price_by_country_and_by_year(rdd):
    rdd = remove_header(rdd)
    # Cria o par (ano, país) -> preço
    pair_rdd = rdd.map(lambda x: ((x.split(";")[1], x.split(";")[0]), float(x.split(";")[5])))
    # Agrupa todos os preços por (ano, país)
    grouped = pair_rdd.groupByKey()
    # Calcula o mínimo para cada grupo
    result = grouped.mapValues(lambda prices: min(prices)).sortByKey().collect()
    open('output/10_min_price_by_country_and_by_year_pair_rdd.txt', 'w', encoding='utf-8').write(str(result))
    print(result)

# Question 11 - Transação que teve o maior preço por kg do tipo export. Buscar essa informação
# e indicar em qual ano, país e categoria, ela aconteceu?
# Column 1 - Year
# Column 4 - Flow
# Column 0 - Country
# Column 9 - Category
# Column 5 - Price
# Column 6 - Weight
def transaction_with_the_highest_price_per_kg_of_the_export_type(rdd):
    rdd = remove_header(rdd)
    # Filtra as transações do tipo export e com peso válido (> 0)
    export_rdd = rdd.filter(
        lambda x: x.split(";")[4] == "Export" and
                  x.split(";")[6] not in ("", "0")
    )
    # Cria o par (ano, país, categoria) -> (preço por kg, preço)
    pair_rdd = export_rdd.map(
        lambda x: (
            (x.split(";")[1], x.split(";")[0], x.split(";")[9]),
            (float(x.split(";")[5]) / float(x.split(";")[6]), float(x.split(";")[5]))
        )
    )
    # Agrupa por (ano, país, categoria) e encontra o máximo pelo preço por kg
    grouped_rdd = pair_rdd.groupByKey()
    max_price_per_kg_rdd = grouped_rdd.mapValues(lambda x: max(x, key=lambda y: y[0]))
    # Encontra o máximo global
    max_price_per_kg_global = max_price_per_kg_rdd.max(key=lambda x: x[1][0])
    # Formata a saída
    result = (max_price_per_kg_global[0], max_price_per_kg_global[1][1], max_price_per_kg_global[1][0])
    open('output/11_transaction_with_the_highest_price_per_kg_of_the_export_type.txt', 'w', encoding='utf-8').write(str(result))
    print(result)

# Read CSV
# number_transactions_involving_brazil(rdd3)
# number_transactions_involving_brazil_pair_rdd(rdd3)
# number_transactions_involving_brazil_during_2016(rdd3)
# number_transactions_involving_brazil_during_2016_pair_rdd(rdd3)
# number_transactions_by_flow_and_by_year_start_2010_and_transform_column_flow_values_in_uppercase(rdd3)
# average_price_for_2016_using_rdd(rdd3)
# average_price_for_2016_using_pair_rdd(rdd3)
# max_and_min_price_by_category_and_by_year(rdd3)
# country_with_the_largest_export(rdd3)
# min_price_by_country_and_by_year(rdd3)
# transaction_with_the_highest_price_per_kg_of_the_export_type(rdd3)
