# tec_conf
### Pacotes importados

rom pyspark.sql.functions import split\
from pyspark.sql.functions import lit, col, column, expr, udf, when\
from pyspark.sql.functions import datediff, to_date\
from pyspark.sql.types import *\
from pyspark.sql.functions import *\
from datetime import datetime\
from functools import reduce\
import pyspark.sql.functions as F\
from pyspark.sql.functions import current_date\
from pyspark.sql.types import IntegerType,BooleanType,DateType\
from pyspark.sql.functions import from_unixtime, unix_timestamp\
from pyspark.sql.functions import split\

### Carregar arquivo

path_dataset = "/FileStore/tables/OriginaisNetflix.parquet"\
file_type = "parquet"\
infer_schema = "true"\
first_row_is_header = "true"\
delimiter = ";"\

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(path_dataset)\

df.show(5)\
#display(df)

### 1. Transformar os campos "Premiere" e "dt_inclusao" de string para datetime.
#### Usado "split" para separar a coluna "Premiere" em Dia, Mês e Ano
f1 = df.withColumn("Premiere1_Dia", split(col("Premiere"), "-").getItem(0))\
.withColumn("Premiere2_Mes", split(col("Premiere"), "-").getItem(1))\
.withColumn("Premiere3_Ano", split(col("Premiere"), "-").getItem(2))\

#### Gerada nova coluna convertendo o mês de "Jan" para "01" possibilitando particionar a informação dos "dados" conforme coluna "Premiere". 
df_1 = df1.withColumn('Premiere2_Mes', 
                      when(df1.Premiere2_Mes.endswith('Jan'),regexp_replace(df1.Premiere2_Mes,'Jan','01')) \
                     .when(df1.Premiere2_Mes.endswith('Feb'),regexp_replace(df1.Premiere2_Mes,'Feb','02')) \
                     .when(df1.Premiere2_Mes.endswith('Mar'),regexp_replace(df1.Premiere2_Mes,'Mar','03')) \
                     .when(df1.Premiere2_Mes.endswith('Apr'),regexp_replace(df1.Premiere2_Mes,'Apr','04')) \
                     .when(df1.Premiere2_Mes.endswith('May'),regexp_replace(df1.Premiere2_Mes,'May','05')) \
                     .when(df1.Premiere2_Mes.endswith('Jun'),regexp_replace(df1.Premiere2_Mes,'Jun','06')) \
                     .when(df1.Premiere2_Mes.endswith('Jul'),regexp_replace(df1.Premiere2_Mes,'Jul','07')) \
                     .when(df1.Premiere2_Mes.endswith('Aug'),regexp_replace(df1.Premiere2_Mes,'Aug','08')) \
                     .when(df1.Premiere2_Mes.endswith('Sep'),regexp_replace(df1.Premiere2_Mes,'Sep','09')) \
                     .when(df1.Premiere2_Mes.endswith('Oct'),regexp_replace(df1.Premiere2_Mes,'Oct','10')) \
                     .when(df1.Premiere2_Mes.endswith('Nov'),regexp_replace(df1.Premiere2_Mes,'Nov','11')) \
                     .when(df1.Premiere2_Mes.endswith('Dec'),regexp_replace(df1.Premiere2_Mes,'Dec','12')).otherwise(df1.Premiere2_Mes))
                     
#### Concatenar o resultado "df_1", usando "Premiere1_Dia", "Premiere2_Mes", "Premiere2_Mes" com o resultado FullPremiere.
df_11 = df_1.withColumn("FullPremiere",concat(col("Premiere1_Dia"),lit('-'),col("Premiere2_Mes"),lit('-'),col("Premiere3_Ano")))

#### Converter campos tipo DataType
f_111 = df_11.withColumn("Premiere",col("Premiere").cast(DateType())) \
    .withColumn("dt_inclusao",col("dt_inclusao").cast(DateType())) \
    .withColumn("FullPremiere",col("FullPremiere").cast(DateType()))
    
### 2. Ordenar os dados por ativos e gênero de forma decrescente, 0 = inativo e 1 = ativo, todos com número 1 devem aparecer primeiro.

df2 = df_11.sort(col("Active").desc(),col("Genre").desc())\
df2.show(2)
#display(df2)

### 3. Remover linhas duplicadas e trocar o resultado das linhas que tiverem a coluna "Seasons" de "TBA" para "a ser anunciado".
df3 = df2.dropDuplicates()\
print("Distinct Seasons: " + str(df3.count()))\
order = df3.sort(col("Active").desc(),col("Genre").desc())\
order.show(2)\
#display(order)

#### Gerar uma coluna onde a coluna "Seasons" é igual "TBA" e será alterado para "a ser anunciado"
order1 = order.withColumn("Seasons", when(order.Seasons == "TBA","a ser anunciado").otherwise(order.Seasons))
order1.show(2)
#display(order1)

### Filtro para identificar volume modificado.
filtro = order1.filter(order1.Seasons == "a ser anunciado")\
filtro.show(2)\
#display(filtro)

#### 4. Criar uma coluna nova chamada "Data de Alteração" e dentro dela um timestamp.

df4 = order1.withColumn("Data de Alteração", to_timestamp(current_timestamp(),"MM-dd-yyyy hh mm a"))\
      .withColumnRenamed("Data de Alteração","Data_de_Alteração")\
#df4.show(1,truncate=False)\
df4.show(2)\
#display(df4)

#### 5. Trocar os nomes das colunas de inglês para português, exemplo: "Title" para "Título" (com acentuação).

oldColumns = df4.schema.names\
newColumns = ["Título","Gênero","GenreLabels","Pré estreia","Temporadas","SeasonsParsed","Episódios analisados"\
              ,"Comprimento","Comprimento mínimo","Comprimento máximo","Status","Ativo","Mesa","Língua","dt_inclusao"\
              ,"Premiere1","Premiere2","Premiere3","FullPremiere","Data_de_Alteração"]\
df11 = reduce(lambda df4, colunas: df4.withColumnRenamed(oldColumns[colunas], newColumns[colunas]), range(len(oldColumns)), df4)\
df11.show(2)\
#display(df11)

#### 6. Testar e verificar se existe algum erro de processamento do spark e identificar onde pode ter ocorrido o erro.

#### Gera tabela Temporária
ata = df11.registerTempTable("data")\
output = spark.sql("SELECT * FROM data")\
output.show(2)
#display(output)

#### 7. Criar apenas 1 . csv com as seguintes colunas que foram nomeadas anteriormente "Title, Genre, Seasons, Premiere, Language, Active, Status, dt_inclusao, Data de Alteração" as colunas devem estar em português com header e separadas por ";".

### Métodos para gerar .CSV
output.write.format("csv").mode("overwrite").option("sep","\t").save("/opt/intellij/spark_datagree/data/my-tsv-file.csv")\
output.write.format("csv").save("/FileStore/tables/mydata_3.csv").option("sep", ";").option("header", "true")\
output.write.format("csv").mode("overwrite").option("sep","\t").save("/opt/intellij/spark_datagree/data/my-tsv-file.csv")
