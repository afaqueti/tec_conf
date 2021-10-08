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

df.show(5)
#display(df)
