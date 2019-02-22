# EloMerchant_SparkDataProcessing

![](https://raw.githubusercontent.com/Hugo-Nattagh/EloMerchant_SparkDataProcessing/master/src/main/resources/elo-logo.png)

### Processing Datasets from  the [Elo kaggle competition](https://www.kaggle.com/c/elo-merchant-category-recommendation)

The goal here is basically to get a pivot table from a large dataset.

### Bits of the Scala Script


Initiating Spark:
```
val spark = SparkSession.builder().appName("SparkSQL For CSV").master("spark://master:7077").getOrCreate()
```

Adding an index to the Spark DataFrame, and creating a table from it to be able to use SQL on it:
```
val df_index = dfi.withColumn("id", monotonically_increasing_id)
df_index.createOrReplaceTempView("tab")
```

Example of how to get a single value from the dataframe returned by the SQL query:
```
//val tryout = spark.sql("select purchase_amount from tab where id = 7").collect()(0)(0)
```

Full code in `src/main/scala/HSpark.scala`
