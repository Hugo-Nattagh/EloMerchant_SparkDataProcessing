import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object HSpark {

  def main(args: Array[String]) {
    // Hide end warnings/infos at the end of build. (En local)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Initialize Session
    val spark = SparkSession.builder().appName("SparkSQL For CSV").master("spark://master:7077").getOrCreate()

    //Import csv file
    /*val dfi = spark.read.option("header","true").option("delimiter",",").csv("C:/Users/Hugo/Desktop/Kaggle/Elo/loops/dfi_loops.csv")
    val df_train = spark.read.option("header","true").option("delimiter",",").csv("C:/Users/Hugo/Desktop/Kaggle/Elo/loops/dftrain_loops.csv")
    val df_test = spark.read.option("header","true").option("delimiter",",").csv("C:/Users/Hugo/Desktop/Kaggle/Elo/loops/dftest_loops.csv")*/

    val dfi = spark.read.option("header","true").option("delimiter",",").csv("hdfs:///user/maria_dev/elo/loops/dfi_loops.csv")
    //val df_train = spark.read.option("header","true").option("delimiter",",").csv("hdfs:///user/maria_dev/elo/loops/dftrain_loops.csv")
    //val df_test = spark.read.option("header","true").option("delimiter",",").csv("hdfs:///user/maria_dev/elo/loops/dftest_loops.csv")

    val dfiLength = dfi.count().asInstanceOf[Int]
    val df_index = dfi.withColumn("id", monotonically_increasing_id)
    df_index.createOrReplaceTempView("tab")
    val allDates = Array("2017-01","2017-02","2017-03", "2017-04", "2017-05", "2017-06","2017-07","2017-08", "2017-09", "2017-10", "2017-11","2017-12","2018-01", "2018-02", "2018-03", "2018-04")
    val lenDates = allDates.size
    var listForDF = new Array[Array[mutable.HashMap[String, Any]]](lenDates)
    var listForJoin = new Array[DataFrame](lenDates)
    //val tryout = spark.sql("select purchase_amount from tab where id = 7").collect()(0)(0)

    for (dates <- 0 to lenDates-1) {
      println(dates, lenDates-1)
      var curDate = allDates(dates)
      var specList = new Array[mutable.HashMap[String, Any]](dfiLength)
      for (row <- 0 to dfiLength-1) {
        var rowS = row.toString
        println(row, dfiLength-1)
        if (curDate == spark.sql("select purchase_date from tab where id = " + rowS).collect()(0)(0)) {
          var amount = spark.sql("select purchase_amount from tab where id = " + rowS).collect()(0)(0)
          var c_id = spark.sql("select card_id from tab where id = " + rowS).collect()(0)(0)
          var hashMapVar: mutable.HashMap[String, Any] = mutable.HashMap(("card_id",c_id),(curDate,amount))
          specList(row) = hashMapVar
        } else {
          specList.patch(row, Nil, 1)
        }
      }
      listForDF(dates) = specList
      println(listForDF.deep.mkString("\n"))
    }

    for (li <- 0 to lenDates-1) {
      if (listForDF(li) != null) {
        // Work in pySpark, not here... List of dictionnaries in Python != Array of hashmaps in Scala...
        // Go over it later
        //listForJoin(li) = spark.sparkContext.parallelize(listForDF(li)).toDF()
      }
    }

    // Following is unchecked
    var findf = listForJoin(0)
    for (dfs <- listForJoin) {
      if (dfs != findf) {
        findf = findf.join(dfs, col("card_id") === col("card_id"))
      }
    }

    findf.show(20)

    findf.write.format("csv").option("header", "true").save("hdfs:///user/maria_dev/elo/outputEloScala.csv")

  }
}
