import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object googleAppsDataFrame {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = new SparkSession
      .Builder()
      .appName("googleAppsDataFrame")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/googleplaystore.csv")

    // print Schema of DataFrame
    println("Original Schema")
    println(df.printSchema())

    val updatedDF = df.withColumnRenamed("Content Rating", "Content_Rating")

    // Know distinct values for each column
//    df.select("Rating").distinct().collect().foreach(println)
//    df.select("Reviews").distinct().collect().foreach(println)
//    df.select("Size").distinct().collect().foreach(println)
//    df.select("Installs").distinct().collect().foreach(println)
//    df.select("Type").distinct().collect().foreach(println)
//    df.select("Price").distinct().collect().foreach(println)
//    df.select("Content Rating").distinct().collect().foreach(println)

    // udf for cleaning data
    val sizeUDF = udf[Float, String]{ (size: String) =>
                                        if (size.charAt(size.length - 1) == 'M') size.substring(0, size.length - 1).toFloat * 1024
                                        else size.substring(0, size.length - 1).toFloat
                                    }
    val installUDF = udf[Int, String]{ (install: String) => install.substring(0, install.length - 1).replace(",", "").toInt }
    val priceUDF = udf[Float, String]{ (price: String) =>
                                        if(price == "0") 0.0f
                                        else price.substring(1).toFloat
                                      }
    val ContentUDF = udf[String, String]{ (content: String) =>
                                          if(content == "Everyone") "Everyone 10+"
                                          else content
                                        }
    val updateUDF = udf[Int, String]{ (update: String) => update.split(',')(1).trim.toInt }

    // filter Df
    val filterDF = updatedDF
      .filter("Rating NOT IN ('NaN', 'navigation','Body') " +
        "AND Reviews NOT IN ('weight lose)\"','3.0M') " +
        "AND Size NOT IN ('MAPS_OR_NAVIGATION','Varies with device','Varies with devic') " +
        "AND Installs NOT IN ('Free', '4.2' ,'4.4') " +
        "AND Type NOT IN ('0' ,'102248' ,'NaN' ,'2509') " +
        "AND Price NOT IN ('5.0M' ,'Varies with device') " +
        "AND Content_Rating NOT IN ('5,000,000+' ,'1,000,000+') AND Content_Rating is Not Null")

    // create new DF with Data Cleaning
    val newDF = filterDF
      .select(
        col("App"),
        col("Rating").cast("float").as("Rating"),
        col("Reviews").cast("int").as("Reviews"),
        sizeUDF(col("Size")).as("Size"),
        installUDF(col("Installs")).as("Installs"),
        col("Type"),
        priceUDF(col("Price")).as("Price"),
        ContentUDF(col("Content_Rating")) as("Content_Rating"),
        col("Genres").as("Category"),
        updateUDF(col("Last Updated")).as("Year"),
        col("Current Ver").as("Version")
      ).cache()

    //print Schema After Data Cleaning
    println("Updated Schema")
    println(newDF.printSchema())


    // Q1 --> Most Installed App
    val q1Result = newDF.select("App", "Installs")
      .groupBy("App")
      .agg(sum("Installs").as("TotalInstalls"))
      .orderBy(col("TotalInstalls").desc)limit(10)

    println("Q1 --> Most Installed Apps are ")
    spark.time(q1Result.show())

    println()

    // Q2 --> Top Categories with number of apps
    val q2Result = newDF.select("Category")
      .groupBy("Category")
      .agg(count("Category").as("NumberOfApps"))
      .orderBy(col("NumberOfApps").desc).limit(20)

    println("Q2 --> Top Categories with number of apps are ")
    spark.time(q2Result.show())

    println()

    // Q3 --> Top Categories with number of Installation of all apps
    val q3Result = newDF.select("Category", "Installs")
      .groupBy("Category")
      .agg(sum("Installs").as("AllInstalls"))
      .orderBy(col("AllInstalls").desc).limit(20)

    println("Q3 --> Top Categories with number of Installation of all apps are ")
    spark.time(q3Result.show())

    println()

    // Q4 --> top Content_Rating with number of Apps
    val q4Result = newDF.select("Content_Rating")
      .groupBy("Content_Rating")
      .agg(count("Content_Rating").as("NumberOfApps"))
      .orderBy(col("NumberOfApps").desc).limit(20)

    println("Q4 --> Top Content_Rating with number of Apps are ")
    spark.time(q4Result.show())

    println()

    // Q5 --> Largest Apps
    val q5Result = newDF.select("App", "Size")
      .groupBy("App")
      .agg(max("Size").as("maxSize_k"))
      .orderBy(col("maxSize_k").desc).limit(20)

    println("Q5 --> Largest Apps are ")
    spark.time(q5Result.show())

    println()

    // Q6 --> Paid Vs Free count Apps
    val q6Result = newDF.select("Type")
      .groupBy("Type")
      .agg(count("Type").as("NumberOfApps"))
      .orderBy(col("NumberOfApps").desc).limit(20)

    println("Q6 --> Paid Vs Free count Apps are ")
    spark.time(q6Result.show())

    println()

    // Q7 --> Most Reviewed Apps
    val q7Result = newDF.select("App", "Reviews")
      .groupBy("App")
      .agg(sum("Reviews").as("TotalReviews"))
      .orderBy(col("TotalReviews").desc)limit(10)

    println("Q7 --> Most Reviewed Apps are ")
    spark.time(q7Result.show())

    println()

    // Q8 --> Average Size of Apps in Same Category
    val q8Result = newDF.select("Category", "Size")
      .groupBy("Category")
      .agg(avg("Size").as("AverageSize"),
        min("Size").as("MinSize"),
        max("Size").as("MaxSize"),
      )
      .orderBy(col("AverageSize").desc)limit(10)

    println("Q8 --> Average - Min - Max Size of Apps in Same Category are ")
    spark.time(q8Result.show())

    println()

    // Q9 --> Top App Installation in each Category
    val q9Result = newDF.select("Category", "App", "Installs")
      .groupBy("Category", "App")
      .agg(sum("Installs").as("TotalInstalls"))
      .withColumn("rank", dense_rank().over(Window.partitionBy("Category").orderBy(col("TotalInstalls").desc)))
      .filter(col("rank") === 1)
      .drop("rank")

    println("Q9 --> Top App Installation in each Category are ")
    spark.time(q9Result.show())

    println()

    // Q10 --> Number of Apps in Each Year
    val q10Result = newDF.select("Year")
      .groupBy("Year")
      .agg(count("Year").as("NumberOfApps"))
      .orderBy(col("Year").desc)

    println("Q10 --> Number of Apps in Each Year are ")
    spark.time(q10Result.show())

    println()

    // Q11 --> Average Price of Paid Apps
    val q11Result = newDF.select("Type", "Price")
      .filter(col("Type") === "Paid")
      .drop("Type")
      .agg(avg(col("Price")).as("AvgPrice"),
        sum(col("Price")).as("AllPrice"),
        count(col("Price")).as("count"),
        max(col("Price")).as("maxPrice"),
        min(col("Price")).as("minPrice"),
        mean(col("Price")).as("meanPrice"),
        stddev_pop(col("Price")).as("STDPrice")
      )

    println("Q11 --> Some Operation on Price of Paid Apps are")
    spark.time(q11Result.show())

    println()

    // Q12 --> Most Installed Category in each year
    val q12Result = newDF.select("Category", "Installs", "Year")
      .groupBy("Year", "Category")
      .agg(sum("Installs").as("TotalInstalls"))
      .withColumn("rank", dense_rank().over(Window.partitionBy("Year").orderBy(col("TotalInstalls").desc)))
      .filter(col("rank") === 1)
      .drop("rank")
      .orderBy("Year")

    println("Q12 --> Most Installed Category in each year are")
    spark.time(q12Result.show())

    println()

  }
}
