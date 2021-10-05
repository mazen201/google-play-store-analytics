import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object googleAppsSql {
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
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Ver")

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

    // Register UDFs
    spark.udf.register("sizeUDF", sizeUDF)
    spark.udf.register("installUDF", installUDF)
    spark.udf.register("priceUDF", priceUDF)
    spark.udf.register("ContentUDF", ContentUDF)
    spark.udf.register("updateUDF", updateUDF)

    // create Temp View
    filterDF.createOrReplaceTempView("df")

    // create new DF with Data Cleaning
    val newDF = spark.sql(
      "SELECT App, CAST(Rating AS float) as Rating, CAST(Reviews AS int) as Reviews, " +
        "sizeUDF(Size) as Size, installUDF(Installs) as Installs, " +
        "Type, priceUDF(Price) as Price, ContentUDF(Content_Rating) as Content_Rating, " +
        "Genres as Category, updateUDF(Last_Updated) as Year, Current_Ver as Version " +
        "FROM df"
    ).cache()

    //print Schema After Data Cleaning
    println("Updated Schema")
    println(newDF.printSchema())

    // create Temp View for our Dataset
    newDF.createOrReplaceTempView("apps")

    // Q1 --> Most Installed App
    val q1Result = spark.sql(
      "select App, Sum(Installs) as TotalInstalls " +
        "from apps " +
        "group by App " +
        "order by TotalInstalls desc"
    )
    spark.time(q1Result.show())
    println()

    // Q2 --> Top Categories with number of apps
    val q2Result = spark.sql(
      "select Category, count(*) as NumberOfApps " +
        "from apps " +
        "group by Category " +
        "order by NumberOfApps desc"
    )

    println("Q2 --> Top Categories with number of apps are ")
    spark.time(q2Result.show())
    println()

    // Q3 --> Top Categories with number of Installation of all apps
    val q3Result = spark.sql(
      "select Category, sum(Installs) as AllInstalls " +
        "from apps " +
        "group by Category " +
        "order by AllInstalls desc"
    )

    println("Q3 --> Top Categories with number of Installation of all apps are ")
    spark.time(q3Result.show())
    println()

    // Q4 --> top Content_Rating with number of Apps
    val q4Result = spark.sql(
      "select Content_Rating, count(Content_Rating) as NumberOfApps " +
        "from apps " +
        "group by Content_Rating " +
        "order by NumberOfApps desc"
    )

    println("Q4 --> Top Content_Rating with number of Apps are ")
    spark.time(q4Result.show())
    println()

    // Q5 --> Largest Apps
    val q5Result = spark.sql(
      "select App, max(Size) as maxSize_k " +
        "from apps " +
        "group by App " +
        "order by maxSize_k desc"
    )

    println("Q5 --> Largest Apps are ")
    spark.time(q5Result.show())
    println()

    // Q6 --> Paid Vs Free count Apps
    val q6Result = spark.sql(
      "select Type, count(*) as NumberOfApps " +
        "from apps " +
        "group by Type " +
        "order by NumberOfApps desc"
    )

    println("Q6 --> Paid Vs Free count Apps are ")
    spark.time(q6Result.show())
    println()

    // Q7 --> Most Reviewed Apps
    val q7Result = spark.sql(
      "select App, sum(Reviews) as TotalReviews " +
        "from apps " +
        "group by App " +
        "order by TotalReviews desc"
    )

    println("Q7 --> Most Reviewed Apps are ")
    spark.time(q7Result.show())
    println()

    // Q8 --> Average Size of Apps in Same Category
    val q8Result = spark.sql(
      "select Category, avg(Size) as AverageSize, min(Size) as MinSize, max(Size) as MaxSize " +
        "from apps " +
        "group by Category " +
        "order by AverageSize desc"
    )

    println("Q8 --> Average - Min - Max Size of Apps in Same Category are ")
    spark.time(q8Result.show())
    println()

    // Q9 --> Top App Installation in each Category
    val q9Result = spark.sql(
      "select Category, App, TotalInstalls, rank " +
        "from (" +
        "select Category, App, TotalInstalls, DENSE_RANK () OVER (PARTITION BY Category Order by TotalInstalls desc) as rank from " +
        "( select Category, App, sum(Installs) as TotalInstalls from apps group by Category, App )" +
        ") " +
        "where rank == 1"
    )

    println("Q9 --> Top App Installation in each Category are ")
    spark.time(q9Result.show())
    println()

    // Q10 --> Number of Apps in Each Year
    val q10Result = spark.sql(
          "select Year, count(*) as NumberOfApps " +
            "from apps " +
            "group by Year " +
            "order by Year desc"
        )

    println("Q10 --> Number of Apps in Each Year are ")
    spark.time(q10Result.show())
    println()

    // Q11 --> Average Price of Paid Apps
    val q11Result = spark.sql(
          "select Type, avg(Price) as AvgPrice, sum(Price) as AllPrice, count(Price) as count, max(Price) as maxPrice, min(Price) as minPrice, stddev(Price) as STDPrice " +
            "from apps " +
            "where Type == 'Paid' " +
            "group by Type"
        )

    println("Q11 --> Some Operation on Price of Paid Apps are")
    spark.time(q11Result.show())
    println()

    // Q12 --> Most Installed Category in each year
    val q12Result = spark.sql(
          "select Year, Category, TotalInstalls, rank " +
            "from ( " +
            "select Year, Category, TotalInstalls, DENSE_RANK () OVER (PARTITION BY Year Order by TotalInstalls desc) as rank " +
            "from ( " +
            "select Year, Category, sum(Installs) as TotalInstalls from apps group by Year, Category" +
            " ) ) " +
            "where rank == 1 " +
            "order by Year"
        )

    println("Q12 --> Most Installed Category in each year are")
    spark.time(q12Result.show())
    println()

  }
}
