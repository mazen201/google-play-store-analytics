import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

object googleAppsRDD {
  // AppName, Rating, Review, Size, Installs, Type, Price, Content_Rating, Category, Last_Update, Version
  //    1   ,    2  ,    3  ,  4  ,  5      ,  6  ,  7   ,   8           ,    9    ,     10     ,   11
  def mapLine(row: Row) : Option[(String, Float, Int, Float, Int, Boolean, Float, String, String, Date, String)] = {
    var arr = new Array[String](row.length)
    for(i <- 0 to row.length - 1){
      if (row(i) == null) return None
      arr(i) = row(i).toString.trim()
    }
    val AppName = arr(0)
    // Handle Rating
    var Rating = 0.0f
    if (arr(2) == "navigation" || arr(2) == "NaN" || arr(2) == "Body"){
      return None
    } else {
      Rating = arr(2).toFloat
    }
    // Handle Review
    var Review = 0
    if (arr(3) == "weight lose)\"" || arr(3) == "3.0M"){
        return None
    } else {
      Review = arr(3).toInt
    }
    // Handle Size
    var Size = 0.0f
    if (arr(4) == "MAPS_AND_NAVIGATION" || arr(4) == "Varies with device" || arr(4) == "Varies with devic"){
      return None
    } else if (arr(4).charAt(arr(4).length - 1) == 'M') {
      Size = arr(4).substring(0, arr(4).length - 1).toFloat * 1024
    } else {
      Size = arr(4).substring(0, arr(4).length - 1).toFloat
    }
    // Handle Installs
    var InstallString = arr(5)
    if (InstallString == "Free" || InstallString == "4.2" || InstallString == "4.4"){
      return None
    } else {
      InstallString = InstallString.replace("+", "").replace(",", "")
    }
    val Installs = InstallString.toInt;
    // Handle Type
    var Type = true
    if(arr(6) == "0" || arr(6) == "102248" || arr(6) == "NaN" || arr(6) == "2509"){
      return None
    } else if (arr(6) == "Paid"){
      Type = false
    }
    // Handle Price
    var Price = 0.0f
    if(arr(7) == "5.0M" || arr(7) == "Varies with device"){
      return None
    } else if (arr(7) == "0"){
      Price = 0.0f
    } else {
      Price = arr(7).substring(1).toFloat
    }
    // Handle Content_Rating
    var Content_Rating = ""
    if(arr(8) == "5,000,000+" || arr(8) == "1,000,000+" || arr(8) == "null"){
      return None
    } else if (arr(8) == "Everyone"){
      Content_Rating = "Everyone 10+"
    } else {
      Content_Rating = arr(8)
    }
    // Handle Category
    val Category = arr(9)
    // Handle Last_Update
    val format = new SimpleDateFormat("MMMM dd, yyyy")
    val Last_Update = format.parse(arr(10))
    // Handle Version
    val Version = arr(11)

    Some(AppName, Rating, Review, Size, Installs, Type, Price, Content_Rating, Category, Last_Update, Version)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("googleAppsRDD")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/googleplaystore.csv")

    // print Schema of DataFrame
    println("Original Schema")
    println(df.printSchema())

    val rdd = df.rdd

    val updatedRDD = rdd.flatMap(mapLine).cache()

    // Q1 --> Most Installed App
    val q1Result = updatedRDD
      .map(x => (x._1, x._5))
      .reduceByKey((x, y) => x + y )
      .sortBy(_._2, false)
      .take(20)

    println("Q1 --> Most Installed Apps are ")
    q1Result.foreach(println)
    println()

    // Q2 --> Top Categories with number of apps
    val q2Result = updatedRDD
      .map(x => (x._9, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)
      .take(20)

    println("Q2 --> Top Categories with number of apps are ")
    q2Result.foreach(println)
    println()

    // Q3 --> Top Categories with number of Installation of all apps
    val q3Result = updatedRDD
      .map(x => (x._9, x._5))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)
      .take(20)

    println("Q3 --> Top Categories with number of Installation of all apps are ")
    q3Result.foreach(println)
    println()

    // Q4 --> top Content_Rating with number of Apps
    val q4Result = updatedRDD
      .map(x => (x._8, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)
      .take(20)

    println("Q4 --> Top Content_Rating with number of Apps are ")
    q4Result.foreach(println)
    println()

    // Q5 --> Largest Apps
    val q5Result = updatedRDD
      .map(x => (x._1, x._4))
      .reduceByKey((x, y) => math.max(x, y))
      .sortBy(_._2, false)
      .take(20)

    println("Q5 --> Largest Apps are ")
    q5Result.foreach(println)
    println()

    // Q6 --> Paid Vs Free count Apps
    val q6Result = updatedRDD
      .map(x => (x._6, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)

    println("Q6 --> Paid Vs Free count Apps are ")
    q6Result.foreach(println)
    println()

    // Q7 --> Most Reviewed Apps
    val q7Result = updatedRDD
      .map(x => (x._1, x._3))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)
      .take(20)

    println("Q7 --> Most Reviewed Apps are ")
    q7Result.foreach(println)
    println()

    // Q8 --> Average Size of Apps in Same Category
    val q8Result = updatedRDD
      .map(x => (x._9, (x._4, x._4, x._4, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, Math.max(x._2, y._2), Math.min(x._3, y._3), x._4 + y._4))
      .map(x => (x._1, x._2._1 / x._2._4, x._2._2, x._2._3))
      .sortBy(_._2, false)
      .take(20)

    println("Q8 --> Average - Min - Max Size of Apps in Same Category are ")
    q8Result.foreach(println)
    println()

    // Q9 --> Top App Installation in each Category
    val q9Result = updatedRDD
      .map(x => ((x._9, x._1), x._5))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .reduceByKey((x, y) => if (x._2 > y._2) x else y)
      .map(x => (x._1, x._2._1, x._2._2))
      .sortBy(_._3, false)
      .take(20)

    println("Q9 --> Top App Installation in each Category are ")
    q9Result.foreach(println)
    println()

    // Q10 --> Number of Apps in Each Year
    val format = new SimpleDateFormat("yyyy")
    val q10Result = updatedRDD
      .map(x => (format.format(x._10), 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._1)

    println("Q10 --> Number of Apps in Each Year are ")
    q10Result.foreach(println)
    println()

    // Q11 --> Average Price of Paid Apps
    val q11Result = updatedRDD
      .map(x => (x._6, x._7))
      .filter(x => x._1 == false)
      .map(x => (x._1, (x._2, 1, x._2, x._2)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, math.max(x._3, y._3), math.min(x._4, y._4)))
      .map(x => (x._2._1, x._2._2, x._2._3, x._2._4, x._2._1 / x._2._2))

    println("Q11 --> Some Operation on Price of Paid Apps are")
    q11Result.foreach(println)
    println()

    // Q12 --> Most Installed Category in each year
    val format2 = new SimpleDateFormat("yyyy")
    val q12Result = updatedRDD
      .map(x => ((x._9, format2.format(x._10)), x._5))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1._2, (x._1._1, x._2)))
      .reduceByKey((x, y) => if (x._2 > y._2) x else y)
      .map(x => (x._1, x._2._1, x._2._2))
      .sortBy(_._3, false)

    println("Q12 --> Most Installed Category in each year are")
    q12Result.foreach(println)
    println()
  }

}
