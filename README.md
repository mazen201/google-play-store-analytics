# Google Play Store Data Analytics
Explore and analyze Google Play store by using Spark framework and Scala.

## Table Of Content

- [Description](#description)
- [Requirements](#requirements)
  - [Technologies](#technologies)
  - [DataSet](#dataset)
  - [Run Code](#run-code)
  - [Project Folder Structure](#project-folder-structure)
- [Queries](#queries)
    - [1.  Most Installed App](#1--most-installed-app)
    - [2.  Which categories have the most number of apps](#2--which-categories-have-the-most-number-of-apps)
    - [3.  Which categories have most of the Installations](#3--which-categories-have-most-of-the-installations)
    - [4.	Which content ratings have the most number of apps](#4which-content-ratings-have-the-most-number-of-apps)
    - [5.	Largest Apps](#5largest-apps)
    - [6.	How many free or paid apps](#6how-many-free-or-paid-apps)
    - [7.	Most Reviewed Apps](#7most-reviewed-apps)
    - [8.	Average - Min - Max Size of Apps in Same Category](#8average---min---max-size-of-apps-in-same-category)
    - [9.	Top App Installation in each Category](#9top-app-installation-in-each-category)
    - [10.	Number of Apps in Each Year](#10number-of-apps-in-each-year)
    - [11.	Average - Min - Max - STD Price of Paid Apps](#11average---min---max---std-price-of-paid-apps)
    - [12.	Most Installed Category in each year](#12most-installed-category-in-each-year)


## Description <a name="description"/>

This project focuses on the analysis of the Google Play Store dataSet in Kaggle to Answer these Queries.
```
1.      Most Installed App
2.      Which categories have the most number of apps
3.      Which categories have most of the Installations
4.	Which content ratings have the most number of apps
5.	Largest Apps
6.	How many free or paid apps
7.	Most Reviewed Apps
8.	Average - Min - Max Size of Apps in Same Category
9.	Top App Installation in each Category
10.	Number of Apps in Each Year
11.	Average - Min - Max - STD Price of Paid Apps
12.	Most Installed Category in each year
```
<br />

## Requirements <a name="requirements"/>

### Technologies <a name="technologies"/>
* **Spark** version 3.1.2
* **Scala** scala-sdk-2.12.12
* **Java** jdk-11.0.12
* **Intellij** IDEA 2021.2.1 (Community Edition)

<br />

### DataSet <a name="dataset"/>
Google Play Store DataSet Contains Information about apps in market.

Each app (row) has values

  * Application Name
  * Category
  * Rating
  * Number of Review
  * Application Size
  * Number of Install
  * Application Type
  * Price
  * Content Rating
  * Genres
  * Android Ver
  * Latest Updates

Download the dataset at Kaggle: [Google Play Store Apps](https://www.kaggle.com/lava18/google-play-store-apps/home).

<br />

### Run Code <a name="run-code"/>

1. Setup Java jdk.
2. Add Spark jars to project build path.
3. Add Scala sdk to project build path.

For adding a new dependency in intellij Visit this [link](https://www.jetbrains.com/help/idea/working-with-module-dependencies.html#add-a-new-dependency) for help.

<br />

### Project Folder Structure <a name="project-folder-structure"/>
```
google-play-store-analytics
           |---- .idea
           |---- charts
                    |---- Categories have most of the Installations.png
                    |---- Categories have the most number of apps.png
                    |---- Content_Rating with number of Apps.png
                    |---- Number of Apps in Each Year.png
                    |---- Paid Vs Free.png
           |---- output
                    |---- DataFrameOutput.txt                    (result file by using DataFrame and UnTyped Operation API)
                    |---- DataFrameSqlOutput.txt                 (result file by using DataFrame and SQL)
                    |---- RddOutput.txt                          (result file by using RDD)
           |---- data
                    |---- googleplaystore.csv                    (data file)
           |---- googleAppStore
                    |---- src
                            |---- googleAppsDataFrame.scala      (scala code by using DataFrame and UnTyped Operation API)
                            |---- googleAppsSql.scala            (scala code by using DataFrame and SQL)
                            |---- googleAppsRDD.scala            (scala code by using RDD)
                    |---- googleAppStore.iml                     
           |---- README.md                                       (readme file)
```

<br />

## Queries <a name="queries"/>
#### 1.  Most Installed App <a name="1--most-installed-app"/>

|  App                | TotalInstalls    |
| ------------------- |:----------------:|
|  Subway Surfers     | 6000000000       |
|  Google News        | 4000000000       |
|  Temple Run 2       | 3000000000       |
|  Pou                | 2000000000       |
| imo free video call | 2000000000       |
| UC Browser          | 1500000000       |
| Dropbox             | 1500000000       |
| SHAREit             | 1000000000       |
| ROBLOX              | 900000000        |

<br />

#### 2.  Which categories have the most number of apps <a name="2--which-categories-have-the-most-number-of-apps"/>

| Category         | NumberOfApps |
| ---------------- |:------------:|
| Tools            | 634|
| Entertainment    | 448|
| Education        | 417|
| Medical          | 324|
| Action           | 322|
| Personalization  | 280|
| Lifestyle        | 279|
| Finance          | 266|
| Sports           | 261|
| Business         | 246|
| Photography      | 236|
| Productivity     | 235|
| Health & Fitness | 222|
| Communication    | 211|
| Arcade           | 186|
| Simulation       | 182|
| Shopping         | 179|
| Social           | 177|
| Dating           | 173|
| News & Magazines | 169|

![alt text](https://github.com/mazen201/google-play-store-analytics/blob/main/charts/Categories%20have%20the%20most%20number%20of%20apps.png?raw=true "Categories have the most number of apps")

<br />

#### 3.  Which categories have most of the Installations <a name="3--which-categories-have-most-of-the-installations"/>

|            Category|AllInstalls|
| ------------------ |:------------------:|
|              Arcade| 9836077727|
|              Casual| 7595478960|
|              Action| 7424937470|
|       Communication| 4941915530|
|    News & Magazines| 4251900550|
|               Tools| 3526053500|
|        Productivity| 2742302080|
|         Photography| 2557893130|
|              Sports| 2254911465|
|            Strategy| 1888127500|
|       Entertainment| 1836869820|
|              Racing| 1681646020|
|            Shopping| 1504231540|
|              Puzzle| 1447771070|
|     Personalization|  998142930|
|              Social|  925240475|
|Adventure;Action ...|  906150000|
|Video Players & E...|  886762200|
|    Health & Fitness|  867406220|
|          Simulation|  690369810|

![alt text](https://github.com/mazen201/google-play-store-analytics/blob/main/charts/Categories%20have%20most%20of%20the%20Installations.png?raw=true "Categories have most of the Installations")

<br />

#### 4.	Which content ratings have the most number of apps <a name="4which-content-ratings-have-the-most-number-of-apps"/>

![alt text](https://github.com/mazen201/google-play-store-analytics/blob/main/charts/Content_Rating%20with%20number%20of%20Apps.png?raw=true "content ratings have the most number of apps")

<br />

#### 5.	Largest Apps <a name="5largest-apps"/>

|                 App|maxSize_k|
| ------------- |:-------------:|
|           Post Bank| 102400.0|
|Draft Simulator f...| 102400.0|
|Miami crime simul...| 102400.0|
|The Walking Dead:...| 102400.0|
|Mini Golf King - ...| 102400.0|
|Car Crash III Bea...| 102400.0|
|Stickman Legends:...| 102400.0|
|Navi Radiography Pro| 102400.0|
|Gangster Town: Vi...| 102400.0|
|Talking Babsy Bab...| 102400.0|
|     Ultimate Tennis| 102400.0|
|     SimCity BuildIt| 102400.0|
|          Vi Trainer| 102400.0|
|Hungry Shark Evol...| 102400.0|
|   My Talking Angela| 101376.0|
|       Gangster Town| 101376.0|
|    Chakra Cleansing| 101376.0|
|Kill Shot Bravo: ...| 101376.0|
|        AI Benchmark| 101376.0|
|Angry Birds POP B...| 101376.0|

<br />

#### 6.	How many free or paid apps <a name="6how-many-free-or-paid-apps"/>

![alt text](https://github.com/mazen201/google-play-store-analytics/blob/main/charts/Paid%20Vs%20Free.png?raw=true "Free Vs Paid")

<br />

#### 7.	Most Reviewed Apps <a name="7most-reviewed-apps"/>

|                 App|TotalReviews|
| ------------------ |:---------------:|
|      Clash of Clans|   179558781|
|      Subway Surfers|   166331958|
|    Candy Crush Saga|   156993136|
|         8 Ball Pool|    99386198|
|        Clash Royale|    92530298|
|UC Browser - Fast...|    53140694|
|        Temple Run 2|    48710930|
|                 Pou|    41939801|
|              ROBLOX|    40038379|
|   My Talking Angela|    39523473|

<br />

#### 8.	Average - Min - Max Size of Apps in Same Category <a name="8average---min---max-size-of-apps-in-same-category"/>

|            Category|      AverageSize| MinSize| MaxSize|
| ------------------ |:---------------:|:---------:|:----------:|
|Lifestyle;Pretend...|         102400.0|102400.0|102400.0|
|Education;Brain G...|          99328.0| 99328.0| 99328.0|
| Adventure;Education|          94208.0| 94208.0| 94208.0|
| Arcade;Pretend Play|          93184.0| 93184.0| 93184.0|
|Health & Fitness;...|          84992.0| 84992.0| 84992.0|
| Racing;Pretend Play|          74752.0| 74752.0| 74752.0|
|Racing;Action & A...|70729.14285714286| 16384.0|101376.0|
|Educational;Creat...|          66560.0| 48128.0| 83968.0|
| Strategy;Creativity|          64512.0| 64512.0| 64512.0|
|Role Playing;Pret...|          64307.2| 49152.0| 81920.0|

<br />

#### 9.	Top App Installation in each Category <a name="9top-app-installation-in-each-category"/>

|            Category|                 App|TotalInstalls|
| ------------------ |:------------------:|:-----------:|
|Video Players & E...|        Video Editor|     10000000|
|Adventure;Action ...|              ROBLOX|    900000000|
|           Education|                 TED|     40000000|
|           Education|Babbel – Learn La...|     40000000|
|           Education|busuu: Learn Lang...|     40000000|
|              Trivia|        Trivia Crack|    100000000|
|     Auto & Vehicles|AutoScout24 - use...|     10000000|
|     Auto & Vehicles|Android Auto - Ma...|     10000000|
|Travel & Local;Ac...|Ascape VR: 360° V...|       100000|
|Simulation;Action...|Dog Run - Pet Dog...|     30000000|
|Education;Pretend...|      TO-FU Oh!SUSHI|     10000000|
|       Entertainment|    IMDb Movies & TV|    300000000|
| Education;Education|           ClassDojo|     30000000|
|Entertainment;Mus...|                Nick|     60000000|
|Parenting;Brain G...|My baby Game (Bal...|      1000000|
|Simulation;Preten...|My Little Pony Ce...|      2000000|
|Arcade;Action & A...|     Shopkins World!|     20000000|
|Educational;Creat...|    Coloring & Learn|      5000000|
| Arcade;Pretend Play|LEGO® Friends: He...|      1000000|
|Casual;Music & Video|Kids Balloon Pop ...|     20000000|

<br />

#### 10.	Number of Apps in Each Year <a name="10number-of-apps-in-each-year"/>

![alt text](https://github.com/mazen201/google-play-store-analytics/blob/main/charts/Number%20of%20Apps%20in%20Each%20Year.png?raw=true "Number of Apps in Each Year")

<br />

#### 11.	Average - Min - Max - STD Price of Paid Apps <a name="11average---min---max---std-price-of-paid-apps"/>

|        AvgPrice|         AllPrice|count|maxPrice|minPrice|       meanPrice|          STDPrice|
| -------------- |:---------------:|:-------:|:--------:|:--------:|:--------------:|:---------------:|
|15.0523831263725|8715.329830169678|  579|   400.0|    0.99|15.0523831263725|61.903024408108514|

<br />

#### 12.	Most Installed Category in each year <a name="12most-installed-category-in-each-year"/>

|Year|        Category|TotalInstalls|
| ---- |:------------------:|:-----------:|
|2010|   Entertainment|       100000|
|2011|          Action|     10000000|
|2012|Libraries & Demo|     10000000|
|2013|          Casual|     50011100|
|2014| Personalization|     72591000|
|2015|          Arcade|    121670150|
|2016|          Arcade|    149751000|
|2017|          Arcade|    400687512|
|2018|          Arcade|   9156709065|
