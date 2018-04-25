import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
//import org.apache.spark.sql.SQLContext.implicits
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object Try {
    def main(args: Array[String]) {
        val spark = SparkSession.builder
              .master("local")
              .appName("my-spark-app")
              .config("spark.some.config.option", "config-value")
              .getOrCreate()

        import spark.implicits._

        //val conf = new SparkConf().setAppName("Try")
        //val sc = new SparkContext(conf)
        
        //val sqlContext= new org.apache.spark.sql.SQLContext(sc)
        //import sqlContext.implicits._
        
        val schema = StructType(Array(
            StructField("VendorID", IntegerType, true),
            StructField("tpep_pickup_datetime", TimestampType, true),
            StructField("tpep_dropoff_datetime", TimestampType, true),
            StructField("passenger_count", IntegerType, true),
            StructField("trip_distance", DoubleType, true),
            StructField("RatecodeID", StringType, true),
            StructField("store_and_fwd_flag", StringType, true),
            StructField("PULocationID", DoubleType, true),
            StructField("DOLocationID", DoubleType, true),
            StructField("payment_type", IntegerType, true),
            StructField("fare_amount", DoubleType, true),
            StructField("extra", StringType, true),
            StructField("mta_tax", DoubleType, true),
            StructField("tip_amount", DoubleType, true),
            StructField("tolls_amount", DoubleType, true),
            StructField("improvement_surcharge", DoubleType, true),
            StructField("total_amount", DoubleType, true)
        ))


        val ndf = spark.read.schema(schema).csv("/Users/student/Desktop/spark_code/src/data/NYCTaxi/yellow_tripdata_2017-01.csv")
        val df = ndf.na.drop()
        df.show()

        df.createOrReplaceTempView("NYC")


        val payment = List((1, "Credit Card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown")).toDF("payment_type","payment_name")

        payment.createOrReplaceTempView("PAYMENT_ID")

        spark.sql("""
        SELECT
        CASE
        WHEN trip_distance Between 0 and 4 then '0-04'
        WHEN trip_distance Between 4 and 8 then '04-08'
        WHEN trip_distance Between 8 and 12 then '08-12'
        WHEN trip_distance Between 12 and 16 then '12-16'
        WHEN trip_distance Between 16 and 20 then '16-20'
        ELSE '20+'
        END AS trip_distance,
        COUNT(*) AS freq
        FROM NYC
        GROUP BY 1
        ORDER BY trip_distance""").show()

        //spark.sql("""SELECT * FROM PAYMENT_ID""").show()

        val news = df.join(payment, Seq("payment_type"))
        val newsdf = news.withColumn("hour", hour(col("tpep_pickup_datetime")))
        newsdf.createOrReplaceTempView("NYCM")

        //df.join(payment, df("payment_type") === payment("payment_type")).show()

        spark.sql("""SELECT
        payment_name,
        COUNT(*) AS freq
        FROM NYCM
        GROUP BY 1
        ORDER BY payment_name""").show()

        spark.sql("""SELECT
        payment_name,
        COUNT(*) AS freq
        FROM NYCM
        WHERE hour >= 18
        GROUP BY 1
        ORDER BY payment_name""").show()

        spark.sql("""SELECT
        payment_name,
        COUNT(*) AS freq
        FROM NYCM
        WHERE hour < 18
        GROUP BY 1
        ORDER BY payment_name""").show()

        val loc = spark.sql("""SELECT DISTINCT PULocationID, count(*) AS freq 
        FROM NYC   
        WHERE total_amount > 100 
        GROUP BY PULocationID
        ORDER BY freq""")

        spark.stop()
    }
    
}

