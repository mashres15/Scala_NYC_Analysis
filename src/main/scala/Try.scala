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
              .appName("my-CS-project-Ajit")
              .config("spark.some.config.option", "config-value")
              .getOrCreate()

        import spark.implicits._

        /* ---------------------------------------------------------------------
        *********** Defining the Schema for the data
        ---------------------------------------------------------------------*/
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

        /* ---------------------------------------------------------------------
        *********** Loading the data
        ---------------------------------------------------------------------*/

        val ndf = spark.read.schema(schema).csv("/Users/student/Desktop/spark_code/src/data/NYCTaxi/yellow_tripdata_2017-01.csv")
        val df = ndf.na.drop()
        df.show()

        /* ---------------------------------------------------------------------
        *********** Creating the View for queries
        ---------------------------------------------------------------------*/
        df.createOrReplaceTempView("NYC")

        /* ---------------------------------------------------------------------
        *********** Defining df for payment type
        ---------------------------------------------------------------------*/
        val payment = List((1, "Credit Card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown")).toDF("payment_type","payment_name")

        payment.createOrReplaceTempView("PAYMENT_ID")



        /* ---------------------------------------------------------------------
        *********** Analysis 1: Taxitrip distance

        The following query shows the frequery of taxi rides for given trip distance range

        ---------------------------------------------------------------------*/
        val query1 = spark.sql("""
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
        ORDER BY trip_distance""")

        query1.show()

        query1.coalesce(1).write.csv("~/Desktop/query1")

        val news = df.join(payment, Seq("payment_type"))
        val newsdf = news.withColumn("hour", hour(col("tpep_pickup_datetime")))
        newsdf.createOrReplaceTempView("NYCM")


        /* ---------------------------------------------------------------------
        *********** Analysis 2: Payment type for whole data

        The following query shows the frequery of different payment type that customers used to pay.

        ---------------------------------------------------------------------*/

        val query2 = spark.sql("""SELECT
        payment_name,
        COUNT(*) AS freq
        FROM NYCM
        GROUP BY 1
        ORDER BY payment_name""")

        query2.show()

        query2.coalesce(1).write.csv("~/Desktop/query2")

        /* ---------------------------------------------------------------------
        *********** Analysis 3: Payment type during night time (after 6pm)

        The following query shows the frequery of different payment type that 
        customers used to pay after 6 pm. So, we are  trying to see if people prefer to 
        use card more in the night than using cash.

        ---------------------------------------------------------------------*/
        val query3 = spark.sql("""SELECT
        payment_name,
        COUNT(*) AS freq
        FROM NYCM
        WHERE hour >= 18
        GROUP BY 1
        ORDER BY payment_name""")

        query3.show()
        query3.coalesce(1).write.csv("~/Desktop/query3")

        /* ---------------------------------------------------------------------
        *********** Analysis 4: Payment type during day time (before 6pm)

        The following query shows the frequery of different payment type that 
        customers used to pay before 6 pm. So, we are  trying to see if people prefer to 
        use cash more than using card in the day time.

        ---------------------------------------------------------------------*/

        val query4 = spark.sql("""SELECT
        payment_name,
        COUNT(*) AS freq
        FROM NYCM
        WHERE hour < 18
        GROUP BY 1
        ORDER BY payment_name""")

        query4.show()
        query4.coalesce(1).write.csv("~/Desktop/query4")


        /* ---------------------------------------------------------------------
        *********** Analysis 5: Where is the demand for taxi the most for fare greater than $100?

        The following query shows the frequery of demand for taxi with respect to PULocationID.

        ---------------------------------------------------------------------*/

        val query5 = spark.sql("""SELECT DISTINCT PULocationID, count(*) AS freq 
        FROM NYC   
        WHERE total_amount > 100 
        GROUP BY PULocationID
        ORDER BY freq""")

        query5.show()
        query5.coalesce(1).write.csv("~/Desktop/query5")

        spark.stop()
    }
    
}

