package selalu;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.*;

public class spark {

    public static void main( String[] args )throws StreamingQueryException {
        //Set log level to warn
        Logger.getLogger("org").setLevel(Level.OFF);
         Logger.getLogger("akka").setLevel(Level.OFF); 
 
        // Define a Spark Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Kafka Integration using Structured Streaming")
                .master("local[3]")
                .getOrCreate();
 
        //Subscribe to topic 'test'
        Dataset< Row > df = spark
                .readStream()
                .format("kafka")
                .option("failOnDataLoss", "false")
                .option("kafka.bootstrap.servers", "10.170.0.22:9092")
                .option("group.id", "idlagu")
                .option("subscribe", "trump")
                .option("startingOffsets", "earliest") // From starting
                .load();
        
        df.printSchema();
 
        //Getting the data value as String
        Dataset<Row> rubahdDf = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)");

        //bikin schema
        StructType schema = new StructType()
                     .add("userId","integer")
                     .add("tanggalTweet","timestamp")
                     .add("isiTweet","string")
                     .add("jumlahFollower","integer")
                     .add("lokasi","string")
                     .add("jumlahFavorite","integer")
                     .add("jumlahRetweet", "integer");
        
        Dataset<Row> personDF = rubahdDf.select(from_json(col("value"), schema).as("data")).select("data.*");
        personDF.createOrReplaceTempView("dataTweet");

        Dataset<Row> coba = spark.sql("select * from dataTweet");

        StreamingQuery query = coba.writeStream()
                   .format("console")
                   .outputMode(OutputMode.Update())
                  .option("truncate", false)
                   .option("numRows", 50)
                   .start();
                query.awaitTermination();

        //uncommet kalau ingin memunculkan di console
        // StreamingQuery query = personDF.writeStream()
        //      .format("console")
        //      .outputMode("append")
        //      .start();

        // query.awaitTermination();

        // StreamingpersonDF.selectExpr("CAST(userId AS STRING) AS key", "to_json(struct(*)) AS value").writeStream().format("kafka")
        //     .outputMode("append")
        //     .option("kafka.bootstrap.servers", "10.170.0.22:9092")
        //     .option("topic", "jokowi")
        //     .option("checkpointLocation", "/home/mancesalfarizi")
        //     .start()
        //     .awaitTermination();
        // Dataset<Row> bersihinData = personDF.withColumn(colName, col)


        // StreamingQuery query = personDF.writeStream()
        //   .outputMode("append")
        //   .format("memory")
        //   .queryName("initDF")
        //   .trigger(Trigger.ProcessingTime(1000))
        //   .start();
        
        //   Dataset<Row> coba = spark.sql("select * from initDF");
        //         coba.write().format("parquet").mode("overwrite").saveAsTable("bigproject.semuadata");
        //   Dataset<Row>      
                
        
        
                



        // query.awaitTermination();
        


     

              


    
    }
   
    }











        // personDF.createOrReplaceTempView("data_tweet");

        // Dataset<Row> call = spark.sql("select * from data_tweet");

        // Dataset<Row> words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

        // Group the data by window and word and compute the count of each group
        // Dataset<Row> tenSecondCounts = personDF
        //         .withWatermark("timestamp", "10 minutes")
        //         .groupBy(col("key"),
        //             window(col("timestamp"), "1 day"))
        //         .count();
                
        // StreamingQuery query = tenSecondCounts
        //         .writeStream()
        //         .outputMode("complete")
        //         .trigger(Trigger.ProcessingTime("25 seconds"))
        //         .format("console")
        //         .start();
        
        // query.awaitTermination();
