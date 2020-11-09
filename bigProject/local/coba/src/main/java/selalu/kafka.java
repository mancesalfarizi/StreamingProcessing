package selalu;

// import java.util.*;
// import org.apache.spark.SparkConf;
// import org.apache.spark.streaming.kafka010.*;
// import org.apache.spark.streaming.Durations;
// import org.apache.spark.streaming.api.java.*;
// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.common.serialization.StringDeserializer;
// import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
// import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
// import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.functions;
// import org.apache.spark.sql.streaming.DataStreamWriter;
// import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.from_json;
// import org.apache.spark.sql.streaming.Trigger;
// import org.apache.spark.sql.Dataset;
// import org.apache.spark.api.java.function.VoidFunction2;
// import org.spark_project.guava.collect.ImmutableMap;
// import static org.apache.spark.sql.types.DataTypes.IntegerType;
// import static org.apache.spark.sql.types.DataTypes.StringType;
// import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.functions.col;
// import org.apache.spark.sql.streaming.DataStreamWriter;

// import static org.apache.spark.sql.functions.*;

// import org.apache.spark.sql.*;
// import org.apache.spark.sql.streaming.StreamingQuery;
// import org.apache.spark.sql.streaming.StreamingQueryException;

// import java.util.Arrays;
// import java.util.Iterator;

public class kafka {

    public static void main( String[] args )throws StreamingQueryException {
     //Set log level to warn
    Logger.getLogger("org").setLevel(Level.OFF);
         Logger.getLogger("akka").setLevel(Level.OFF); 
 
        // Define a Spark Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Kafka Integration using Structured Streaming")
                .master("local[3]")
                .enableHiveSupport()
                .getOrCreate();
 
        //Subscribe to topic 'test'
        Dataset< Row > df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.170.0.22:9092")
                .option("group.id", "lagubersih")
                .option("subscribe", "jokowi")
                .option("failOnDataLoss", "false")
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

        
        // personDF.write()
        //          .mode("append")
        //          .format("jdbc")
        //          .option("url", "jdbc:postgresql://localhost:5432/postgres")
        //          .option("dbtable", "public.tweetlagu")
        //          .option("user", "postgres")
        //          .option("password", "123456")
        //          .save();

        StreamingQuery pQuery = personDF.writeStream()
          .outputMode("append")
          .format("csv") 
        //   .queryName("pQuery")
        .option("checkpointLocation", "/home/mancesalfarizi")  
          .option("path", "hdfs://jyon-m/user/hive/warehouse/pquery")
          .option("failOnDataLoss", "false")
        //   .trigger(Trigger.ProcessingTime(4000))
          .start();
            // spark.sql("select * from pQuery").show();

        pQuery.awaitTermination();
        
        // StreamingQuery query = personDF.writeStream()
        //      .format("console")
        //      .outputMode("append")
        //      .start();

        // query.awaitTermination();



        // StreamingQuery query = personDF.writeStream().foreachBatch(
        //     new VoidFunction2<Dataset<String>, Long>() {
        //         public void call(Dataset<String> dataset, Long batchId) {
        //             personDF.createOrReplaceTempView("data_tweet");

        //             kafka hive = new kafka();


        

                                    
        //         }    
                                    
        // }
        // ).start();

                             
 
         




        }  
        
    }

