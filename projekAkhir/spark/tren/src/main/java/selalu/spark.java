package selalu;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
                .master("local")
                .getOrCreate();
 
        //Subscribe to topic 'test'
        Dataset< Row > df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.170.0.22:9092")
                .option("subscribe", "trump")
                .option("startingOffsets", "earliest") // From starting
                .load();
        
        df.printSchema();
 
        //Getting the data value as String
        Dataset<Row> rubahdDf = df.selectExpr("CAST(value AS STRING)");

        //bikin schema

        StructType schema = new StructType()
                     .add("id","integer")
                     .add("firstname","string")
                     .add("middlename","string")
                     .add("lastname","string")
                     .add("dob_year","string")
                     .add("dob_month","string")
                     .add("gender","string")
                     .add("salary","string");

        
        Dataset<Row> personDF = rubahdDf.select(from_json(col("value"), schema).as("data")).select("data.*");

        personDF.writeStream().format("console")
             .outputMode("append")
             .start()
             .awaitTermination();
        
        
 
        
        
    }
    
}
