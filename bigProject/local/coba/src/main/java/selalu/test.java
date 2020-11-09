package selalu;

// import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// import static org.apache.spark.sql.functions.*;


public class test {
    public static void main(String[] args) {
        
        SparkSession spark = SparkSession
        .builder()
        .master("yarn")
        .appName("Spark Kafka Integration using Structured Streaming")
        .enableHiveSupport()
        .getOrCreate();

        Dataset<Row> bacaDariHive = spark.read().format("csv").option("delimiter", ",").option("quote", "\"")
        .option("escape", "\"")
        .option("header", "true")
        .load("hdfs:///user/hive/warehouse/pquery/part-00000-72307cce-eae3-48c4-b5b0-df0b64741e86-c000.csv");
        
        bacaDariHive.show();


    }
}