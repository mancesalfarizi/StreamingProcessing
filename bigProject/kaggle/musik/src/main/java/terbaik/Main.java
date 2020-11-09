package terbaik;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import static org.apache.spark.sql.functions.*;
import java.io.File;
// import static org.apache.spark.sql.functions.*;
// import java.time.LocalDateTime;
// import java.sql.Timestamp;

public class Main {
    public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF); 

    // buat spark session
    String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Read JSON File to DataSet")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate(); 

     //read dari local
    //  Dataset<Row> df1 = spark.read()
    //  .format("csv")
    //  .option("delimiter", ",")
    //  .option("quote", "\"")
    //  .option("escape", "\"") 
    //  .option("header", "true")
    //  .load("file:///E:/data.csv");
    
     
    //write ke postgres 
    //  df1.write()
    //           .mode("append")
    //           .format("jdbc")
    //           .option("url", "jdbc:postgresql://localhost:5432/projekAkhir")
    //           .option("dbtable", "public.lagu")
    //           .option("user", "postgres")
    //           .option("password", "123456")
    //           .save();

    
    //show hasi read di console
    // df1.show();
    
    //cleansing data
    // Dataset<Row> data_bersih = df1.withColumn("artist", regexp_replace(col("artist"), "[^\\p{L}\\p{N}\\p{P}\\p{Z}]","" ));

    spark.sql("CREATE TABLE lagu (acousticness STRING, artists STRING, danceability STRING, duration_ms STRING, energy STRING, "
    + "explicit STRING, id STRING, instrumentalness STRING, key STRING, liveness STRING, loudness STRING, mode STRING, name STRING, "
    + "popularity STRING, release_date INT, speechiness STRING, tempo STRING, valence STRING, year STRING)"
    + "row format delimited fields terminated by ',' "
    + "tblproperties(skip.header.line.count = 1)");
    

    spark.sql("LOAD DATA INPATH '/user/mancesalfarizi/data.csv' OVERWRITE INTO TABLE lagu");
    Dataset<Row> semualagu = spark.sql("SELECT * FROM default.lagu");
    
    Main hive = new Main();

    hive.top10(semualagu);

    
        
    
    
    
    

        
    }
    private void top10(Dataset<Row> data){
        Dataset<Row> res = data.select("*")
                .filter("release_date between '2020-01-01' and '2020-12-01'")
                .orderBy(desc("popularity"))
                .limit(10);

        res.write().mode("overwrite").format("parquet").saveAsTable("bigproject.top10");

    }
}
