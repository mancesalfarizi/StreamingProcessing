package selalu;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.File;

public class Main{
    public static void main( String[] args )
    {
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
    
    Dataset<Row> famaousF = spark.sql("select * from default.pquery3 order by jumlahFavorite desc limit 10 ");
    
    Dataset<Row> famaousR = spark.sql("select * from default.pquery3 order by jumlahRetweet desc limit 10 "); 
    famaousF.show();

    famaousR.show();

    famaousF.write()
    .mode("overwrite")
    .format("parquet")
    .saveAsTable("bigproject.topfavorite");

    famaousR.write()
    .mode("overwrite")
    .format("parquet")
    .saveAsTable("bigproject.topRetweet");

    }
}
