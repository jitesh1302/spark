package jitesh.spark

import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext.implicits.*
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column
import java.sql.{ Connection, DriverManager }

object wordcount extends App {

  //def main(args: Array[String]) {

  val url = "jdbc:mysql://localhost:3306/demo?user=demo&password=demo"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "demo"
  val password = "demo"
  var connection: Connection = _

  try {

    // set spark.driver.allowMultipleContexts = true;
    val conf = new SparkConf()
      .setAppName("WordCount")
    val sc = new SparkContext(conf);

    val sqlContext = new org.apache.spark.sql.SQLContext(sc);

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    val rs = statement.executeQuery("SELECT * FROM Master_table")

    while (rs.next) {

      val Subject_Area = rs.getString("Subject_Area")
      val Source_Type = rs.getString("Source_Type")
      val Username = rs.getString("Username")
      val Password = rs.getString("Password")
      val Servername = rs.getString("Servername")
      val Table_FileMask = rs.getString("Table_FileMask")
      val Extract_Type = rs.getString("Extract_Type")
      val Data_Type = rs.getString("Data_Type")
      val Last_Extract = rs.getString("Last_Extract")
      val file_location = rs.getString("file_location")
      val file_type = rs.getString("file_type")
      val Filter = rs.getString("Filter")

      if (file_type == "CSV") {
        
        
            
        val df = sqlContext.read
          .format("csv")
          .option("header", "true")
          .option("nullvalue", "NA")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
          .option("mode", "failfast")
          .load(file_location.concat(Table_FileMask))
        val val1 = "spark_processed"
        df.registerTempTable("Temp")
        
        if ( Filter !=null ) 
        {

        val df1 = sqlContext.sql("select * from Temp ")
        
        val df1_filter = df1.filter(Filter)

        val df2 = df1_filter.withColumn("process", lit("spark_process")).withColumn("CreationDate", org.apache.spark.sql.functions.current_timestamp())
        
        //val df1 = df.withColumn("CreationDate",org.apache.spark.sql.functions.current_timestamp()).select("Spark_process")
        //withColumn("Process", [Spark_Process]);

        df2.write
          .format("csv")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
          .option("header" ,"true")
          .mode("overwrite")
          .save("/home/ubuntu/Jitesh/survey") 
          
        }
        
        else {
          
            val df1 = sqlContext.sql("select * from Temp ")
        
        

        val df2 = df1.withColumn("process", lit("spark_process")).withColumn("CreationDate", org.apache.spark.sql.functions.current_timestamp())
        
        //val df1 = df.withColumn("CreationDate",org.apache.spark.sql.functions.current_timestamp()).select("Spark_process")
        //withColumn("Process", [Spark_Process]);

        df2.write
          .format("csv")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
         .option("header" ,"true")
          .mode("overwrite")
          .save("/home/ubuntu/Jitesh/csv") 

          
          
        }
      } else if (file_type == "json") {

        val df3 = sqlContext.read.format("json")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
          .option("mode", "failfast")
          .load(file_location.concat(Table_FileMask))

        df3.write
          .format("csv")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
          .option("header" ,"true")
          .mode("overwrite")
          .save("/home/ubuntu/Jitesh/json/")
         

}
      
      else if ( file_type =="mysql")
        
      {
       // val df4 = sqlContext.read.format("csv")
       
       /* .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url","jdbc:mysql://localhost:3306/demo?user=demo&password=demo")
        .option("dbtable","Master_table")
        .option("user" , "demo")
        .option("password","demo")


       */

        val df4 = sqlContext.read
          .format("jdbc")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("url","jdbc:mysql://localhost:3306/demo?user=demo&password=demo")
        .option("dbtable","Master_table")
        .option("user" , "demo")
        .option("password","demo")
        .load()
     
   
        
       df4.write
         .format("csv")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
          .option("header" ,"true")
          .mode("overwrite")
          .save("/home/ubuntu/Jitesh/database/")
        
        
    
      }
        

    }

    sc.stop

  } catch {
    case e: Exception => e.printStackTrace

  }
  connection.close

}
  

//}