// Import Spark Libraries
object BatchJob {
  def main(args: Array[String]): Unit = {

    val hive_loc = "hive_meta_loc"
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("DailyAnalytics")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    try {
      val daysToCheck = 1
      val stddays = 40
      val thr_Dev_VoiceQoE = 90

      //SQL SERVER Connection Params--give ip and port ,usename,password
      val jurl = "jdbc:sqlserver://ip:port;databaseName=xxxx;"
      import java.util.Properties
      val connectionProperties = new Properties()
      connectionProperties.put("user", "appadmin")
      connectionProperties.put("password", "xxx@12345")
      connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

      // create user required dataframe here
      val maxdateDF = spark.sql("select regexp_replace(date(cast(from_unixtime(unix_timestamp()- " + (daysToCheck.toInt * 24 * 60 * 60) + " )  as timestamp)) ,\"-\",''  )) ");

      // Schema of your dataframe and sql server table should match , types can be different
      maxdateDF.write.mode("append").jdbc(jurl, "SQL_Server_Table", connectionProperties);


    }

    catch {
      case e: Exception => e.printStackTrace()
        return 1;
    }
  }
}
