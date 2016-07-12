/**
  * Created by torbjorn.torbjornsen on 12.07.2016.
  */

import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext} //allows registering temptables
import play.api.libs.json._


System.setProperty("hadoop.home.dir", "C:\\Users\\torbjorn.torbjornsen\\Hadoop\\")
val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","192.168.56.56")
val sc = new SparkContext(conf)
sc.setLogLevel("WARN")
val csc = new CassandraSQLContext(sc)
csc.setKeyspace("finncars")
val hc = new HiveContext(sc)
import hc.implicits._ //allows registering temptables


