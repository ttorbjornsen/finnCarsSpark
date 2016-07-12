/**
  * Created by torbjorn.torbjornsen on 11.07.2016.
  */
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


class DAO (sqlc: SQLContext, csc:CassandraSQLContext) {
  import sqlc.implicits._

  def getLatestDetails(headerUrl:String, loadTime:Long):DataFrame = {
    val df = sqlc.sql("SELECT url, properties, equipment, information, deleted, load_time, load_date FROM acq_car_details WHERE url = \"" + headerUrl + "\" AND load_time <= " + loadTime.toString + " AND deleted = false ORDER BY load_time DESC LIMIT 1")
    df
    //    val loadTime = 3
//    val headerUrl = "http://m.finn.no/car/used/ad.html?finnkode=78866263"
//    hc.sql("select * from acq_car_details where load_time < 3").show
//    val df = hc.sql("SELECT url, properties, equipment, information, deleted, load_time, load_date FROM acq_car_details WHERE url = \"" + headerUrl + "\" AND load_time<= " + loadTime)
  }



  def getAcqDetailsFromURL(url:String):DataFrame = {
    val dfAcqCarDetails = csc.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_details", "keyspace" -> "finncars")).
      load().
      select("url", "properties", "equipment", "information", "deleted", "load_time", "load_date").
      limit(3).
      toDF


    dfAcqCarDetails
  }


}
