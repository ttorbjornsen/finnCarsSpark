/**
  * Created by torbjorn.torbjornsen on 11.07.2016.
  */
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.datastax.spark.connector._


class DAO (_hc: SQLContext, _csc:CassandraSQLContext) {
  import _hc.implicits._

  def getLatestDetails(acqCarHeader:AcqCarHeader):PropCar = {
    val acqCarHeaderUrl = acqCarHeader.url
    val acqCarHeaderLoadTime = acqCarHeader.load_time
    val prevAcqCarDetails = _csc.sparkContext.cassandraTable[AcqCarDetails]("finncars", "acq_car_details").
      where("url = ?", acqCarHeaderUrl).
      where("load_time <= ?", acqCarHeaderLoadTime).
      filter(row => row.deleted == false).collect

    val acqCarDetails = prevAcqCarDetails.maxBy(_.load_time)
    val propertiesMap = Utility.getMapFromJsonMap(acqCarDetails.properties)
    val equipmentList = Utility.getListSubsetFromJsonArray(acqCarDetails.equipment)

    //TODO PropCar(url=acqCarHeader.url, location=acqCarHeader.location, title=acqCarHeader.title, year=acqCarHeader.year, km=acqCarHeader.km, price=acqCarHeader.price, properties=)
    case class PropCar(url:String, title:String, location:String, year: String, km: String, price: String, properties:Map[String,String], equipment:Set[String], information:String, deleted:Boolean, load_time:java.sql.Timestamp, load_date:String)

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
