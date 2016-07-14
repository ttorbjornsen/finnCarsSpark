/**
  * Created by torbjorn.torbjornsen on 11.07.2016.
  */
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.datastax.spark.connector._
import play.api.libs.json.{JsObject, JsValue, Json}


object DAO2 {

  val func = (acqCarHeader:AcqCarHeader, _csc:CassandraSQLContext) =>{
    //val acqCarHeader = AcqCarHeader("Volkswagen Passat 1,6 TDI 105hk BlueMotion Business","http://m.finn.no/car/used/ad.html?finnkode=78537231","Kirkenes","2010","121 835 km","149 000,-",2016-07-04 13:41:21.477,2016-07-04))

    val acqCarHeaderUrl = acqCarHeader.url
    val acqCarHeaderLoadTime = acqCarHeader.load_time
    val prevAcqCarDetails = _csc.sparkContext.cassandraTable[AcqCarDetails]("finncars", "acq_car_details").
      where("url = ?", acqCarHeaderUrl).
      where("load_time <= ?", new java.util.Date(acqCarHeaderLoadTime)).
      filter(row => row.deleted == false).collect


//    if (prevAcqCarDetails.length > 0) {
//      val acqCarDetails = prevAcqCarDetails.maxBy(_.load_time)
//      val propertiesMap:Map[String,String] = {
//        val jsValueMap: JsObject = Json.parse(acqCarDetails.properties).asInstanceOf[JsObject]
//        jsValueMap.as[Map[String, String]].filterKeys {
//          "".contains(_) == false
//        }
//      }
        if (prevAcqCarDetails.length > 0) {
          val acqCarDetails = prevAcqCarDetails.maxBy(_.load_time)
          val propertiesMap:Map[String,String] = {
            val jsValueMap: JsObject = Json.parse(acqCarDetails.properties).asInstanceOf[JsObject]
            jsValueMap.as[Map[String, String]].filterKeys {
              "".contains(_) == false
            }
          }



      val equipmentList:Set[String] = Set("Test")
      PropCar(url=acqCarHeader.url, location=acqCarHeader.location, title=acqCarHeader.title, year=acqCarHeader.year, km=acqCarHeader.km, price=acqCarHeader.price, properties=propertiesMap, equipment=equipmentList, information=acqCarDetails.information, deleted=acqCarDetails.deleted, load_time=acqCarHeader.load_time, load_date=acqCarHeader.load_date)
      //PropCar(url=acqCarHeader.url, location=acqCarHeader.location, title=acqCarHeader.title, year=acqCarHeader.year, km=acqCarHeader.km, price=acqCarHeader.price, properties=Utility.Constants.EmptyMap, equipment=Utility.Constants.EmptyList, information=Utility.Constants.EmptyString, deleted=false, load_time=acqCarHeader.load_time, load_date=acqCarHeader.load_date)
    } else {
      PropCar(url=acqCarHeader.url, location=acqCarHeader.location, title=acqCarHeader.title, year=acqCarHeader.year, km=acqCarHeader.km, price=acqCarHeader.price, properties=Utility.Constants.EmptyMap, equipment=Utility.Constants.EmptyList, information=Utility.Constants.EmptyString, deleted=false, load_time=acqCarHeader.load_time, load_date=acqCarHeader.load_date)
    }


  }


  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }

  def closureFunction[E,D,R](enclosed: E)(gen: E => (D => R)) = gen(enclosed)

}
