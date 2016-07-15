/**
  * Created by torbjorn.torbjornsen on 11.07.2016.
  */
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.datastax.spark.connector._


class DAO (_hc: SQLContext, _csc:CassandraSQLContext) extends java.io.Serializable{
  import _hc.implicits._

  /* returns the current price, but if the car is marked as sold, then the price from before it was sold is retrieved.  */
  def getLastPrice(acqCarHeader:AcqCarHeader):Int = {
    if (acqCarHeader.price != "Solgt") {
      val parsedPrice = acqCarHeader.price.replace(",-","").replace(" ","").replace("\"", "")
      if (parsedPrice.forall(_.isDigit)) parsedPrice.toInt else -1 //price invalid
    } else {
      val prevAcqCarHeaderNotSold = _csc.sparkContext.cassandraTable[AcqCarHeader]("finncars", "acq_car_header").
        where("url = ?", acqCarHeader.url).
        where("load_time <= ?", new java.util.Date(acqCarHeader.load_time)).
        filter(row => row.price != "Solgt").
        filter(row => row.load_date != acqCarHeader.load_date). //since we also want to find the price diff from yesterday
        collect

      if (prevAcqCarHeaderNotSold.length > 0) {
        val acqCarHeader = prevAcqCarHeaderNotSold.maxBy(_.load_time)
        val parsedPrice = acqCarHeader.price.replace(",-","").replace(" ","").replace("\"", "")
        if (parsedPrice.forall(_.isDigit)) parsedPrice.toInt else -1 //price invalid
      } else -1 //previous car price not found
    }
  }


  def createPropCar(acqCarHeader:AcqCarHeader):PropCar = {
    //val acqCarHeader = AcqCarHeader("Volkswagen Passat 1,6 TDI 105hk BlueMotion Business","http://m.finn.no/car/used/ad.html?finnkode=78537231","Kirkenes","2010","121 835 km","149 000,-",2016-07-04 13:41:21.477,2016-07-04))

    val prevAcqCarDetails = _csc.sparkContext.cassandraTable[AcqCarDetails]("finncars", "acq_car_details").
      where("url = ?", acqCarHeader.url).
      where("load_time <= ?", new java.util.Date(acqCarHeader.load_time)).
      filter(row => row.deleted == false).collect

    if (prevAcqCarDetails.length > 0) {
      val acqCarDetails = prevAcqCarDetails.maxBy(_.load_time)
      val propertiesMap:Map[String,String] = Utility.getMapFromJsonMap(acqCarDetails.properties)
      val equipmentList:Set[String] = Utility.getSetFromJsonArray(acqCarDetails.equipment)
      PropCar(url=acqCarHeader.url,
        finnkode=Utility.parseFinnkode(acqCarHeader.url),
        location=acqCarHeader.location,
        title=acqCarHeader.title,
        year=Utility.parseYear(acqCarHeader.year),
        km=Utility.parseKM(acqCarHeader.km),
        price=getLastPrice(acqCarHeader),
        properties=propertiesMap,
        equipment=equipmentList,
        information=acqCarDetails.information,
        sold=Utility.carMarkedAsSold(acqCarHeader.price),
        deleted=acqCarDetails.deleted,
        load_time=acqCarHeader.load_time,
        load_date=acqCarHeader.load_date)
    } else {
      PropCar(url=acqCarHeader.url,
        finnkode=Utility.parseFinnkode(acqCarHeader.url),
        location=acqCarHeader.location,
        title=acqCarHeader.title,
        year=Utility.parseYear(acqCarHeader.year),
        km=Utility.parseKM(acqCarHeader.km),
        price=getLastPrice(acqCarHeader),
        properties=Utility.Constants.EmptyMap,
        equipment=Utility.Constants.EmptyList,
        information=Utility.Constants.EmptyString,
        sold=Utility.carMarkedAsSold(acqCarHeader.price),
        deleted=true,
        load_time=acqCarHeader.load_time,
        load_date=acqCarHeader.load_date)
    }


  }

}
