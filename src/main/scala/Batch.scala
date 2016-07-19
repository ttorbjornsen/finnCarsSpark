import java.time.LocalDate

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.hive.HiveContext
import com.datastax.spark.connector._




/**
  * Created by torbjorn.torbjornsen on 18.07.2016.
  */
object Batch extends App{
  System.setProperty("hadoop.home.dir", "C:\\Users\\torbjorn.torbjornsen\\Hadoop\\")
  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","192.168.56.56")
  val sc = new SparkContext(conf)
  sc.setLogLevel("INFO")
  val csc = new CassandraSQLContext(sc)
  csc.setKeyspace("finncars")
  val hc = new HiveContext(sc)
  import hc.implicits._ //allows registering temptables
  val dao = new DAO(hc, csc)


  val deltaLoadDates = Utility.getDatesBetween(dao.getLatestLoadDate("prop_car_daily"), LocalDate.now) //prop_car_daily is the target table, check last load date

  //get all dates, not only delta
  //val deltaLoadDates = Utility.getDatesBetween(LocalDate.of(2016,7,15), LocalDate.now) //prop_car_daily is the target table, check last load date

//  val url = "http://m.finn.no/car/used/ad.html?finnkode=79020080"

  val rddDeltaLoadAcqHeaderDatePartition  = deltaLoadDates.map { date =>
    sc.cassandraTable[AcqCarHeader]("finncars", "acq_car_header").
      where("load_date = ?", date)//.where("url = ?", url)
  }

  val rddDeltaLoadAcqHeaderLastLoadTimePerDay = sc.union(rddDeltaLoadAcqHeaderDatePartition).
    map(row => ((row.load_date, row.url),(AcqCarHeader(title=row.title, url=row.url, location=row.location, year=row.year, km=row.km, price=row.price, load_time=row.load_time, load_date=row.load_date)))).reduceByKey((x,y) => if(y.load_time > x.load_time) y else x)

  rddDeltaLoadAcqHeaderLastLoadTimePerDay.cache


  val rddDeltaLoadAcqDetailsDatePartition = deltaLoadDates.map(date => sc.cassandraTable[AcqCarDetails]("finncars", "acq_car_details").
    where("load_date = ?", date)//.where("url = ?", url)
  )

  val rddDeltaLoadAcqDetailsLastLoadTimePerDay = sc.union(rddDeltaLoadAcqDetailsDatePartition).map(row =>
    ((row.load_date, row.url),(AcqCarDetails(url=row.url, load_date=row.load_date, load_time=row.load_time, properties=row.properties, equipment=row.equipment, information=row.information, deleted=row.deleted)))).
    reduceByKey((x,y) => if(y.load_time > x.load_time) y else x)

  val propCarRDD = rddDeltaLoadAcqHeaderLastLoadTimePerDay.join(rddDeltaLoadAcqDetailsLastLoadTimePerDay ).map { row =>
    PropCar(load_date=row._1._1, url=row._1._2, finnkode=Utility.parseFinnkode(row._1._2), title=row._2._1.title, location=row._2._1.location, year=Utility.parseYear(row._2._1.year), km=Utility.parseKM(row._2._1.km), price=dao.getLastPrice(row._2._1.price, row._2._1.url, row._2._1.load_date, row._2._1.load_time), properties=Utility.getMapFromJsonMap(row._2._2.properties), equipment=Utility.getSetFromJsonArray(row._2._2.equipment), information=Utility.getStringFromJsonString(row._2._2.information), sold=Utility.carMarkedAsSold(row._2._1.price), deleted=row._2._2.deleted, load_time=row._2._1.load_time)
  }
  //NOTE! Number of records between PropCar and AcqHeader/AcqDetails may differ. E.g. when traversing Finn header pages, some cars will be bound to come two times when new cars are entered. Duplicate entries may in other words occur due to load_time being part of Acq* primary key.
  propCarRDD.saveToCassandra("finncars", "prop_car_daily")

  /* Start populating BTL-layer */
  val btlDeltaUrlList = rddDeltaLoadAcqHeaderLastLoadTimePerDay.map(row => row._1._2).distinct.collect
  val deltaLoadDatesBTL = Utility.getDatesBetween(LocalDate.now.plusDays(-365), LocalDate.now) //assume all cars have been at finn for less than x amount of days. If this is not the case, the car is not updated correctly to BTL layer.

  // traverse all urls that have been updated since last (delta)
  btlDeltaUrlList.map { url =>
    deltaLoadDatesBTL.map { date =>
      val partition = deltaLoadDatesBTL.map { date => sc.cassandraTable[PropCar]("finncars", "prop_car_daily").
        where("load_date = ?", date).
        where("url = ?", url)
      }
      val propCarUrlRDD = sc.union(partition)
    }
  }
}
