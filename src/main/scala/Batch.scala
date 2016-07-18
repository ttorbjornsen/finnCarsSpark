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
  sc.setLogLevel("WARN")
  val csc = new CassandraSQLContext(sc)
  csc.setKeyspace("finncars")
  val hc = new HiveContext(sc)
  import hc.implicits._ //allows registering temptables
  val dao = new DAO(hc, csc)


  val deltaLoadDates = Utility.getDatesBetween(dao.getLatestLoadDate("prop_car_daily"), LocalDate.now) //prop_car_daily is the target table, check last load date
  val rddDeltaLoadAcqHeaderPerPartition  = deltaLoadDates.map { date =>
    sc.cassandraTable[AcqCarHeader]("finncars", "acq_car_header").
      where("load_date = ?", date)
  }

  val rddDeltaLoadAcqHeader = sc.union(rddDeltaLoadAcqHeaderPerPartition)
  val rddDeltaLoadAcqHeaderPairRDD = rddDeltaLoadAcqHeader.map(row =>
    ((row.load_date, row.url),row)
  )

  val rddDeltaLoadAcqDetailsPerPartition = deltaLoadDates.map(date => sc.cassandraTable[AcqCarDetails]("finncars", "acq_car_details").
  where("load_date = ?", date)
  )
  val rddDeltaLoadAcqDetails = sc.union(rddDeltaLoadAcqDetailsPerPartition)

  val rddDeltaLoadAcqDetailsPairRDD = rddDeltaLoadAcqDetails.map(row =>
    ((row.load_date, row.url),row)
  )

  val rddJoinCarHeaderDetails = rddDeltaLoadAcqHeaderPairRDD.join(rddDeltaLoadAcqDetailsPairRDD)



  //if successful, write load date to t


//  val propCarRDD = sc.parallelize(acqCarHeaderList.map(hdr => dao.createPropCar(hdr)))
//  propCarRDD.saveToCassandra("finncars", "prop_car_daily")
//
//  //println(propCarRDD.count + " records written to prop_car_daily")
//  println(numOfCars + " records written to prop_car_daily")


}
