import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.hive._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._
import org.apache.spark.sql.{SQLContext, DataFrame}
import com.datastax.spark.connector._



/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */
object StreamFromKafkaToProp extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\torbjorn.torbjornsen\\Hadoop\\")
  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","192.168.56.56")
  //val conf = new SparkConf().setAppName("loadRaw").setMaster("spark://torbjorn-VirtualBox:7077").set("spark.cassandra.connection.host","192.168.56.56")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val csc = new CassandraSQLContext(sc)
  csc.setKeyspace("finncars")
  val hc = new HiveContext(sc)
  import hc.implicits._ //allows registering temptables
  val dao = new DAO(hc, csc)
  val kafkaParams = Map("metadata.broker.list" -> "192.168.56.56:9092")//, "auto.offset.reset" -> "smallest")
  val topics = Set("cars_header")
  //val fromOffsets = Map(new TopicAndPartition("finnCars", 0) -> 0L)
  //val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, fromOffsets, (mmd:MessageAndMetadata[String, String]) => mmd)
  val ssc = new StreamingContext(sc, Seconds(5)) //60 in production
  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  directKafkaStream.foreachRDD(
    (rdd, time) => {
      if (rdd.toLocalIterator.nonEmpty) {
        //when new data from Kafka is available
        println(rdd.count + " new Kafka messages to process")
        //val content = rdd.map(_._2).collect
        val content = rdd.map(_._2)

        content.foreach { jsonDoc =>
          val jsonCarHdr: JsValue = Json.parse(jsonDoc.mkString)
          val headerUrl = jsonCarHdr.\\("url").head.as[String]
          val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
          val acqCarHeaderList = Range(0, numOfCars).map(i =>
            Utility.createAcqCarHeaderObject(i, jsonCarHdr)).toList

          val acqCarHeaderDF = sc.parallelize(acqCarHeaderList).toDF

          acqCarHeaderDF.write.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
            mode(SaveMode.Append).
            save()

          //println(acqCarHeaderDF.count + " records written to acq_car_header")
          println(numOfCars + " records written to acq_car_header. Url: " + headerUrl)
          val acqCarDetailsList = Range(0, numOfCars).map(i =>
            Utility.createAcqCarDetailsObject(i, jsonCarHdr)).toList

          val acqCarDetailsDF = sc.parallelize(acqCarDetailsList).toDF()

          acqCarDetailsDF.write.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "acq_car_details", "keyspace" -> "finncars")).
            mode(SaveMode.Append).
            save()

          println(numOfCars + " records written to acq_car_details")

        }
      }
    })

  ssc.start()
  //ssc.stop(false) //for debugging in REPL
  ssc.awaitTermination()
}


