import java.time.ZoneId

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.hive._
import play.api.libs.json._


case class AcqCarHeader(title:String, url:String, location:String, year: String, km: String, price: String, load_time:Long, load_date:String)

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */
object loadRaw extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\torbjorn.torbjornsen\\Hadoop\\")
  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","192.168.56.56")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val csc = new CassandraSQLContext(sc)
  csc.setKeyspace("finncars")
  val hc = new HiveContext(sc)
  import hc.implicits._ //allows registering temptables
  val kafkaParams = Map("metadata.broker.list" -> "192.168.56.56:9092", "auto.offset.reset" -> "smallest")
  val topics = Set("finnCars")
  //val fromOffsets = Map(new TopicAndPartition("finnCars", 0) -> 0L)
  //val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, fromOffsets, (mmd:MessageAndMetadata[String, String]) => mmd)
  val ssc = new StreamingContext(sc, Seconds(5)) //60 in production
  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  directKafkaStream.foreachRDD(
    (rdd,time) => {
      if (rdd.toLocalIterator.nonEmpty) {
        //when new data from Kafka is available
        println(rdd.count)

        //val content = rdd.map(_._2).collect
        val content = rdd.map(_._2)

        content.foreach{jsonDoc =>
          val json: JsValue = Json.parse(jsonDoc.mkString)
          val numOfCars = json.\\("group")(0).as[JsArray].value.size
          val acqCarHeaderList = Range(0, numOfCars).map(i =>
            Utility.createAcqCarHeaderObject(i, json)).toList

          val acqCarHeaderDF = sc.parallelize(acqCarHeaderList).toDF

          acqCarHeaderDF.write.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
            mode(SaveMode.Append).
            save()

          println(acqCarHeaderDF.count + " records written to acq_car_header")
        }
      }})

  ssc.start()
  ssc.stop(false)
  //ssc.awaitTermination()


  val rawData = sc.textFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\debug_files\\carsFinnOrig.json")
  val rawDataArray = rawData.collect
  rawDataArray(0) = "{"
  rawDataArray(rawDataArray.length-1) = "}"
  val json: JsValue = Json.parse(rawDataArray.mkString)
  val numOfCars = json.\\("group")(0).as[JsArray].value.size
  val extractTime = json.\\("timestamp")(0).as[Long]






  //
//
//  val kafkaStream = KafkaUtils.createStream(streamingContext,
//    [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])
}
