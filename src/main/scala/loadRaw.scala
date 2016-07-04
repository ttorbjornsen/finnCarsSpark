import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.hive._
import play.api.libs.json._

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */
object loadRaw extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\torbjorn.torbjornsen\\Hadoop\\")
  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","192.168.56.56")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val hc = new HiveContext(sc)
  import hc.implicits._ //allows registering temptables
  val ssc = new StreamingContext(sc, Seconds(5)) //60 in production

  val kafkaParams = Map("metadata.broker.list" -> "192.168.56.56:9092")
  val topics = Set("finnCars")
  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  directKafkaStream.foreachRDD(
    (rdd,time) => {
      rdd.foreach(println)
    })

  val rawData = sc.textFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\debug_files\\carsFinnOrig.json")
  val rawDataArray = rawData.collect
  rawDataArray(0) = "{"
  rawDataArray(rawDataArray.length-1) = "}"
  val rawDataListString = rawDataArray.mkString :: Nil


  val json: JsValue = Json.parse(rawDataArray.mkString)

  val i = 0

  val location = json.\\("group")(0)(i).\("location")(0).\("text")
  val title = json.\\("group")(0)(i).\("title")(0).\("text")
  val url = json.\\("group")(0)(i).\("title")(0).\("href")
  val year = json.\\("group")(0)(i).\("year")(0).\("text")
  val km = json.\\("group")(0)(i).\("km")(0).\("text")
  val price = json.\\("group")(0)(i).\("price")(0).\("text")




  val rawData2 = hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\debug_files\\carsFinn2.json")
  rawData2.show


  val rawDataValidJson = sc.parallelize(rawDataArray.mkString :: Nil) //Nil to make the array to a List of [String] not Char


  hc.read.json(rawDataValidJson).show


  println(rawDataArray.mkString(""))
  ssc.start()
  ssc.awaitTermination()

  //
//
//  val kafkaStream = KafkaUtils.createStream(streamingContext,
//    [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])
}
