import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.jsoup.{HttpStatusException, Jsoup}
import org.jsoup.nodes.{Entities, _}
import org.jsoup.nodes.Document.OutputSettings
import play.api.libs.json._
import org.jsoup.select.Elements
import org.jsoup.nodes.Document.OutputSettings

import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import java.util.HashMap

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import scala.util.{Failure, Success}
import java.net.URL
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, ZoneId}
import java.util
import java.util.HashMap

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */


object Utility {

  def createAcqCarDetailsObject(i:Int, jsonCarHdr:JsValue)= {
    val url = jsonCarHdr.\\("group").head(i).\("title").head.\("href").as[String]
    val jsonCarDetail = scrapeCarDetails(url)
    val carProperties = jsonCarDetail("properties").toString
    val carEquipment = jsonCarDetail("equipment").toString
    val carInformation = jsonCarDetail("information").toString
    val deleted = jsonCarDetail("deleted").as[Boolean]
    val load_time = jsonCarHdr.\\("timestamp")(0).as[Long]
    val load_date = new java.util.Date(load_time).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarDetails(url = url, properties = carProperties, equipment = carEquipment, information = carInformation, deleted = deleted, load_time = load_time, load_date = load_date)
  }


  def createAcqCarHeaderObject(i:Int, jsonCarHdr:JsValue) = {
    val location = jsonCarHdr.\\("group")(0)(i).\("location")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val url = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("href").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val title = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val year = jsonCarHdr.\\("group")(0)(i).\("year")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val km = jsonCarHdr.\\("group")(0)(i).\("km")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val price = jsonCarHdr.\\("group")(0)(i).\("price")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val load_time = jsonCarHdr.\\("timestamp")(0).as[Long]
    val load_date = new java.util.Date(load_time).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarHeader(title=title, url=url, location=location, year=year, km=km, price=price, load_time=load_time, load_date=load_date)
  }

  def getURL(url: String)(retry: Int): Try[Document] = {
    Try(Jsoup.connect(url).get)
      .recoverWith {
        case _ if(retry > 0) => {
          Thread.sleep(3000)
          println("Retry url " + url + " - " + retry + " retries left")
          getURL(url)(retry - 1)
        }
      }
  }
  def scrapeCarDetails(url:String):Map[String, JsValue]= {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78647939"
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //sold
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78601940" //deleted page
    val validUrl = url.replace("\"", "")
    val doc: Try[Document] = getURL(validUrl)(10)
    doc match {
      case Success(doc) =>
        val carPropElements: Element = doc.select(".mvn+ .col-count2from990").first()
        var carPropMap = Map[String, String]()

        if (carPropElements != null) {
          var i = 0
          for (elem: Element <- carPropElements.children()) {
            if ((i % 2) == 0) {
              val key = elem.text
              val value = elem.nextElementSibling().text
              carPropMap += (key.asInstanceOf[String] -> value.asInstanceOf[String])
            }
            i = i + 1
          }
        } else carPropMap = Map("MissingKeys" -> "MissingValues")

        var carEquipListBuffer: ListBuffer[String] = ListBuffer()
        val carEquipElements: Element = doc.select(".col-count2upto990").first()
        if (carEquipElements != null) {
          for (elem: Element <- carEquipElements.children()) {
            carEquipListBuffer += elem.text
          }
        } else carEquipListBuffer = ListBuffer("MissingValues")

        val carInfoElements: Element = doc.select(".object-description p[data-automation-id]").first()
        val carInfoElementsText = {
          if (carInfoElements != null) {
            carInfoElements.text
          } else "MissingValues"
        }

        val carPriceElement: Element = doc.select(".mtn.r-margin").first()
        val carPriceText = {
          if (carPriceElement != null) carPriceElement.text else "MissingValue"
        }

        val carTitleElement: Element = doc.select(".tcon").first()
        val carTitleText = {
          if (carTitleElement != null) carTitleElement.text else "MissingValue"
        }

        val carLocationElement: Element = doc.select(".hide-lt768 h2").first()
        val carLocationText= {
          if (carLocationElement != null) carLocationElement.text else "MissingValue"
        }

        val carYearElement: Element = doc.select("hr+ .col-count2from990 dd:nth-child(2) , .mvn+ .col-count2from990 dt:nth-child(2)").first()
        val carYearText= {
          if (carYearElement != null) carYearElement.text else "MissingValue"
        }

        val carKMElement: Element = doc.select(".mvn+ .col-count2from990 dd:nth-child(6)").first()
        val carKMText = {
          if (carKMElement != null) carKMElement.text else "MissingValue"
        }

        val jsObj = Json.obj("url" -> url, "properties" -> carPropMap, "information" -> carInfoElementsText, "equipment" -> carEquipListBuffer.toList, "deleted" -> false, "title" -> carTitleText, "location" -> carLocationText, "price" -> carPriceText, "year" -> carYearText, "km" -> carKMText)
        jsObj.value.toMap
      case Failure(e) => {
        println("URL " + url + " has been deleted.")
        val jsObj = Json.obj("url" -> url, "properties" -> Map("NULL" -> "NULL"), "information" -> "NULL", "equipment" -> ListBuffer("NULL").toList, "deleted" -> true, "title" -> "NULL", "location" -> "NULL", "price" -> "NULL", "year" -> "NULL", "km" -> "NULL")
        jsObj.value.toMap
      }
    }
  }


  def getMapFromJsonMap(jsonString:String, excludedKeys:Seq[String]=Seq("None")):HashMap[String,String] = {
    //    val keys = Seq("Salgsform", "Girkasse")
    //    val jsonString = "{\"Salgsform\":\"Bruktbil til salgs\",\"Girkasse\":\"Automat\",\"Antall seter\":\"5\"}"
    val jsValueMap: JsValue = Json.parse(jsonString)
    val propertiesMap = jsValueMap.as[Map[String,String]]
    val hashMap = new HashMap[String, String]
    propertiesMap.map{case(k,v) => if (!excludedKeys.contains(k)) hashMap.put(k,v) }
    hashMap
  }

  def getSetFromJsonArray(jsonString:String, excludedElements:Seq[String]=Seq("None")):Set[String] = {
    //    val elements = Seq("Automatisk klimaanlegg", "Skinnseter")
    val jsValueArray:JsValue = Json.parse(jsonString)
    val set = jsValueArray.as[Set[String]]
    set.filter(x => !excludedElements.contains(x))
  }

  def getStringFromJsonString(jsonString:String):String = {
    //    val jsonString = "\"Fin bil\""
    Json.parse(jsonString).as[String]
  }

  def setupCassandraTestKeyspace() = {
    val conf = new SparkConf().setAppName("Testing").setMaster("local[*]").set("spark.cassandra.connection.host", "192.168.56.56")
    val ddl_prod = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\c.ddl").getLines.mkString
    val ddl_test = ddl_prod.replace("finncars", "test_finncars")
    val ddl_test_split = ddl_test.split(";")
    val ddl_test_cmds = ddl_test_split.map(elem => elem + ";")

    CassandraConnector(conf).withSessionDo { session =>
      ddl_test_cmds.map { cmd =>
        println(cmd)
        session.execute(cmd)
      }
    }
  }

  def parseKM(km:String):Int = {
    //val km = "99 000 km"
    val parsedKM = km.replace(" ", "").replace("km", "").replace("\"", "")
    if (parsedKM.forall(_.isDigit)) parsedKM.toInt else -1
  }

  def parseFinnkode(url:String):Int = {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=72921101"
    val parsedFinnkode = url.substring(url.length-8,url.length).replace("\"", "")
    if (parsedFinnkode.forall(_.isDigit)) parsedFinnkode.toInt else -1
  }


  def parseYear(year:String):Int = {
    //val year = "2007"
    val parsedYear = year.replace("\"", "").replace("\"", "")
    if (parsedYear.forall(_.isDigit)) parsedYear.toInt else -1
  }

  def carMarkedAsSold(price:String):Boolean = {
    if (price == "Solgt") true else false
  }

  def getDaysBetweenStringDates(dateStart:String, dateFinish:String):Int = {
    LocalDate.parse(dateStart).until(LocalDate.parse(dateFinish), ChronoUnit.DAYS).toInt
  }

  def getDatesBetween(dateStart:LocalDate, dateEnd:LocalDate):Seq[String] = {
    //val dateStart = LocalDate.now
    //val dateEnd = LocalDate.now.plusDays(-365)
    val daysBetween = dateStart.until(dateEnd, ChronoUnit.DAYS).toInt
    val listOfDays = ListBuffer[String]()

    for (i <- 0 to daysBetween by 1) {
      listOfDays += (dateStart.plusDays(i)).toString
    }

    listOfDays.toList
  }




  def getFirstRecordFromFilteredPropCarRDD(propCarPairRDD:RDD[(String,PropCar)], filterFunc: ((String, PropCar)) => Boolean = (t => true)):RDD[(String, PropCar)] = {
    val propCarPairRDDFiltered = propCarPairRDD.filter(filterFunc)
    val firstRecordsRDD = propCarPairRDDFiltered.reduceByKey((c1,c2) => if (c1.load_time < c2.load_time) c1 else c2)
    //val firstRecordsRDD = propCarPairRDD.reduceByKey((c1,c2) => if (c1.load_time < c2.load_time) c1 else c2)
    firstRecordsRDD
  }

  def getLastPropCarAll(propCarPairRDD: RDD[(String,PropCar)], filterFunc: ((String, PropCar)) => Boolean = (t => true)):RDD[(String, PropCar)] = {
    val propCarPairRDDFiltered = propCarPairRDD.filter(filterFunc)
    val lastRecordsRDD = propCarPairRDDFiltered.reduceByKey((c1,c2) => {
      if (c1.load_time >= c2.load_time) c1 else c2
    })
    lastRecordsRDD
  }
  //    val jsonString = "[\"Aluminiumsfelger\",\"Automatisk klimaanlegg\",\"Skinnseter\"]"

  def popTopPropCarRecord(propCarPairRDD:RDD[(String,PropCar)], url:String):PropCar = {
    val topPropCar = propCarPairRDD.lookup(url)(0)
    topPropCar
  }

  def getBtlKfFirstLoad(firstPropCar:PropCar):BtlCar ={
    BtlCar(price_first = firstPropCar.price, load_date_first = firstPropCar.load_date)
  }

  def getBtlKfLastLoad(lastPropCar:PropCar):BtlCar ={
    BtlCar(url=lastPropCar.url,
    finnkode=lastPropCar.finnkode,
    title=lastPropCar.title,
    location=lastPropCar.location,
    year=lastPropCar.year,
    km=lastPropCar.km,
    price_last = lastPropCar.price,
    sold = lastPropCar.sold,
    deleted = lastPropCar.deleted,
    load_date_latest = lastPropCar.load_date,
    automatgir = hasAutomatgir(lastPropCar.properties)
    )
  }

  def getBtlKfEventDates(propCarPairRDD:RDD[(String,PropCar)], url:String):BtlCar= {
    val firstDeletedDatePropCar: PropCar = popTopPropCarRecord(getFirstRecordFromFilteredPropCarRDD(propCarPairRDD, (t => t._1 == url & t._2.deleted == true)), url)
    val firstSoldDatePropCar: PropCar = popTopPropCarRecord(getFirstRecordFromFilteredPropCarRDD(propCarPairRDD, (t => t._1 == url & t._2.sold == true)), url)
    BtlCar(sold_date = firstDeletedDatePropCar.load_date,
      deleted_date = firstSoldDatePropCar.load_date)
  }

  def hasAutomatgir(properties:HashMap[String, String]):Boolean = {
    properties.get("Girkasse") == "Automat"
  }

  def propCarToString(p:Product):String = {
    p.productIterator.map {
      case s: String => "\"" + s + "\""
      case hm: HashMap[_, _] => "new HashMap[String,String](Map" + hm.map(t => "\"" + t._1 + "\"" + "->" + "\"" + t._2 + "\"") + ")" //cannot convert HashMap without first specifying a Scala map
      case set: Set[_] => set.map(v => "\"" + v + "\"")
      case l:Long => l.toString + "L"
      case other => other.toString
    }.mkString (p.productPrefix + "(", ", ", ")").replace("ArrayBuffer", "")
  }

  def propCarToStringAndKey(p:Product, url:String):(String,String) = {
    (url, p.productIterator.map {
      case s: String => "\"" + s + "\""
      case hm: HashMap[_, _] => "new HashMap[String,String](Map" + hm.map(t => "\"" + t._1 + "\"" + "->" + "\"" + t._2 + "\"") + ")" //cannot convert HashMap without first specifying a Scala map
      case set: Set[_] => set.map(v => "\"" + v + "\"")
      case l:Long => l.toString + "L"
      case other => other.toString
    }.mkString (p.productPrefix + "(", ", ", ")").replace("ArrayBuffer", ""))
  }


  def saveToCSV(rdd:RDD[org.apache.spark.sql.Row]) = {
    val temp = rdd.map(row => row.mkString(";"))
    temp.coalesce(1).saveAsTextFile("/home/torbjorn/projects/temp_spark_output/")
  }

  def printCurrentMethodName() : Unit = println(Thread.currentThread.getStackTrace()(2).getMethodName)

  object Constants {
    val EmptyMap = new java.util.HashMap[String,String](Map("NULL" -> "NULL"))
    val EmptyList = Set("NULL")
    val EmptyString = "NULL"
    val EmptyInt = -1
    val EmptyDate = "1900-01-01"
    val ETLSafetyMargin = 7 //days
    val ETLFirstLoadDate = LocalDate.of(2016,7,1)
  }





}

