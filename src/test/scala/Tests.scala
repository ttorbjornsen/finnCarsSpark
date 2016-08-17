import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.mkuthan.spark.SparkSqlSpec
import org.scalatest.{BeforeAndAfter, FunSpec, FunSuite, Matchers}
import play.api.libs.json._
import java.util.HashMap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.collection.JavaConversions._
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive._

import scala.collection.mutable.ArrayBuffer
import java.sql.{Date, Timestamp}
import java.time.LocalDate

import scala.collection.JavaConversions._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success}



/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers with SparkSqlSpec{

  private var dao:DAO = _
  private var randomAcqCarHeader:AcqCarHeader = _
  private var testPropCarRDD:RDD[PropCar] = _
  private var testPropCarPairRDD:RDD[(String, PropCar)] = _
  override def beforeAll():Unit={
    super.beforeAll()
    val _csc = csc
    val _hc = hc
    import _hc.implicits._

    //Utility.setupCassandraTestKeyspace() //create keyspace test_finncars

//    val dfAcqCarHeader = _hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarHeader.json").toDF()
//    dfAcqCarHeader.write.
//      format("org.apache.spark.sql.cassandra").
//      options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
//      mode(SaveMode.Append).
//      save()
//
//    val dfAcqCarDetails = _hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarDetails.json").toDF()
//    dfAcqCarDetails.write.
//      format("org.apache.spark.sql.cassandra").
//      options(Map("table" -> "acq_car_details", "keyspace" -> "finncars")).
//      mode(SaveMode.Append).
//      save()
//
//    dfAcqCarHeader.registerTempTable("acq_car_header")
//    dfAcqCarDetails.registerTempTable("acq_car_details")
//
//    val dfCarHeader = _csc.read.
//      format("org.apache.spark.sql.cassandra").
//      options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
//      load().
//      select("title", "url", "location", "year", "km", "price", "load_time", "load_date").
//      filter($"url" === "http://m.finn.no/car/used/ad.html?finnkode=78866263").
//      limit(1)

    //REPL : USE val

    val dfRandomCarHeader = _csc.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
      load().
      select("title", "url", "location", "year", "km", "price", "load_time", "load_date").
      limit(1)
//REPL : USE val
      randomAcqCarHeader = dfRandomCarHeader.map(row => AcqCarHeader(title=row.getString(0), url=row.getString(1), location=row.getString(2), year=row.getString(3), km=row.getString(4), price=row.getString(5), load_time=(row(6).asInstanceOf[java.util.Date]).getTime(), load_date=row.getString(7))).collect.toList(0)


    //propCarRDD.take(9).map(row => println(Utility.propCarToString(row)))
    //REPL: USE val
    testPropCarRDD = sc.parallelize(Seq(
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79166971", "2016-07-18", 79166971, "Volkswagen Passat 1,6 Comfortline", "Stjørdal", 2000, 191200, 27538, new HashMap[String,String](Map("Effekt"->"101 Hk", "Km.stand"->"191 200 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"BP52393", "CO2 utslipp"->"202 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2000", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 282 kg", "1. gang registrert"->"25.07.2000", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Sølv", "Antall eiere"->"4", "Sylindervolum"->"1,6 l")), Set("MissingValues"), "Jeg selger denne bilen, grunnen til at jeg skal kjøpe stor bil, EU godkjent til 2018 april. Har ikke noen problem. spør mer informasjon om bilen.", false, false, 1468837502495L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79021972", "2016-07-16", 79021972, "Volvo 940 aut", "Svene", 1995, 395352, 16538, new HashMap[String,String](Map("Effekt"->"135 Hk", "Km.stand"->"395 352 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Bakhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DH52357", "Bilen står i"->"Norge", "Årsmodell"->"1995", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 440 kg", "1. gang registrert"->"04.05.1995", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Blå", "Antall eiere"->"3", "Sylindervolum"->"2,3 l")), Set("MissingValues"), "Dette er en grei bil, ikke strøken. Bilen er brukt som ekstra bil under rep. av annen bil. Bremse skiver og klosser er byttet. Eksos anlegg er nytt 3 deler. Motor olje og filter er bytte. Lakk rust er det, kanaler ser OK ut. Men ingen bemerkning på EU kontroll, godkjent til november 2017", false, false, 1468660323000L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79021972", "2016-07-15", 79021972, "Volvo 940 aut", "Svene", 1995, 395352, 16538, new HashMap[String,String](Map("Effekt"->"135 Hk", "Km.stand"->"395 352 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Bakhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DH52357", "Bilen står i"->"Norge", "Årsmodell"->"1995", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 440 kg", "1. gang registrert"->"04.05.1995", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Blå", "Antall eiere"->"3", "Sylindervolum"->"2,3 l")), Set("MissingValues"), "Dette er en grei bil, ikke strøken. Bilen er brukt som ekstra bil under rep. av annen bil. Bremse skiver og klosser er byttet. Eksos anlegg er nytt 3 deler. Motor olje og filter er bytte. Lakk rust er det, kanaler ser OK ut. Men ingen bemerkning på EU kontroll, godkjent til november 2017", false, false, 1468569871692L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79021972", "2016-07-18", 79021972, "Volvo 940 aut", "Svene", 1995, 395352, 16538, new HashMap[String,String](Map("Effekt"->"135 Hk", "Km.stand"->"395 352 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Bakhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DH52357", "Bilen står i"->"Norge", "Årsmodell"->"1995", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 440 kg", "1. gang registrert"->"04.05.1995", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Blå", "Antall eiere"->"3", "Sylindervolum"->"2,3 l")), Set("MissingValues"), "Dette er en grei bil, ikke strøken. Bilen er brukt som ekstra bil under rep. av annen bil. Bremse skiver og klosser er byttet. Eksos anlegg er nytt 3 deler. Motor olje og filter er bytte. Lakk rust er det, kanaler ser OK ut. Men ingen bemerkning på EU kontroll, godkjent til november 2017", false, false, 1468830669000L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79030138", "2016-07-18", 79030138, "Saab 9-5 2.0TURBO, AUTOMAT, CRUISE,F1 GIRING", "Kolbotn", 2003, 305000, 23038, new HashMap[String,String](Map("Effekt"->"150 Hk", "Km.stand"->"305 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"ad95736", "CO2 utslipp"->"251 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2003", "Karosseri"->"Stasjonsvogn", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Sølv", "Sylindervolum"->"2 l")), Set("Lettmet. felg sommer", "Sentrallås", "Sideairbager", "Cruisekontroll", "Kjørecomputer", "Antispinn", "Airbag foran", "Farget glass", "Hengerfeste", "El.vinduer", "Startsperre", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Alarm", "Metallic lakk", "Elektrisk sete m. memory", "Bagasjeromstrekk", "Stålbjelker", "Midtarmlene", "Air Condition", "ABS-bremser", "Multifunksjonsratt", "Skinninteriør"), "EN MEGET POPULÆR, VELUTSTYRT, EKSKLUSIV, SIKKER OG ROMSLIG BIL. Bilen fremstår som meget hel og fin. Bruker lite bensin, er veldig driftsikker bil og veldig økonomisk da den er kun er 2.0 liter bensin. Dette er en meget velutstyrt utgave av Saab 95 med: Automat gir, F1 giring på ratt, Cruise controll, Elekrisk memory sete, Skinn, Klima osv++ Utrolig behagelig bil å kjøre med stor bagasjeplass. Utrolig gode og behagelige skinn seter. Under panseret finner man en 2.0 liter bensin motor med turbo som yter hele 150 hester. Motor og girkasse jobber bra sammen. EU godkjent til 30 JUNI 2017. Ring 47442525 for mer info", false, false, 1468837517823L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79052280", "2016-07-15", 79052280, "Volvo V70", "Kongsvinger", 2001, 279000, 31538, new HashMap[String,String](Map("Effekt"->"170 Hk", "Km.stand"->"279 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DK73754", "Bilen står i"->"Norge", "Årsmodell"->"2001", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 479 kg", "1. gang registrert"->"05.02.2004", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Grønn", "Antall eiere"->"2", "Sylindervolum"->"2,4 l")), Set("Sentrallås", "Cruisekontroll", "Lettmet. felg vinter", "Airbag foran", "Sommerhjul", "Hengerfeste", "El.vinduer", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Alarm", "Vinterhjul", "Motorvarmer", "Air Condition", "ABS-bremser"), "meget fin bil å kjøre ingen rust fin bade innvendig og utvendig NYLIG EU GODKJENT Neste frist for godkjent EU-kontroll 30.04.2018", false, false, 1468569503691L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79154538", "2016-07-18", 79154538, "Ford Focus 1,6 Comfort", "Løken", 2002, 265913, 15528, new HashMap[String,String](Map("Effekt"->"101 Hk", "Km.stand"->"265 913 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"PP43120", "CO2 utslipp"->"165 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2002", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 105 kg", "1. gang registrert"->"04.03.2002", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Svart", "Antall eiere"->"5", "Sylindervolum"->"1,6 l")), Set("MissingValues"), "Skiftet begge hjul lager foran. Topp deksel pakning og ABS føler. I meget bra stand. Noe overflaterust er pusset og lakkert over hjemme. Bra og billig bruksbil. Pris kan diskuteres ved rask handel.", false, false, 1468837517501L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79032963", "2016-07-15", 79032963, "Audi A4 1,9 TDI 130hk Multitronic", "Rådal", 2004, 284000, 29538, new HashMap[String,String](Map("Effekt"->"131 Hk", "Km.stand"->"284 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"BR60403", "CO2 utslipp"->"151 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2004", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 485 kg", "Interiørfarge"->"Sort", "1. gang registrert"->"21.02.2008", "Drivstoff"->"Diesel", "Antall seter"->"5", "Farge"->"Sølv", "Antall eiere"->"4", "Sylindervolum"->"1,9 l")), Set("MissingValues"), "Mye bil for lite penger! EU godkjent til April 2018.", false, false, 1468569858155L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=79032583", "2016-07-18", 79032583, "Audi A4 2,0TDI,143HK,KLIMA,KROK,EU-OK NOV18", "Kristiansand S", 2009, 184000, 129988, new HashMap[String,String](Map("Effekt"->"143 Hk", "Km.stand"->"184 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "CO2 utslipp"->"149 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2009", "Karosseri"->"Stasjonsvogn", "1. gang registrert"->"16.05.2008", "Drivstoff"->"Diesel", "Antall seter"->"5", "Farge"->"Svart", "Antall eiere"->"2", "Sylindervolum"->"2 l")), Set("Lettmet. felg sommer", "Sentrallås", "Sideairbager", "Parkeringsensor bak", "Lettmet. felg vinter", "Kjørecomputer", "Antispinn", "Airbag foran", "Farget glass", "Sommerhjul", "Hengerfeste", "Xenonlys", "El.vinduer", "Startsperre", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Regnsensor", "Metallic lakk", "Vinterhjul", "Antiskrens", "Takrails", "Bagasjeromstrekk", "Diesel-partikkelfilter", "Midtarmlene", "Air Condition", "ABS-bremser"), "Pen bil med Servicehefte,Xennon,Klima,AUX,Ryggesensor,Tilhengerfeste +++ ISOFIX,Airbag passasjersete kan slås av. Registerreim byttet på 177140 den 25.02.16 Eu-godkjent til november 2018. Bare kontakt oss for en hyggelig bilprat,Finansiering ordner vi Vi holder til i Sørlandsparken utenfor Kr.sand Barstølveien 14 a. 2-etg", false, false, 1468837600615L),
    PropCar("http://m.finn.no/car/used/ad.html?finnkode=78973820", "2016-07-15", 78973820, "Renault Talisman Sport Tourer dCi 130 Zen", "Kjeller", 2016, 3000, 359000, new HashMap[String,String](Map("Effekt"->"130 Hk", "Km.stand"->"3 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"CF84392", "Bilen står i"->"Norge", "Garanti"->"Nybilgaranti", "Årsmodell"->"2016", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 579 kg", "Interiørfarge"->"Sort", "Fargebeskrivelse"->"Cosmos Blue", "Str. lasterom"->"572 l", "1. gang registrert"->"06.06.2016", "Garanti inntil"->"100 000 km", "Drivstoff"->"Diesel", "Antall seter"->"5", "Farge"->"Blå", "Sylindervolum"->"1,6 l")), Set("Lyktespylere", "Lakkerte utvendige speil", "Nødbremseforsterker", "Sentrallås", "Klimaanlegg med flere soner", "Stabilitetssystem antiskrens", "Massasje førerstol", "Parkeringsensor bak", "Oppvarmet speil", "Aluminiumsfelger", "Kjørecomputer", "Parkeringsensor foran", "Antispinn", "Sommerhjul", "Beltevarsler", "Nedfellbare bakseter", "Startsperre", "ABS bremser", "Korsryggjustering i førersete", "Lakkerte støtfangere", "Elektronisk bremsekraftfordeler", "Elektriske speil", "Keyless go", "Regnsensor", "Lengdejusterbart ratt", "Tonede ruter", "Elektriske vindusheiser", "Vinterhjul", "Navigasjonssystem", "Høydejusterbart ratt", "Automatisk klimaanlegg", "Isofix barnesetefesting", "Pollenfilter", "Kollisjonsputer", "Varme i seter", "Multifunksjonsratt", "Delte bakseter"), "http://www.dinside.no/936208/test-renault-talisman-utfordrer-selveste-vw-passat MotorForum AS Lillestrøm er din Mitsubishi, Renault og Dacia forhandler på Romerike. Vi er et komplett bilanlegg som har mekanisk verksted, samt et topp moderne skade/lakk verksted. Vi er lokalisert ved E6 rett ved Olavsgård hotell, her er det også gode bussmuligheter. Når du kjøper bil av oss henter vi deg enkelt på Gardermoen eller ved Flytoget. Vi tilbyr ellers fleksible finansierings- og forsikrings løsninger. Vi kan hjelpe deg gjennom hele søkeprosessen på en enkel måte. Ved å velge finansiering via våre samarbeidspartnere kan vi tilby veldig gode betingelser, og kan ordne billån på dagen. Om ønskelig vurderer vi gjerne din bil som innbyttebil. Ta gjerne kontakt med oss for mer informasjon, en hyggelig bilprat og et godt tilbud! Morten Vorvik 90508741- morten.vorvik@motorforum.no Espen Lysø 90561096 - espen.lyso@motorforum.no Pål Bredo Ruud 48899838 - pal.bredo.ruud@motorforum.no Våre Åpningstider: Man-Fre: 08.00-17.00 Tors: 08.00-19.00 Lør: 10-14", false, false, 1468569942114L)
    ))

    //propCarRDD.take(9).map(row => println(Utility.propCarToStringAndKey(row, "\"" + row.url + "\"")))
    //REPL: USE val
    testPropCarPairRDD = sc.parallelize(Seq(
      ("http://m.finn.no/car/used/ad.html?finnkode=79166971",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79166971", "2016-07-18", 79166971, "Volkswagen Passat 1,6 Comfortline", "Stjørdal", 2000, 191200, 27538, new HashMap[String,String](Map("Effekt"->"101 Hk", "Km.stand"->"191 200 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"BP52393", "CO2 utslipp"->"202 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2000", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 282 kg", "1. gang registrert"->"25.07.2000", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Sølv", "Antall eiere"->"4", "Sylindervolum"->"1,6 l")), Set("MissingValues"), "Jeg selger denne bilen, grunnen til at jeg skal kjøpe stor bil, EU godkjent til 2018 april. Har ikke noen problem. spør mer informasjon om bilen.", false, false, 1468837502495L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79021972",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79021972", "2016-07-16", 79021972, "Volvo 940 aut", "Svene", 1995, 395352, 16538, new HashMap[String,String](Map("Effekt"->"135 Hk", "Km.stand"->"395 352 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Bakhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DH52357", "Bilen står i"->"Norge", "Årsmodell"->"1995", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 440 kg", "1. gang registrert"->"04.05.1995", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Blå", "Antall eiere"->"3", "Sylindervolum"->"2,3 l")), Set("Lettmet. felg sommer", "Sentrallås", "Sideairbager", "Cruisekontroll", "Kjørecomputer", "Antispinn", "Airbag foran", "Farget glass", "Hengerfeste", "El.vinduer", "Startsperre", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Alarm", "Metallic lakk", "Elektrisk sete m. memory", "Bagasjeromstrekk", "Stålbjelker", "Midtarmlene", "Air Condition", "ABS-bremser", "Multifunksjonsratt", "Skinninteriør"), "Dette er en grei bil, ikke strøken. Bilen er brukt som ekstra bil under rep. av annen bil. Bremse skiver og klosser er byttet. Eksos anlegg er nytt 3 deler. Motor olje og filter er bytte. Lakk rust er det, kanaler ser OK ut. Men ingen bemerkning på EU kontroll, godkjent til november 2017", true, false, 1468660323000L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79021972",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79021972", "2016-07-15", 79021972, "Volvo 940 aut", "Svene", 1995, 395352, 23538, new HashMap[String,String](Map("Effekt"->"135 Hk", "Km.stand"->"395 352 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Bakhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DH52357", "Bilen står i"->"Norge", "Årsmodell"->"1995", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 440 kg", "1. gang registrert"->"04.05.1995", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Blå", "Antall eiere"->"3", "Sylindervolum"->"2,3 l")), Set("Lettmet. felg sommer", "Sentrallås", "Sideairbager", "Cruisekontroll", "Kjørecomputer", "Antispinn", "Airbag foran", "Farget glass", "Hengerfeste", "El.vinduer", "Startsperre", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Alarm", "Metallic lakk", "Elektrisk sete m. memory", "Bagasjeromstrekk", "Stålbjelker", "Midtarmlene", "Air Condition", "ABS-bremser", "Multifunksjonsratt", "Skinninteriør"), "Dette er en grei bil, ikke strøken. Bilen er brukt som ekstra bil under rep. av annen bil. Bremse skiver og klosser er byttet. Eksos anlegg er nytt 3 deler. Motor olje og filter er bytte. Lakk rust er det, kanaler ser OK ut. Men ingen bemerkning på EU kontroll, godkjent til november 2017", false, false, 1468569871692L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79021972",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79021972", "2016-07-18", 79021972, "Volvo 940 aut", "Svene", 1995, 395352, 16538, new HashMap[String,String](Map("Effekt"->"135 Hk", "Km.stand"->"395 352 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Bakhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DH52357", "Bilen står i"->"Norge", "Årsmodell"->"1995", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 440 kg", "1. gang registrert"->"04.05.1995", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Blå", "Antall eiere"->"3", "Sylindervolum"->"2,3 l")), Set("Lettmet. felg sommer", "Sentrallås", "Sideairbager", "Cruisekontroll", "Kjørecomputer", "Antispinn", "Airbag foran", "Farget glass", "Hengerfeste", "El.vinduer", "Startsperre", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Alarm", "Metallic lakk", "Elektrisk sete m. memory", "Bagasjeromstrekk", "Stålbjelker", "Midtarmlene", "Air Condition", "ABS-bremser", "Multifunksjonsratt", "Skinninteriør"), "Dette er en grei bil, ikke strøken. Bilen er brukt som ekstra bil under rep. av annen bil. Bremse skiver og klosser er byttet. Eksos anlegg er nytt 3 deler. Motor olje og filter er bytte. Lakk rust er det, kanaler ser OK ut. Men ingen bemerkning på EU kontroll, godkjent til november 2017", true, false, 1468830669000L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79030138",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79030138", "2016-07-18", 79030138, "Saab 9-5 2.0TURBO, AUTOMAT, CRUISE,F1 GIRING", "Kolbotn", 2003, 305000, 23038, new HashMap[String,String](Map("Effekt"->"150 Hk", "Km.stand"->"305 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"ad95736", "CO2 utslipp"->"251 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2003", "Karosseri"->"Stasjonsvogn", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Sølv", "Sylindervolum"->"2 l")), Set("Lettmet. felg sommer", "Sentrallås", "Sideairbager", "Cruisekontroll", "Kjørecomputer", "Antispinn", "Airbag foran", "Farget glass", "Hengerfeste", "El.vinduer", "Startsperre", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Alarm", "Metallic lakk", "Elektrisk sete m. memory", "Bagasjeromstrekk", "Stålbjelker", "Midtarmlene", "Air Condition", "ABS-bremser", "Multifunksjonsratt", "Skinninteriør"), "EN MEGET POPULÆR, VELUTSTYRT, EKSKLUSIV, SIKKER OG ROMSLIG BIL. Bilen fremstår som meget hel og fin. Bruker lite bensin, er veldig driftsikker bil og veldig økonomisk da den er kun er 2.0 liter bensin. Dette er en meget velutstyrt utgave av Saab 95 med: Automat gir, F1 giring på ratt, Cruise controll, Elekrisk memory sete, Skinn, Klima osv++ Utrolig behagelig bil å kjøre med stor bagasjeplass. Utrolig gode og behagelige skinn seter. Under panseret finner man en 2.0 liter bensin motor med turbo som yter hele 150 hester. Motor og girkasse jobber bra sammen. EU godkjent til 30 JUNI 2017. Ring 47442525 for mer info", false, false, 1468837517823L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79052280",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79052280", "2016-07-15", 79052280, "Volvo V70", "Kongsvinger", 2001, 279000, 31538, new HashMap[String,String](Map("Effekt"->"170 Hk", "Km.stand"->"279 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"DK73754", "Bilen står i"->"Norge", "Årsmodell"->"2001", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 479 kg", "1. gang registrert"->"05.02.2004", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Grønn", "Antall eiere"->"2", "Sylindervolum"->"2,4 l")), Set("Sentrallås", "Cruisekontroll", "Lettmet. felg vinter", "Airbag foran", "Sommerhjul", "Hengerfeste", "El.vinduer", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Alarm", "Vinterhjul", "Motorvarmer", "Air Condition", "ABS-bremser"), "meget fin bil å kjøre ingen rust fin bade innvendig og utvendig NYLIG EU GODKJENT Neste frist for godkjent EU-kontroll 30.04.2018", false, false, 1468569503691L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79154538",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79154538", "2016-07-18", 79154538, "Ford Focus 1,6 Comfort", "Løken", 2002, 265913, 15528, new HashMap[String,String](Map("Effekt"->"101 Hk", "Km.stand"->"265 913 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"PP43120", "CO2 utslipp"->"165 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2002", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 105 kg", "1. gang registrert"->"04.03.2002", "Drivstoff"->"Bensin", "Antall seter"->"5", "Farge"->"Svart", "Antall eiere"->"5", "Sylindervolum"->"1,6 l")), Set("MissingValues"), "Skiftet begge hjul lager foran. Topp deksel pakning og ABS føler. I meget bra stand. Noe overflaterust er pusset og lakkert over hjemme. Bra og billig bruksbil. Pris kan diskuteres ved rask handel.", false, false, 1468837517501L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79032963",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79032963", "2016-07-15", 79032963, "Audi A4 1,9 TDI 130hk Multitronic", "Rådal", 2004, 284000, 29538, new HashMap[String,String](Map("Effekt"->"131 Hk", "Km.stand"->"284 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Automat", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"BR60403", "CO2 utslipp"->"151 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2004", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 485 kg", "Interiørfarge"->"Sort", "1. gang registrert"->"21.02.2008", "Drivstoff"->"Diesel", "Antall seter"->"5", "Farge"->"Sølv", "Antall eiere"->"4", "Sylindervolum"->"1,9 l")), Set("MissingValues"), "Mye bil for lite penger! EU godkjent til April 2018.", false, false, 1468569858155L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=79032583",PropCar("http://m.finn.no/car/used/ad.html?finnkode=79032583", "2016-07-18", 79032583, "Audi A4 2,0TDI,143HK,KLIMA,KROK,EU-OK NOV18", "Kristiansand S", 2009, 184000, 129988, new HashMap[String,String](Map("Effekt"->"143 Hk", "Km.stand"->"184 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "CO2 utslipp"->"149 g/km", "Bilen står i"->"Norge", "Årsmodell"->"2009", "Karosseri"->"Stasjonsvogn", "1. gang registrert"->"16.05.2008", "Drivstoff"->"Diesel", "Antall seter"->"5", "Farge"->"Svart", "Antall eiere"->"2", "Sylindervolum"->"2 l")), Set("Lettmet. felg sommer", "Sentrallås", "Sideairbager", "Parkeringsensor bak", "Lettmet. felg vinter", "Kjørecomputer", "Antispinn", "Airbag foran", "Farget glass", "Sommerhjul", "Hengerfeste", "Xenonlys", "El.vinduer", "Startsperre", "Radio/CD", "Klimaanlegg", "Servostyring", "Oppvarmede seter", "Elektriske speil", "Regnsensor", "Metallic lakk", "Vinterhjul", "Antiskrens", "Takrails", "Bagasjeromstrekk", "Diesel-partikkelfilter", "Midtarmlene", "Air Condition", "ABS-bremser"), "Pen bil med Servicehefte,Xennon,Klima,AUX,Ryggesensor,Tilhengerfeste +++ ISOFIX,Airbag passasjersete kan slås av. Registerreim byttet på 177140 den 25.02.16 Eu-godkjent til november 2018. Bare kontakt oss for en hyggelig bilprat,Finansiering ordner vi Vi holder til i Sørlandsparken utenfor Kr.sand Barstølveien 14 a. 2-etg", false, false, 1468837600615L)),
      ("http://m.finn.no/car/used/ad.html?finnkode=78973820",PropCar("http://m.finn.no/car/used/ad.html?finnkode=78973820", "2016-07-15", 78973820, "Renault Talisman Sport Tourer dCi 130 Zen", "Kjeller", 2016, 3000, 359000, new HashMap[String,String](Map("Effekt"->"130 Hk", "Km.stand"->"3 000 km", "Salgsform"->"Bruktbil til salgs", "Girkasse"->"Manuell", "Antall dører"->"5", "Hjuldrift"->"Forhjulsdrift", "Avgiftsklasse"->"Personbil", "Reg.nr."->"CF84392", "Bilen står i"->"Norge", "Garanti"->"Nybilgaranti", "Årsmodell"->"2016", "Karosseri"->"Stasjonsvogn", "Vekt"->"1 579 kg", "Interiørfarge"->"Sort", "Fargebeskrivelse"->"Cosmos Blue", "Str. lasterom"->"572 l", "1. gang registrert"->"06.06.2016", "Garanti inntil"->"100 000 km", "Drivstoff"->"Diesel", "Antall seter"->"5", "Farge"->"Blå", "Sylindervolum"->"1,6 l")), Set("Lyktespylere", "Lakkerte utvendige speil", "Nødbremseforsterker", "Sentrallås", "Klimaanlegg med flere soner", "Stabilitetssystem antiskrens", "Massasje førerstol", "Parkeringsensor bak", "Oppvarmet speil", "Aluminiumsfelger", "Kjørecomputer", "Parkeringsensor foran", "Antispinn", "Sommerhjul", "Beltevarsler", "Nedfellbare bakseter", "Startsperre", "ABS bremser", "Korsryggjustering i førersete", "Lakkerte støtfangere", "Elektronisk bremsekraftfordeler", "Elektriske speil", "Keyless go", "Regnsensor", "Lengdejusterbart ratt", "Tonede ruter", "Elektriske vindusheiser", "Vinterhjul", "Navigasjonssystem", "Høydejusterbart ratt", "Automatisk klimaanlegg", "Isofix barnesetefesting", "Pollenfilter", "Kollisjonsputer", "Varme i seter", "Multifunksjonsratt", "Delte bakseter"), "http://www.dinside.no/936208/test-renault-talisman-utfordrer-selveste-vw-passat MotorForum AS Lillestrøm er din Mitsubishi, Renault og Dacia forhandler på Romerike. Vi er et komplett bilanlegg som har mekanisk verksted, samt et topp moderne skade/lakk verksted. Vi er lokalisert ved E6 rett ved Olavsgård hotell, her er det også gode bussmuligheter. Når du kjøper bil av oss henter vi deg enkelt på Gardermoen eller ved Flytoget. Vi tilbyr ellers fleksible finansierings- og forsikrings løsninger. Vi kan hjelpe deg gjennom hele søkeprosessen på en enkel måte. Ved å velge finansiering via våre samarbeidspartnere kan vi tilby veldig gode betingelser, og kan ordne billån på dagen. Om ønskelig vurderer vi gjerne din bil som innbyttebil. Ta gjerne kontakt med oss for mer informasjon, en hyggelig bilprat og et godt tilbud! Morten Vorvik 90508741- morten.vorvik@motorforum.no Espen Lysø 90561096 - espen.lyso@motorforum.no Pål Bredo Ruud 48899838 - pal.bredo.ruud@motorforum.no Våre Åpningstider: Man-Fre: 08.00-17.00 Tors: 08.00-19.00 Lør: 10-14", false, false, 1468569942114L))
      ))

    dao = new DAO(_hc, _csc)
   }

  describe("application") {
    ignore("should be able to extract and correctly parse details page") {
      val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //temp
      val carDetails: Map[String, JsValue] = Utility.scrapeCarDetails(url)

      carDetails("properties").as[Map[String, String]] should contain key("Årsmodell")
      carDetails("equipment").as[List[String]] should contain ("Vinterhjul")
      carDetails("information").as[String] should include ("Xenonpakke")
      carDetails("deleted").as[Boolean] should equal(false)

    }

    //temp removed due to 10 retries taking long time
    ignore("can handle deleted detail car pages from finn") {
      val url = "http://m.finn.no/car/used/ad.html?finnkode=76755775"
      val carDetails = Utility.scrapeCarDetails(url)

      carDetails("properties").as[Map[String, String]] should contain key("NULL")
      carDetails("equipment").as[List[String]] should contain ("NULL")
      carDetails("information").as[String] should include ("NULL")
      carDetails("deleted").as[Boolean] should equal(true)
    }

    it ("can parse propcar record correctly into btlcar record (i.e. derive extra properties") {
      val urlList = List("http://m.finn.no/car/used/ad.html?finnkode=79021972")

      val propCarYearDeletedCarsMap = sc.broadcast(Utility.getFirstRecordFromFilteredPropCarRDD(testPropCarPairRDD , (t => t._2.deleted == true)).collectAsMap)
      val propCarYearSoldCarsMap = sc.broadcast(Utility.getFirstRecordFromFilteredPropCarRDD(testPropCarPairRDD, (t => t._2.sold == true)).collectAsMap)

      /* START populate key figures in BTL based on the first record */
      val propCarFirstRecordsRDD = Utility.getFirstRecordFromFilteredPropCarRDD(testPropCarPairRDD)
      val propCarLastRecordsRDD = Utility.getLastPropCarAll(testPropCarPairRDD)



      val btlCar = urlList.map{url =>
        val btlCarKf_FirstLoad:BtlCar = Utility.getBtlKfFirstLoad(Utility.popTopPropCarRecord(propCarFirstRecordsRDD,url))
        val btlCarKf_LastLoad:BtlCar = Utility.getBtlKfLastLoad(Utility.popTopPropCarRecord(propCarLastRecordsRDD,url))
        val btlCarKf_sold_date = propCarYearSoldCarsMap.value.getOrElse(url, PropCar()).load_date
        val btlCarKf_deleted_date = propCarYearDeletedCarsMap.value.getOrElse(url, PropCar()).load_date

        BtlCar(url = url,
          finnkode = btlCarKf_LastLoad.finnkode,
          title = btlCarKf_LastLoad.title,
          location = btlCarKf_LastLoad.location,
          year = btlCarKf_LastLoad.year,
          km = btlCarKf_LastLoad.km,
          price_first = btlCarKf_FirstLoad.price_first,
          price_last = btlCarKf_LastLoad.price_last,
          price_delta = (btlCarKf_LastLoad.price_last - btlCarKf_FirstLoad.price_first),
          sold = btlCarKf_LastLoad.sold,
          sold_date = btlCarKf_sold_date,
          lead_time_sold = Utility.getDaysBetweenStringDates(btlCarKf_FirstLoad.load_date_first, btlCarKf_sold_date),
          deleted = btlCarKf_LastLoad.deleted,
          deleted_date = btlCarKf_deleted_date,
          lead_time_deleted = Utility.getDaysBetweenStringDates(btlCarKf_FirstLoad.load_date_first, btlCarKf_deleted_date),
          load_date_first = btlCarKf_FirstLoad.load_date_first,
          load_date_latest = btlCarKf_LastLoad.load_date_latest,
          automatgir = btlCarKf_LastLoad.automatgir,
          hengerfeste = btlCarKf_LastLoad.hengerfeste,
          skinninterior = btlCarKf_LastLoad.skinninterior,
          drivstoff = btlCarKf_LastLoad.drivstoff,
          sylindervolum = btlCarKf_LastLoad.sylindervolum,
          effekt = btlCarKf_LastLoad.effekt,
          regnsensor = btlCarKf_LastLoad.regnsensor,
          farge = btlCarKf_LastLoad.farge,
          cruisekontroll = btlCarKf_LastLoad.cruisekontroll,
          parkeringsensor = btlCarKf_LastLoad.parkeringsensor,
          antall_eiere = btlCarKf_LastLoad.antall_eiere,
          kommune = btlCarKf_LastLoad.kommune,
          fylke = btlCarKf_LastLoad.fylke,
          xenon = btlCarKf_LastLoad.xenon,
          navigasjon = btlCarKf_LastLoad.navigasjon,
          servicehefte = btlCarKf_LastLoad.servicehefte,
          sportsseter = btlCarKf_LastLoad.sportsseter,
          tilstandsrapport = btlCarKf_LastLoad.tilstandsrapport,
          vekt = btlCarKf_LastLoad.vekt
        )
      }

      //check first car
      val btlCarTest1 = btlCar(0) //based on the url list input
      btlCarTest1.sold_date should equal ("2016-07-16")
      btlCarTest1.hengerfeste should equal (true)
      btlCarTest1.sylindervolum should equal (2.3)
      btlCarTest1.vekt should equal (1440)
      btlCarTest1.effekt should equal (135)
      btlCarTest1.load_date_first should equal ("2016-07-15")
      btlCarTest1.price_first should equal (23538)
      btlCarTest1.price_last should equal (16538)
      btlCarTest1.antall_eiere should equal (3)
      btlCarTest1.skinninterior should equal ("Skinnseter")
    }

     it("can parse and subset json car properties into scala map") {
      val jsonPropertiesMap = "{\"Salgsform\":\"Bruktbil til salgs\",\"Girkasse\":\"Automat\",\"Antall seter\":\"5\"}"
      val parsedPropertiesMap = Utility.getMapFromJsonMap(jsonPropertiesMap, Seq("Salgsform")) //exclude keys and remove json structure
      parsedPropertiesMap.size should equal(2)
      parsedPropertiesMap should contain key("Antall seter")
      parsedPropertiesMap should contain value("5")
      parsedPropertiesMap should contain key("Girkasse")
      parsedPropertiesMap should contain value("Automat")
    }

    ignore("can parse and subset json car equipment to scala list") {
      val jsonEquipmentArray = "[\"Aluminiumsfelger\",\"Automatisk klimaanlegg\",\"Skinnseter\"]"
      val parsedEquipmentList = Utility.getSetFromJsonArray(jsonEquipmentArray, Seq("Aluminiumsfelger"))
      parsedEquipmentList.size should equal(2)
      parsedEquipmentList should contain ("Automatisk klimaanlegg")
      parsedEquipmentList should contain ("Skinnseter")
    }

    ignore("can merge AcqCarHeader object with AcqCarDetails object") {
      val propCar:PropCar = dao.createPropCar(randomAcqCarHeader)
      propCar.information should equal ("Fin bil. NEDSATT PRIS")
    }

    ignore("can return a sequence of dates between two dates, used for querying C*") {
      val startDate = LocalDate.of(2016,12,31)
      val endDate = LocalDate.of(2017,1,4)
      val dates:Seq[String] = Utility.getDatesBetween(startDate, endDate)
      dates.length should equal(5)
    }

    ignore("can get the date when a car was first loaded into Acq-layer") {
      val propCarFirstRecords = Utility.getFirstRecordFromFilteredPropCarRDD(testPropCarPairRDD)
      val propCarRecord = Utility.popTopPropCarRecord(propCarFirstRecords,"http://m.finn.no/car/used/ad.html?finnkode=79021972")
      propCarRecord.load_date should equal ("2016-07-15")
      propCarRecord.url should equal ("http://m.finn.no/car/used/ad.html?finnkode=79021972")
    }

    ignore("can get the last known price when car is marked as solgt") {
      val rddDeltaLoadAcqHeaderDatePartition = sc.cassandraTable[AcqCarHeader]("finncars", "acq_car_header")//.where("load_date = ?", "2016-07-15")

      val rddDeltaLoadAcqHeaderLastLoadTimePerDay = sc.union(rddDeltaLoadAcqHeaderDatePartition).
        map(row => ((row.load_date, row.url), (AcqCarHeader(title = row.title, url = row.url, location = row.location, year = row.year, km = row.km, price = row.price, load_time = row.load_time, load_date = row.load_date)))).reduceByKey((x, y) => if (y.load_time > x.load_time) y else x)

      rddDeltaLoadAcqHeaderLastLoadTimePerDay.cache
      rddDeltaLoadAcqHeaderLastLoadTimePerDay.first
      val rddDeltaLoadAcqDetailsDatePartition = sc.cassandraTable[AcqCarDetails]("finncars", "acq_car_details")//.where("load_date = ?", "2016-07-15")

      val rddDeltaLoadAcqDetailsLastLoadTimePerDay = sc.union(rddDeltaLoadAcqDetailsDatePartition).map(row =>
        ((row.load_date, row.url), (AcqCarDetails(url = row.url, load_date = row.load_date, load_time = row.load_time, properties = row.properties, equipment = row.equipment, information = row.information, deleted = row.deleted)))).
        reduceByKey((x, y) => if (y.load_time > x.load_time) y else x)

      rddDeltaLoadAcqDetailsLastLoadTimePerDay.cache
      rddDeltaLoadAcqDetailsLastLoadTimePerDay.first
      val propCarDeltaRDD = rddDeltaLoadAcqHeaderLastLoadTimePerDay.join(rddDeltaLoadAcqDetailsLastLoadTimePerDay).map { row =>
        (row._1._2, PropCar(load_date = row._1._1, url = row._1._2,price = getLastPrice(row._2._1.price, row._2._1.url, row._2._1.load_date, row._2._1.load_time) ))
      }
//      val propCarDeltaRDD = rddDeltaLoadAcqHeaderLastLoadTimePerDay.join(rddDeltaLoadAcqDetailsLastLoadTimePerDay).map { row =>
//        (row._1._2, PropCar(load_date = row._1._1, url = row._1._2,price = dao.getLastPriceAsync(row._2._1.price, row._2._1.url, row._2._1.load_date, row._2._1.load_time) ))
//      }


      propCarDeltaRDD.take(1000)

    }




    def getLastPrice(price:String, url:String, load_date:String, load_time:Long):Int = {
      if (price != "Solgt") {
        val parsedPrice = price.replace(",-","").replace(" ","").replace("\"", "")
        if (parsedPrice.forall(_.isDigit)) parsedPrice.toInt else -1 //price invalid
      } else {
        //      println("get last price before acq car header lookup")
        println(url + ";ATTEMPT")
        val prevAcqCarHeaderNotSold = sc.cassandraTable[AcqCarHeader]("finncars", "acq_car_header").
          where("url = ?", url).
          where("load_time <= ?", new java.util.Date(load_time)).
          filter(row => row.price != "Solgt").
          collect
        println(url + ";SUCCESS")
        -2
      }
    }



    ignore("can get the date when a car was last loaded into Acq-layer") {
      val propCarLastRecords = Utility.getLastPropCarAll(testPropCarPairRDD)
      val propCarRecord = Utility.popTopPropCarRecord(propCarLastRecords,"http://m.finn.no/car/used/ad.html?finnkode=79021972")
      propCarRecord.load_date should equal ("2016-07-18")
      propCarRecord.url should equal ("http://m.finn.no/car/used/ad.html?finnkode=79021972")
    }
    ignore("can identify automatgir") {
      var propCarRecord = Utility.popTopPropCarRecord(testPropCarPairRDD,"http://m.finn.no/car/used/ad.html?finnkode=79021972")
      var automatgir = Utility.hasAutomatgir(propCarRecord.properties)
      automatgir should equal(true)

      propCarRecord = Utility.popTopPropCarRecord(testPropCarPairRDD,"http://m.finn.no/car/used/ad.html?finnkode=79052280")
      automatgir = Utility.hasAutomatgir(propCarRecord.properties)
      automatgir should equal(false)
    }
    ignore("can calculate number of days between two date strings") {
      val startDate = "2006-12-31"
      val endDate = "2007-01-04"
      val numberOfDays = Utility.getDaysBetweenStringDates(startDate, endDate)
      numberOfDays should equal(4)
    }


  }


  describe("JSON to Cassandra") {
    //subject of the test

    ignore("can convert JSON hdr file to list of AcqCarHeaders") {
      val sourceJson = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinn.json")
      val jsonCarHdr: JsValue = Json.parse(sourceJson.mkString)
      val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
      val acqCarHeaderList = Range(0, numOfCars).map(i =>
        Utility.createAcqCarHeaderObject(i, jsonCarHdr)).toList
      acqCarHeaderList.length should equal (numOfCars)
    }

    ignore("can convert JSON hdr file to list of AcqCarDetails") {
      val sourceJson = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinnLimited.json")
      val jsonCarHdr: JsValue = Json.parse(sourceJson.mkString)
      val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
      val acqCarDetailList = Range(0, numOfCars).map(i =>
        Utility.createAcqCarDetailsObject(i, jsonCarHdr)).toList
      acqCarDetailList.length should equal (numOfCars)
    }



  }





}
