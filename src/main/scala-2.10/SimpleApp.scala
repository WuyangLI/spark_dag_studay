/**
  * Created by lwy on 17/02/17.
  */
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark._

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleApp")
    conf.set("es.index.auto.create", "true")
    //conf.
    val ssc = new StreamingContext(conf, Seconds(60))
    val rddRaw = ssc.textFileStream("user/wli/SimpleApp/input")
      .repartition(10)
      .map(x => x.toLowerCase())

    val rddIntermediate = rddRaw.map(x => parseLine(x))
      .filter(x => x._2.length() > 0)
      .flatMap(x => splitPair(x))
      .cache()

    rddIntermediate.map(x => x._1+","+x._2).saveAsTextFiles("user/wli/SimpleApp/output/")

    rddIntermediate.foreachRDD(x => saveRddToEs(x))

    ssc.start()
    ssc.awaitTermination()
  }

  def parseLine(line: String):(String,String) ={
    val kv = line.split(" ")
    if(kv.length == 2) (kv(0), kv(1))
    else (kv(0), "")
  }

  def splitPair(kv:(String, String)):Array[(String, String)] = kv._2.split(",").map( x => (kv._1, x))

  def saveRddToEs(rdd: RDD[(String, String)]) = {
    val dates = rdd.map(x=>x._1).distinct().collect()
    for(date <- dates){
      val esConf = Map("es.nodes" -> "nceorihad56.nce.amadeus.net",
        "es.port" -> "9200",
        "es.resource" -> ("simpleapp-"+date+"/test"),
        //"es.input.json"->"true",
        "es.index.auto.create"->"true",
        "es.http.timeout"-> "5m",
        "es.http.retries"-> "10",
        "es.action.heart.beat.lead"->"50",
        "es.batch.write.refresh"-> "false",
        "es.batch.write.retry.count"->"10",
        "es.batch.write.retry.wait"->"500",
        "es.nodes.wan.only"-> "true"
      )
      rdd.filter(x => x._1 == date).map(x => Map("date" -> x._1, "word" -> x._2)).saveToEs(esConf)
    }
  }
}

