package org.example.spark.POSmetricProcessing


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.example.spark.POSmetricProcessing.POJO.EntityMapper._
import org.example.spark.POSmetricProcessing.Serde.JSONSerde
import org.apache.spark.sql.functions._


object MetricProcessMain {

  def writeOffset(tblNm:String = "HBASE_OFFSET_TBL",part_offset_list:List[(String,String)]) ={

    if(hbaseUtils.hbaseOps.getConfig)
    {
      if(hbaseUtils.hbaseOps.getOrCreateTbl(tblNm))
        hbaseUtils.hbaseOps.insUpdRec(tblNm,part_offset_list)
    }
    else
    {
      println("Table Does Not Exist!!")
    }



  }

  def readOffset(tblNm:String) = {

    if(hbaseUtils.hbaseOps.getConfig)
    {
      if(hbaseUtils.hbaseOps.getOrCreateTbl(tblNm))
        hbaseUtils.hbaseOps.fetchRecs(tblNm)
    }
    else
    {
      println("Table Does Not Exist!!")
    }

  }


  def main(args: Array[String]): Unit = {

    implicit val invoiceSerde: JSONSerde[Invoice] = new JSONSerde[Invoice]
    implicit val consumerSerde = new JSONSerde[SinkConsumerDetail]
    implicit val merchantSerde = new JSONSerde[SinkMerchantDetail]
    implicit val billingSerde = new JSONSerde[SinkBillingDetail]
    implicit val productPurchasedSerde = new JSONSerde[SinkPurchasedProduct]

    var props=scala.collection.mutable.Map[String,String]()
    val spark = SparkSession.builder().master("local").config(new SparkConf()).getOrCreate()

      scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SparkKafkaConfig.cfg")).getLines().foreach(line=>props.+=(line.split("=")(0)->line.split("=")(1)))

    import spark.implicits._

    val sDf=spark.readStream.format("kafka").options(props).option("startingOffsets", "earliest").load().selectExpr("CAST(value AS STRING)").as[String]
    //sDf.printSchema()
    //val writeDF=sDf.selectExpr("CAST(CAST(key as STRING) AS INT)","CAST(value as STRING)").as[(Int,String)]
    //sDf.writeStream.format("console").start().awaitTermination()
    sDf.writeStream.foreach(new hbaseUtils.invoiceWriter("abc")).start().awaitTermination()
    /*if(hbaseUtils.hbaseOps.getConfig)
      {
        if(hbaseUtils.hbaseOps.getOrCreateTbl("t2"))
          hbaseUtils.hbaseOps.insUpdRec("t2")
      }
    else
      {
        println("Table Does Not Exist!!")
      }
*/




  }



}
