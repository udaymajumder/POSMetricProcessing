package org.example.spark.POSmetricProcessing.Process

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.example.spark.POSmetricProcessing.AppUtil.logPrefix
import org.example.spark.POSmetricProcessing.POJO.EntityMapper.Invoice
import org.example.spark.POSmetricProcessing.hbaseUtils
import org.example.spark.POSmetricProcessing.hbaseUtils.hbaseOps.{chkTbl, hbaseConfiguration, parseOffset}

import scala.concurrent.Promise
import scala.util.Try

class ProcessInvoice(spark:SparkSession,promise:Promise[Exception]) extends Runnable{

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.info(s"${logPrefix(this.getClass.getName)} - Process Invoice Application Started" )

  override def run(): Unit = {
    val invoiceOffsetParam: Map[String, String] = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SparkKafkaConfig.cfg")).getLines().filter(line=>line.startsWith("invoice")).map(line=>line.split("#")(1)).map(line=>(line.split("=")(0),line.split("=")(1))).toMap
    val invoice_subscribe_props = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SparkKafkaConfig.cfg")).getLines().filter(line=>(line.startsWith("init#") || line.startsWith("invoice#"))).map(line=>line.split("#")(1)).map(line=>(line.split("=")(0),line.split("=")(1))).toMap

    val sDf=spark.readStream.format("kafka").options(invoice_subscribe_props).option("startingOffsets", parseOffset(invoiceOffsetParam)).load()
      .selectExpr("CAST(topic AS STRING)","CAST(value AS STRING)","CAST(partition AS INT)","CAST(offset AS LONG)")

    sDf.writeStream.foreach(new ForeachWriter[Row] {

      private var tbl: HTable = null

      override def open(partitionId: Long, version: Long): Boolean = {
        Try( if(chkTbl("INVOICE_TBL")) tbl = new HTable(hbaseConfiguration, "INVOICE_TBL")).isSuccess
      }

      override def process(value: Row): Unit = {

        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        val dSerValue = mapper.readValue[Invoice](value.getAs("value").toString)
        val put: Put = new Put(Bytes.toBytes(dSerValue.invoiceNum.toString))
        val enrPut = hbaseUtils.hbaseEntityUtils.splitAndFetchInvoiceDtl(dSerValue, put)
        tbl.put(enrPut)

      }

      override def close(errorOrNull: Throwable): Unit = {
        tbl.close()
      }

    }).start().awaitTermination()

    promise.failure(new Exception)
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = logger.info(s"${logPrefix(this.getClass.getName)} - Process Invoice Application ShutDown" )
  }))
}
