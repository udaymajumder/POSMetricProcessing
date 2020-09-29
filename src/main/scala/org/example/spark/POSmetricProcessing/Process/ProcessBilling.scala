package org.example.spark.POSmetricProcessing.Process

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{col, window}
import org.example.spark.POSmetricProcessing.AppUtil.logPrefix
import org.example.spark.POSmetricProcessing.POJO.EntityMapper.HbaseMRCHCatgBilling
import org.example.spark.POSmetricProcessing.hbaseUtils
import org.example.spark.POSmetricProcessing.hbaseUtils.hbaseOps.{chkTbl, hbaseConfiguration, parseOffset}
import org.example.spark.POSmetricProcessing.sparkUdfUtils.MerchantBillingAggregator
import org.example.spark.POSmetricProcessing.sparkUdfUtils.SparkUdfObject.parseBill

import scala.concurrent.Promise
import scala.util.Try

class ProcessBilling(spark:SparkSession,promise:Promise[Exception]) extends Runnable {

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.info(s"${logPrefix(this.getClass.getName)} - Process Billing Application Started" )

  override def run(): Unit = {
    val billingOffsetParam: Map[String, String] = scala.io.Source.fromInputStream((this.asInstanceOf[Any]).getClass.getResourceAsStream("/SparkKafkaConfig.cfg")).getLines().filter(line=>line.startsWith("billing")).map(line=>line.split("#")(1)).map(line=>(line.split("=")(0),line.split("=")(1))).toMap
    val billing_subscribe_props = scala.io.Source.fromInputStream((this.asInstanceOf[Any]).getClass.getResourceAsStream("/SparkKafkaConfig.cfg")).getLines().filter(line=>(line.startsWith("init#") || line.startsWith("billing#"))).map(line=>line.split("#")(1)).map(line=>(line.split("=")(0),line.split("=")(1))).toMap

    val catAgg = new MerchantBillingAggregator()

    val billingDf=spark.readStream.format("kafka").options(billing_subscribe_props).option("startingOffsets", parseOffset(billingOffsetParam)).load()
      .select(col("topic").cast("string"),parseBill(col("value").cast("string")).as("value"),col("partition").cast("int"),col("offset").cast("long"),col("timestamp"))

    val billingDfAggr = billingDf.withWatermark("timestamp", "20 seconds").groupBy(window(col("timestamp"),"20 seconds","20 seconds").as("time"),
      col("value").getField("MRCH_CAT_CD").as("MRCH_CAT_CD"))
      .agg(catAgg(col("value").getField("BILL_AMT")).as("MRCH_CATG_CD_Cumulative_Billing"))
      .select(col("time").getField("start").cast("string").as("WindowStartTime"),
        col("time").getField("end").cast("string").as("WindowEndTime"),
        col("MRCH_CAT_CD"),
        col("MRCH_CATG_CD_Cumulative_Billing")
      )

    billingDfAggr.writeStream.outputMode("update").foreach(new ForeachWriter[Row] {

      private var tbl: HTable = null

      override def open(partitionId: Long, version: Long): Boolean = {
        Try( if(chkTbl("WINDOWED_MRCH_CATG_BILLING_DTL")) tbl = new HTable(hbaseConfiguration, "WINDOWED_MRCH_CATG_BILLING_DTL")).isSuccess
      }

      override def process(value: Row): Unit = {
        val dSerValue = HbaseMRCHCatgBilling(value.getAs("WindowStartTime"),value.getAs("WindowEndTime"),value.getAs("MRCH_CAT_CD"),value.getAs("MRCH_CATG_CD_Cumulative_Billing"))
        println(dSerValue)
        val put: Put = new Put(Bytes.toBytes(dSerValue.MRCH_CAT_CD.toString + "_" + dSerValue.WindowStartTime.toString + "_" + dSerValue.WindowEndTime.toString))
        val enrPut = hbaseUtils.hbaseEntityUtils.splitAndFetchBillingDtl(dSerValue, put)
        tbl.put(enrPut)
      }

      override def close(errorOrNull: Throwable): Unit = {
        tbl.close()
      }
    }).start().awaitTermination()

    promise.failure(new Exception)
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = logger.info(s"${logPrefix(this.getClass.getName)} - Process Billing Application ShutDown" )
  }))
}
