package org.example.spark.POSmetricProcessing



import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.example.spark.POSmetricProcessing.AppUtil.logPrefix
import org.example.spark.POSmetricProcessing.Process.{ProcessBilling, ProcessInvoice, ProcessOffset}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise}


object MetricProcessMain {


  def main(args: Array[String]): Unit = {

    val logger: Logger = LogManager.getLogger(this.getClass)
    logger.info(s"${logPrefix(this.getClass.getName)} - Metric Process Application Started" )

    var promiseException: Promise[Exception] = Promise[Exception]

    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()
    spark.streams.addListener(new ProcessOffset())


    val seqFuture = Seq(new Thread(new ProcessInvoice(spark,promiseException)),new Thread(new ProcessBilling(spark,promiseException)))
    seqFuture.foreach(_.start())
    promiseException.future.onFailure{case e:Exception => logger.info(s"${logPrefix(this.getClass.getName)}" + e.getStackTrace)}

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = /* Cleanup jobs here If any*/
        logger.info(s"${logPrefix(this.getClass.getName)} - Metric Process Application Stopped" )
    }))

    }

}
