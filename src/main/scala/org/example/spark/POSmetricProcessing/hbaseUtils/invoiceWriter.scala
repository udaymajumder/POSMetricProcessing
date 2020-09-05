package org.example.spark.POSmetricProcessing.hbaseUtils

import org.apache.hadoop.hbase.client.HTable
import org.apache.spark.sql.{ForeachWriter, Row}
import org.example.spark.POSmetricProcessing.POJO.EntityMapper.Invoice
import org.example.spark.POSmetricProcessing.hbaseUtils
import org.example.spark.POSmetricProcessing.hbaseUtils.hbaseOps._

class invoiceWriter(val tblNm:String) extends ForeachWriter[String] {

  val tbl:HTable = null

  override def open(partitionId: Long, version: Long): Boolean = {
    /*if(hbaseUtils.hbaseOps.getConfig)
      if(hbaseUtils.hbaseOps.getOrCreateTbl(tblNm))
        {
          val tbl = new HTable(hbaseConfiguration,tblNm)
        }

    else
    {
      println("Table Does Not Exist!!")
    }*/
    true
  }

  override def process(value: String): Unit = {


    println(value)

  }

  override def close(errorOrNull: Throwable): Unit = {}
}
