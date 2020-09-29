package org.example.spark.POSmetricProcessing.Process

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.example.spark.POSmetricProcessing.hbaseUtils.hbaseOps.{chkTbl, hbaseConfiguration}
import org.example.spark.POSmetricProcessing.POJO.EntityMapper._

import scala.util.Try

class ProcessOffset extends StreamingQueryListener {

  private var tbl: HTable = null
  private var offsetmarker: offsetMarker = _

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    //println("Query started:" + queryStarted.id)
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    //println("Query terminated" + queryTerminated.id)
  }

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {

    val offSetList: List[SourceProgress] = queryProgress.progress.sources.toList

    offSetList.foreach(eachTopic => {
      val offDtl = eachTopic.endOffset.toString()
      val topic = offDtl.split('{')(1).split(':')(0).replace("\"", "")
      val offsetLst = offDtl.split('{')(2).split('}')(0).split(',').toList
      offsetLst.foreach(lst => {
        offsetmarker = offsetMarker(topic, lst.split(':')(0).replace("\"", "").toInt, lst.split(':')(1).toLong)
        if (offsetmarker == null || offsetmarker.offset == -1) {

          println("NO BATCH PROCESSED!")

        }
        else {

          Try(if (chkTbl("POS_HBASE_OFFSET_TBL")) {
            tbl = new HTable(hbaseConfiguration, "POS_HBASE_OFFSET_TBL")
          }
          )

          println(offsetmarker)
          val offsetPut = new Put(Bytes.toBytes(offsetmarker.topic.toString + "_" + offsetmarker.partition.toString))
          offsetPut.addColumn(Bytes.toBytes(offsetmarker.topic.toString), Bytes.toBytes(offsetmarker.partition.toString), Bytes.toBytes(offsetmarker.offset.toString))
          tbl.put(offsetPut)
        }

      })
    })

  }
}
