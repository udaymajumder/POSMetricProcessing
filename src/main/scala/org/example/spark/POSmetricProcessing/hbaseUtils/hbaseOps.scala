package org.example.spark.POSmetricProcessing.hbaseUtils

import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get, HTable, Put, Scan}
import org.apache.hadoop.hbase.util._
import java.util.Properties

import org.example.spark.POSmetricProcessing.hbaseUtils

import collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object hbaseOps {

  var  hbaseConfiguration:Configuration = null
  var conn:Connection = null
  var props:Properties = null
  var admin:Admin = null
  private var  cfList: List[String] = Nil
  private var tblCFmapping: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()



  def loadcolMapping() = {
    if (tblCFmapping.isEmpty) {
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("/tblColFamily.cfg")).getLines().map(line => line.split(":")(1)).foreach(line => {
        println(line);
        tblCFmapping += (line.split("=")(0) -> line.split("=")(1))
      })
    }
  }

  def getConfig = {
      if(hbaseConfiguration == null)
        {
          val hdpConf = new Configuration()
          hbaseConfiguration = HBaseConfiguration.create(new Configuration())
          //hbaseConfiguration.addResource("file://quickstart.cloudera/etc/hbase/conf/hbase-site.xml")
          val iStream: InputStream = getClass.getResourceAsStream("/HbaseConfig.cfg")
          scala.io.Source.fromInputStream(iStream).getLines().foreach(line=>hbaseConfiguration.set(line.split("=")(0),line.split("=")(1)))
          conn = ConnectionFactory.createConnection(hbaseConfiguration)
          admin=conn.getAdmin
          print("Configuration Connection Created!!")
        }
    true
  }

  def getOrCreateTbl(tblNm:String) = {

    val tbl = TableName.valueOf(tblNm)

    if(!admin.tableExists(tbl))
      {
        hbaseOps.loadcolMapping()
        cfList = tblCFmapping.get(tblNm) match {
          case Some(x) => x.split(",").toList
          case None => Nil
        }
        val tblDesc = new HTableDescriptor(tblNm)
        cfList.foreach(cf=>tblDesc.addFamily(new HColumnDescriptor(cf)))
        admin.createTable(tblDesc)
      }

    true
  }

  def chkTbl(tblNm:String) = {

    Try(if (hbaseUtils.hbaseOps.getConfig)
          if (hbaseUtils.hbaseOps.getOrCreateTbl(tblNm))
            {
              println(s"CHECK FOR TABLE ${tblNm} SUCCESSFUL")
            }).isSuccess

  }

  def insUpdRec(tblNm:String,hbaseOffset:List[(String,String)]= Nil) = {

    val tbl = new HTable(hbaseConfiguration,tblNm)

    val listOfPut = scala.collection.mutable.MutableList[Put]()

    if(tblNm.matches("HBASE_OFFSET_TBL"))
      {

        hbaseOffset.foreach(elem => {
          val put:Put = new Put(Bytes.toBytes(elem._1.split("_")(0)))
          put.addColumn(Bytes.toBytes(elem._1.split("_")(0)),Bytes.toBytes(elem._1.split("_")(1)),Bytes.toBytes(elem._2))
          listOfPut.+=(put)
        })

        tbl.put(listOfPut.asJava)
      }

  }

  def fetchOffsets(offsetParam:Map[String,String]) = {

    val tblNm:String="POS_HBASE_OFFSET_TBL"
    var tbl: HTable = null
    val cf: String = offsetParam.get("subscribe") match
    { case Some(x) => x}
    val parts: Int = offsetParam.get("partitions") match
    {
      case Some(x) => x.toInt - 1
    }


    if(chkTbl(tblNm)) tbl = new HTable(hbaseConfiguration, tblNm) else println("Error")

    val tblFetchOffset: List[(Int, String)] = (0 to parts).map(part=> {
      val offset: String = Bytes.toString(tbl.get(new Get(Bytes.toBytes(cf + "_" + part.toString))).getValue(Bytes.toBytes(cf),Bytes.toBytes(part.toString)))
      (part, if(offset == null) (-2).toString else offset)
    }).toList


    (cf,tblFetchOffset)

  }

  def parseOffset(offsetParam:Map[String,String]) = {

    val (cf:String,tblFetchOffset:List[(Int,String)]) = fetchOffsets(offsetParam)
    val tmpOffsetStr = "{\"" + cf + "\":{" + tblFetchOffset.map(x=> "\"" + x._1 + "\":" + (x._2.toLong + 1).toString  + ",").mkString + "}}"

    val offsetStr = tmpOffsetStr.patch(tmpOffsetStr.lastIndexOf(","),"",1)
    println(offsetStr)
    offsetStr
  }



}
