package org.example.spark.POSmetricProcessing.hbaseUtils

import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get, HTable, Put, Scan}
import org.apache.hadoop.hbase.util._
import java.util.Properties



import collection.JavaConverters._
import scala.collection.mutable

object hbaseOps {

  var  hbaseConfiguration:Configuration = null
  var conn:Connection = null
  var props:Properties = null
  var admin:Admin = null
  private var  cfList: List[String] = Nil
  private var tblCFmapping: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()



  def loadcolMapping() = {

    if (tblCFmapping.isEmpty) {
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("/tblColFamily.cfg")).getLines().foreach(line => tblCFmapping += (line.split("=")(0) -> line.split("=")(1)))
    }
  }

  def getConfig = {
      if(hbaseConfiguration == null)
        {
          val hdpConf = new Configuration()
          hbaseConfiguration = HBaseConfiguration.create(new Configuration())
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
        loadcolMapping()

        cfList = tblCFmapping.get(tblNm) match {
          case Some(x) => x.split(",").toList
          case None => Nil
        }
      }

        cfList.foreach(println)
        val tblDesc = new HTableDescriptor(tblNm)
        cfList.foreach(cf=>tblDesc.addFamily(new HColumnDescriptor(cf)))
        admin.createTable(tblDesc)
        println("Table Created!!")

    true
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

  def fetchRecs(tblNm:String="HBASE_OFFSET_TBL",hbaseOffset:List[(String,String)]= Nil) = {

    loadcolMapping()

    cfList = tblCFmapping.get(tblNm) match {
      case Some(x) => x.split(",").toList
      case None => Nil
    }

    cfList.foreach(elem => {
      val get:Get = new Get(Bytes.toBytes("invoice"))
      get.addFamily(Bytes.toBytes("invoice_0".split("_")(0)))
    })



  }



}
