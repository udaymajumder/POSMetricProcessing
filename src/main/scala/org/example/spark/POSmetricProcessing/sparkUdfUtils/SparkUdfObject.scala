package org.example.spark.POSmetricProcessing.sparkUdfUtils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.example.spark.POSmetricProcessing.POJO.EntityMapper.SinkBillingDetail

object SparkUdfObject {


  val billingSchema =  StructType(Seq(StructField("invoiceNum",IntegerType,true),StructField("MRCH_CD",IntegerType,true),
    StructField("MRCH_CAT_CD",IntegerType,true),StructField("MRCH_NM",StringType,true),StructField("BILL_AMT",DoubleType,true)))



  val parseBill = udf((billing:String) => {

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val dSerValue = mapper.readValue[SinkBillingDetail](billing)
    dSerValue},billingSchema)

}
