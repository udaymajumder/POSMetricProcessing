package org.example.spark.POSmetricProcessing.sparkUdfUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

class MerchantBillingAggregator extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(Seq(StructField("invoiceNum",IntegerType,true),StructField("MRCH_CD",IntegerType,true),
    StructField("MRCH_CAT_CD",IntegerType,true),StructField("MRCH_NM",StringType,true),StructField("BILL_AMT",DoubleType,true)))

  override def bufferSchema: StructType = StructType(
    StructField("MRCH_CAT_CD", IntegerType) ::
      StructField("TOTAL_BILLED_AMOUNT", DoubleType) :: Nil)

  override def dataType: DataType = StructType(
    StructField("MRCH_CAT_CD", IntegerType) ::
      StructField("TOTAL_BILLED_AMOUNT", DoubleType) :: Nil)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 1.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = input.getAs("MRCH_CAT_CD")
    buffer(1) = buffer.getAs[Double]("BILL_AMT") + input.getAs[Double]("BILL_AMT")
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer
  }
}
