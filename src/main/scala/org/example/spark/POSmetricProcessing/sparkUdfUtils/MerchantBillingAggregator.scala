package org.example.spark.POSmetricProcessing.sparkUdfUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DataType, DoubleType,StructField, StructType}

class MerchantBillingAggregator extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(
    StructField("BILL_AMT",DoubleType,true):: Nil)

  override def bufferSchema: StructType = StructType(
      StructField("TOTAL_BILLED_AMOUNT", DoubleType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) =  buffer.getAs[Double](0) + input.getAs[Double](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
}

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Double](0)
  }
}
