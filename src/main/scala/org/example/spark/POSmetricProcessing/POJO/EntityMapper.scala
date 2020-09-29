package org.example.spark.POSmetricProcessing.POJO

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

object EntityMapper {

  /*case class Consumer(@JsonProperty("CON_ID") CON_ID:Int,
                      @JsonProperty("NAME") NAME:String,
                      @JsonProperty("GENDER") GENDER:String,
                      @JsonProperty("PHONE") PHONE:String,
                      @JsonProperty("PRM_IND") PRM_IND:Boolean)

  case class ConsumerAddress(@JsonProperty("CON_ID") CON_ID:Int,
                             @JsonProperty("ADDR_LINE") ADDR_LINE:String,
                             @JsonProperty("PIN") PIN:Int,
                             @JsonProperty("STATE") STATE:String)*/

  case class ConsumerInvoiceDetail(@JsonProperty("CON_ID") CON_ID:Int,
                                   @JsonProperty("NAME") NAME:String,
                                   @JsonProperty("GENDER") GENDER:String,
                                   @JsonProperty("PHONE") PHONE:String,
                                   @JsonProperty("ADDR_LINE") ADDR_LINE:String,
                                   @JsonProperty("PIN") PIN:Int,
                                   @JsonProperty("STATE") STATE:String,
                                   @JsonProperty("PRM_IND") PRM_IND:Boolean)

  case class Product(@JsonProperty("PRD_CD") PRD_CD:Int,
                     @JsonProperty("PRD_NM") PRD_NM:String,
                     @JsonProperty("PRD_CAT_CD") PRD_CAT_CD:Int,
                     @JsonProperty("PRICE") PRICE:String)

  case class Product_Cart(@JsonProperty("PRD_PURCHASE_DTL") product:Product,
                          @JsonProperty("QTY") QTY:Int=0)

  case class Product_Purchased(@JsonProperty("PRD_LIST") PRD_LIST:List[Product_Cart],
                               @JsonProperty("BILL_AMT") BILL_AMT:Double)

  case class Merchant(@JsonProperty("MRCH_CD") MRCH_CD:Int,
                      @JsonProperty("MRCH_NM") MRCH_NM:String,
                      @JsonProperty("MRCH_CAT_CD") MRCH_CAT_CD:Int)

  case class Location(@JsonProperty("LOC_ID") LOC_ID:Int,
                      @JsonProperty("LOC_NM") LOC_NM:String,
                      @JsonProperty("LOC_PIN") LOC_PIN:Int,
                      @JsonProperty("LOC_STATE") LOC_STATE:String,
                      @JsonProperty("LOC_CTRY") LOC_CTRY:String)

  case class Invoice(@JsonProperty("Invoice_Number") invoiceNum:Int,
                     @JsonProperty("Consumer_Detail") consumerDetails:ConsumerInvoiceDetail,
                     @JsonProperty("Merchant_Detail") merchant:Merchant,
                     @JsonProperty("Location_Detail") location:Location,
                     @JsonProperty("Billing_Detail") productPurchased:Product_Purchased)

  case class SinkConsumerDetail(@JsonProperty("Invoice_Number") invoiceNum:Int,
                                @JsonProperty("Consumer_Detail") consumerDetails:ConsumerInvoiceDetail)

  case class SinkMerchantDetail(@JsonProperty("Invoice_Number") invoiceNum:Int,
                                @JsonProperty("Merchant_Detail") merchant:Merchant,
                                @JsonProperty("Location_Detail") location:Location)

  case class SinkBillingDetail( @JsonProperty("Invoice_Number") invoiceNum:Int,
                                @JsonProperty("MRCH_CD") MRCH_CD:Int,
                                @JsonProperty("MRCH_CAT_CD") MRCH_CAT_CD:Int,
                                @JsonProperty("MRCH_NM") MRCH_NM:String,
                                @JsonProperty("BILL_AMT") BILL_AMT:Double)

  case class SinkPurchasedProduct( @JsonProperty("Invoice_Number") invoiceNum:Int,
                                   @JsonProperty("PRD_LIST") PRD_LIST:List[Product_Cart])

  /* HBase Specific classes */
  case class HbaseMRCHCatgBilling(WindowStartTime:String,WindowEndTime:String,MRCH_CAT_CD:Int,BILL_AMT:Double)

  /*Kafka Specific Classes*/
  case class offsetMarker(topic:String,partition:Int,offset:Long)

}
