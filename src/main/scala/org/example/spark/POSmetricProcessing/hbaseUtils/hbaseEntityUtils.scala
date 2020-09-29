package org.example.spark.POSmetricProcessing.hbaseUtils

import java.lang.reflect.Field
import org.apache.hadoop.hbase.client.Put
import org.example.spark.POSmetricProcessing.POJO.EntityMapper._
import org.apache.hadoop.hbase.util._

object hbaseEntityUtils {

  def extractCustmer(c:ConsumerInvoiceDetail,field:String) = {

    field match {
      case "CON_ID" => c.CON_ID
      case "NAME" => c.NAME
      case "GENDER" => c.GENDER
      case "PHONE" => c.PHONE
      case "ADDR_LINE" => c.ADDR_LINE
      case "PIN" => c.PIN
      case "STATE" => c.STATE
      case "PRM_IND" => c.PRM_IND

    }

  }

  def extractMerchant(c:Merchant,field:String) = {

    field match {
      case "MRCH_CD" => c.MRCH_CD
      case "MRCH_NM"  => c.MRCH_NM
      case "MRCH_CAT_CD" => c.MRCH_CAT_CD

    }

  }

  def extractLocation(c:Location,field:String) = {

    field match {
      case "LOC_ID" => c.LOC_ID
      case "LOC_NM" => c.LOC_NM
      case "LOC_PIN" => c.LOC_PIN
      case "LOC_STATE" => c.LOC_STATE
      case "LOC_CTRY" => c.LOC_CTRY

    }

  }

  def extractProduct_Purchased(c:Product_Purchased,field:String) = {

    field match {
      case "Product_Purchased" => if((c.PRD_LIST.isEmpty)) null else c.PRD_LIST.mkString
      case "BILL_AMT" => c.BILL_AMT

    }

  }

  def extractMrchCatgBilling(c:HbaseMRCHCatgBilling,field:String) = {

    field match {
      case "WindowStartTime" => c.WindowStartTime
      case "WindowEndTime"  => c.WindowEndTime
      case "MRCH_CAT_CD" => c.MRCH_CAT_CD.toString
      case "BILL_AMT" => c.BILL_AMT.toString

    }

  }


  def splitAndFetchInvoiceDtl(dSerValue:Invoice, put:Put) = {




    val enrPut: Put =put
    val customerDTL: ConsumerInvoiceDetail = dSerValue.consumerDetails
    val locationDTL: Location = dSerValue.location
    val merchantDTL: Merchant = dSerValue.merchant
    val ppDTL: Product_Purchased = dSerValue.productPurchased

    val customerDTLAttrList: List[Field] = customerDTL.getClass.getDeclaredFields.toList
    val locationDTLAttrList = locationDTL.getClass.getDeclaredFields.toList
    val merchantDTLAttrList = merchantDTL.getClass.getDeclaredFields.toList
    val ppDTLAttrList = Product_Purchased.getClass.getDeclaredFields.toList


    customerDTLAttrList.map(x=>x.toString.split('$')(1).split('.')(1))
      .foreach(x=> {
        if(extractCustmer(customerDTL,x) == null) println("Skipping Value")
        else {
          enrPut.addColumn(Bytes.toBytes("ConsumerDetail"),Bytes.toBytes(x),Bytes.toBytes(extractCustmer(customerDTL,x).toString))
        }
      })

    locationDTLAttrList.map(x=>x.toString.split('$')(1).split('.')(1))
      .foreach(x=> {
        if(extractLocation(locationDTL,x) == null) println("Skipping Value")
        else {
          enrPut.addColumn(Bytes.toBytes("LocationDetail"),Bytes.toBytes(x),Bytes.toBytes(extractLocation(locationDTL,x).toString))
        }
      })

    merchantDTLAttrList.map(x=>x.toString.split('$')(1).split('.')(1))
      .foreach(x=> {
        if(extractMerchant(merchantDTL,x) == null) println("Skipping Value")
        else enrPut.addColumn(Bytes.toBytes("MerchantDetail"),Bytes.toBytes(x),Bytes.toBytes(extractMerchant(merchantDTL,x).toString))
      })

    ppDTLAttrList.map(x=>x.toString.split(' ')(4).split('$')(1))
      .foreach(x=> {
        if(extractProduct_Purchased(ppDTL,x) == null) println("Skipping Value")
        else enrPut.addColumn(Bytes.toBytes("productPurchasedDetail"),Bytes.toBytes(x),Bytes.toBytes(extractProduct_Purchased(ppDTL,x).toString))
      })

    enrPut
  }

  def splitAndFetchBillingDtl(dSerValue:HbaseMRCHCatgBilling, put:Put) = {

    val enrPut: Put =put
    val billingDtl: HbaseMRCHCatgBilling = dSerValue

    val billingDTLAttrList: List[Field] = billingDtl.getClass.getDeclaredFields.toList

    billingDTLAttrList.map(x=>x.toString.split('$')(1).split('.')(1))
      .foreach(x=> {
        if(extractMrchCatgBilling(billingDtl,x) == null) println("Skipping Value")
        else {
          enrPut.addColumn(Bytes.toBytes("BillDetail"),Bytes.toBytes(x),Bytes.toBytes(extractMrchCatgBilling(billingDtl,x)))
        }
      })
    enrPut
  }

}
