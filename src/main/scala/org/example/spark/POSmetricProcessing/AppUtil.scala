package org.example.spark.POSmetricProcessing

import java.text.SimpleDateFormat
import java.util.Date

object AppUtil {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val dateInfo = new Date()

  def logPrefix(classNm:String): String = s"${sdf.format(dateInfo.getTime)} : ${classNm}"


}
