package org.example.spark.POSmetricProcessing.Serde

import java.util

import org.apache.kafka.common.serialization.Serializer
import org.apache.log4j.{LogManager, Logger}
import org.example.spark.POSmetricProcessing.AppUtil._
import scala.reflect.runtime.universe._

class JSONSerializer[T : TypeTag] extends Serializer[T]
{

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.info(s"${logPrefix(this.getClass.getName)} - Started" )

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = super.configure(configs, isKey)

  override def serialize(topic: String, data: T): Array[Byte] = {
    if(data==null)
      null
    else
      SerdeUtils.ByteArray.encode(data)
  }

  override def close(): Unit = super.close()


}
