package io.everydata.tpch

import java.util

import io.prestosql.tpch.TpchEntity
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

class SparkTpchRowIterator(rowGenerator: util.Iterator[_ <: TpchEntity], schema: StructType) extends Iterator[Row] {
  private val logger = LoggerFactory.getLogger(classOf[SparkTpchRowIterator])

  override def hasNext: Boolean = {
    rowGenerator.hasNext
  }

  override def next(): Row = {
    val next = rowGenerator.next
    val values = next.values

    var valueArray: Array[Any] = new Array[Any](values.size)
    for (i <- 0 until values.size) {
      var temp = values.apply(i)
      valueArray.update(i, convertRead(schema.apply(i).dataType, temp));
    }
    new GenericRowWithSchema(valueArray, schema)
  }

  def convertRead(dt: DataType, value: Any): Any = {
    if (value == null) {
      return null
    }
    dt match {
      case e: StringType => {
        UTF8String.fromString(String.valueOf(value))
      }
      case e: LongType => {
        value.asInstanceOf[Long]
      }
      case e: IntegerType => {
        value.asInstanceOf[Int]
      }
      case e: BooleanType => {
        value.asInstanceOf[Boolean]
      }
      case e: DoubleType => {
        value.asInstanceOf[Double]
      }
      case e: FloatType => {
        value.asInstanceOf[Float]
      }
      case e: DateType => {
        value.asInstanceOf[java.sql.Date]
      }
      case _: Any => {
        logger.info("In Any case")
        value
      }
    }
  }
}
