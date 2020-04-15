package io.everydata.tpch

import io.everydata.tpch.exception.TpchRunTimeException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory

class TpchTableRelation
(@transient val sqlContext: SQLContext, allOptions: Map[String, String]) extends BaseRelation with Serializable with TableScan {

  private val logger = LoggerFactory.getLogger(classOf[TpchTableRelation])

  override def schema: StructType = {
    // read table option and create schema
    if (!allOptions.contains(TpchProperties.TABLE_NAME)) {
      logger.error("No table name configured. Throwing exception")
      val msg = String.format("No table name configured. Specify property [%s]", TpchProperties.TABLE_NAME)
      throw new TpchRunTimeException(msg)
    }
    val tableName = allOptions.get(TpchProperties.TABLE_NAME);

    logger.trace("In the readSchema")

    val structType = Utils.readSchema(tableName.get.toLowerCase())
    logger.debug("Tpch Table Schema: " + structType)
    return structType
  }

  override def buildScan(): RDD[Row] = {
    new TpchTableRDD(sqlContext, allOptions, schema)
  }
}
