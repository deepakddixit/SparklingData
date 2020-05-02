package io.everydata.tpcds

import io.everydata.tpcds.exception.TpcdsRunTimeException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory

class TpcdsTableRelation
(@transient val sqlContext: SQLContext, allOptions: Map[String, String]) extends BaseRelation with Serializable with TableScan {

  private val logger = LoggerFactory.getLogger(classOf[TpcdsTableRelation])

  override def schema: StructType = {
    // read table option and create schema
    if (!allOptions.contains(TpcdsProperties.TABLE_NAME)) {
      logger.error("No table name configured. Throwing exception")
      val msg = String.format("No table name configured. Specify property [%s]", TpcdsProperties.TABLE_NAME)
      throw new TpcdsRunTimeException(msg)
    }
    val tableName = allOptions.get(TpcdsProperties.TABLE_NAME);

    logger.trace("In the readSchema")

    val structType = Utils.readSchema(tableName.get.toLowerCase())
    logger.debug("Tpcds Table Schema: " + structType)
    return structType
  }

  override def buildScan(): RDD[Row] = {
    new TpcdsTableRDD(sqlContext, allOptions, schema)
  }
}
