package io.everydata.tpch

import java.util

import io.prestosql.tpch.{TpchEntity, TpchTable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}
import org.slf4j.LoggerFactory

class TpchTableRDD
(@transient val sqlContext: SQLContext, allOptions: Map[String, String], schema: StructType) extends RDD[Row](sqlContext.sparkContext, deps = Nil) {

  private val logger = LoggerFactory.getLogger(classOf[TpchTableRDD])


  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    // create reader
    val tableName = allOptions.get(TpchProperties.TABLE_NAME);

    val totalPartitions = allOptions.getOrElse(TpchProperties.PARTITIONS, 1).asInstanceOf[String].toInt
    val scaleFactor = allOptions.getOrElse(TpchProperties.SCALE_FACTOR, 1.0).asInstanceOf[String].toDouble

    logger.info("Initializing Table Reader for Table {} with scale Factor {} for partition {} out of {} partitions", tableName.get, String.valueOf(scaleFactor), String.valueOf(split.index), String.valueOf(totalPartitions))
    val table = TpchTable.getTable(tableName.get)
    val tpchItr: util.Iterator[_ <: TpchEntity] = table.createGenerator(scaleFactor, split.index + 1, totalPartitions).iterator
    new SparkTpchRowIterator(tpchItr, schema);
  }

  override protected def getPartitions: Array[Partition] = {
    // create here partitions
    // in above method create iterator from partitions
    val totalPartitions = allOptions.getOrElse(TpchProperties.PARTITIONS, 1).asInstanceOf[String].toInt

    Range(0, totalPartitions).map(part => {
      new TpchTablePartition(part, allOptions)
    }).toArray
  }
}
