package io.everydata.tpcds

import io.prestosql.tpcds.{Table, TableGenerator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


class TpcdsTableRDD
(@transient val sqlContext: SQLContext, allOptions: Map[String, String], schema: StructType) extends RDD[Row](sqlContext.sparkContext, deps = Nil) {

  private val logger = LoggerFactory.getLogger(classOf[TpcdsTableRDD])


  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    // create reader
    val tableName = allOptions.get(TpcdsProperties.TABLE_NAME);

    val totalPartitions = allOptions.getOrElse(TpcdsProperties.PARTITIONS, 1).asInstanceOf[String].toInt
    val scaleFactor = allOptions.getOrElse(TpcdsProperties.SCALE_FACTOR, 1.0).asInstanceOf[String].toDouble

    logger.info("Initializing Table Reader for Table {} with scale Factor {} for partition {} out of {} partitions", tableName.get, String.valueOf(scaleFactor), String.valueOf(split.index), String.valueOf(totalPartitions))

    var options: io.prestosql.tpcds.Options = Utils.asTpcdsOption(mapAsJavaMap(allOptions).asInstanceOf[java.util.Map[String, String]])

    val table = Table.getTable(tableName.get)

    val session = options.toSession.withChunkNumber(split.index + 1)
    val tableGenerator = new TableGenerator(session)
    val sparkResultIterable = tableGenerator.generateTableAsItr(table)

    new SparkTpcdsRowIterator(sparkResultIterable, schema);
  }

  override protected def getPartitions: Array[Partition] = {
    // create here partitions
    // in above method create iterator from partitions
    val totalPartitions = allOptions.getOrElse(TpcdsProperties.PARTITIONS, 1).asInstanceOf[String].toInt

    Range(0, totalPartitions).map(part => {
      new TpcdsTablePartition(part, allOptions)
    }).toArray
  }
}
