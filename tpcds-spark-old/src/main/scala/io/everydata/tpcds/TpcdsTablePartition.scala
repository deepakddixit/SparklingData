package io.everydata.tpcds

import org.apache.spark.Partition

case class TpcdsTablePartition(index: Int, allOptions: Map[String, String]) extends Partition with Serializable {
}
