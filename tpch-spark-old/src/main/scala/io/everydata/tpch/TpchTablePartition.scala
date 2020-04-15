package io.everydata.tpch

import org.apache.spark.Partition

case class TpchTablePartition(index: Int, allOptions: Map[String, String]) extends Partition with Serializable {
}
