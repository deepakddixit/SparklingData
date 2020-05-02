package io.everydata.tpcds

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class TpcdsDataSource extends RelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new TpcdsTableRelation(sqlContext, parameters)
  }
}
