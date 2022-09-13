package com.zeotap.merge.dp.poc.ingestion

import org.apache.spark.sql.Column

trait DeltaExpressionBuilder[A] {
  def insertColumn(a: A): (String, Column)

  def updateColumn(a: A): (String, Column)
}
