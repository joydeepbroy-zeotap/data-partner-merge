package com.zeotap.merge.dp.poc.ingestion

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{coalesce, col}

case object StaticColumn extends Expression[ProfileColumn, Column] {
  override def insert(a: ProfileColumn): Column = coalesce(col(s"update.${a.name}"), col(s"snapshot.${a.name}"))

  override def update(a: ProfileColumn): Column = insert(a)
}
