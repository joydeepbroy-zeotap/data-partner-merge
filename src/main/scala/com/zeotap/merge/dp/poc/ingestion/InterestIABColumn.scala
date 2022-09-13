package com.zeotap.merge.dp.poc.ingestion

import com.zeotap.merge.dp.poc.util.SparkUDFOps.commaSeparatedStringStandardiser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

case object InterestIABColumn extends DynamicColumn {
  override def insert(a: ProfileColumn): Column = when(col(s"update.${a.name}").isNotNull,
    commaSeparatedStringStandardiser(
      col(s"snapshot.${a.name}"), col(s"update.${a.name}"))
  ).otherwise(col(s"snapshot.${a.name}"))

  override def update(a: ProfileColumn): Column = insert(a)
}
