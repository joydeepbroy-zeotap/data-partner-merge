package com.zeotap.merge.dp.poc.ingestion

import com.zeotap.merge.dp.poc.util.SparkUDFOps.arrayOfStringStandardiser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

case object BrandsColumn extends DynamicColumn {
  override def insert(a: ProfileColumn): Column = when(col(s"update.${a.name}").isNotNull,
    arrayOfStringStandardiser(col(s"snapshot.${a.name}"), col(s"update.${a.name}"))
  ).otherwise(col(s"snapshot.${a.name}"))

  override def update(a: ProfileColumn): Column = insert(a)
}
