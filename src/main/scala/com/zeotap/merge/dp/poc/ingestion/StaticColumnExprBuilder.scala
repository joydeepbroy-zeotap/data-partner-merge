package com.zeotap.merge.dp.poc.ingestion

import org.apache.spark.sql.Column

class StaticColumnExprBuilder extends DeltaExpressionBuilder[ProfileColumn] {
  override def insertColumn(a: ProfileColumn): (String, Column) = (s"snapshot.${a.name}", StaticColumn.insert(a))

  override def updateColumn(a: ProfileColumn): (String, Column) = (s"snapshot.${a.name}", StaticColumn.update(a))
}
