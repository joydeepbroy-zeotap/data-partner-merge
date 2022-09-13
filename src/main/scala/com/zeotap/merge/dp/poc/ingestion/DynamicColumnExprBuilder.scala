package com.zeotap.merge.dp.poc.ingestion

import org.apache.spark.sql.Column

class DynamicColumnExprBuilder extends DeltaExpressionBuilder[ProfileColumn] {
  override def insertColumn(a: ProfileColumn): (String, Column) = (s"snapshot.${a.name}", a.name match {
    case "brands" => BrandsColumn.insert(a)
    case "Interest_IAB" => InterestIABColumn.insert(a)
  })

  override def updateColumn(a: ProfileColumn): (String, Column) = insertColumn(a)
}
