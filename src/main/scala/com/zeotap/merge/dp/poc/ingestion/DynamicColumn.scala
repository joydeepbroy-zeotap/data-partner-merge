package com.zeotap.merge.dp.poc.ingestion

import org.apache.spark.sql.Column

trait DynamicColumn extends Expression[ProfileColumn, Column]
