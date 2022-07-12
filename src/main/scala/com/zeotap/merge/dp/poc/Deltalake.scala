package com.zeotap.merge.dp.poc

import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable


object Deltalake {

  type BlobPath = String
  type Format = String

  val basePath = "/Users/joydeep/IdeaProjects/data-partner-merge/src/main/resources/delta/onaudience/dpm/base"
  //createDeltaTable(spark,"/Users/joydeep/Downloads/dpm/onaudience","avro")
  //createDeltaTable(spark, "/Users/joydeep/Downloads/ael/onaudience", "avro")
  val snapshot = "snapshot"
  val update = "updates"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val spark = SparkSession.builder()
      .master("local")
      .appName("DELTA LAKE POC")
      .config(conf)
      .getOrCreate()

    val deltaTable = DeltaTable.forPath(spark, basePath).as(snapshot)
    val updates = spark.read.format("avro").load("/Users/joydeep/Downloads/ael/onaudience")


    val explodedUpdates = updates.selectExpr("*", "explode_outer(cookies) as (id_type,id)")

    spark.sql(s"CREATE TABLE snapshot USING DELTA LOCATION '$basePath'")
    spark.sql("select * from default.snapshot").show()

    val id_type_condition = joinCondition(snapshot, update, "id_type")
    val id_condition = joinCondition(snapshot, update, "id")

    //val intersectionDF = deltaTable.toDF.select("id", "id_type").distinct().intersect(explodedUpdates.select("id", "id_type").distinct())
    //intersectionDF.show(false)
    //intersectionDF.count()

    val cdc = explodedUpdates.filter("id == '133900b3-44f1-43fe-873f-f4bde3dfd6af'")
      //.drop("Demographic_Gender")
      .withColumn("Demographic_Gender",
        when(col("id").equalTo("133900b3-44f1-43fe-873f-f4bde3dfd6af"), "FEMAL").otherwise(col("Demographic_Gender")))
      .withColumn("Demographic_MinAge",
        when(col("id").equalTo("133900b3-44f1-43fe-873f-f4bde3dfd6af"), lit(801).cast(IntegerType)).otherwise(col("Demographic_MinAge")))
      .withColumn("Demographic_MaxAge",
        when(col("id").equalTo("133900b3-44f1-43fe-873f-f4bde3dfd6af"), 1901).otherwise(col("Demographic_MaxAge")))
    //val columns = List("Demographic_MinAge", "Demographic_MaxAge", "Demographic_Gender")
    val columns = List(("Demographic_MinAge", IntegerType), ("Demographic_MaxAge", IntegerType), ("Demographic_Gender", StringType))


    val dmb: DeltaMergeBuilder = deltaTable
      .as(snapshot)
      .merge(cdc.as(update), s"$id_condition and $id_type_condition")


//    val deltaMergeBuilder = builderWithSimpleMatch(dmb)
//    deltaMergeBuilder.execute()

//    val colBuilder: DeltaMergeBuilder = builderWithForMatch(update, columns, dmb)
//    colBuilder.execute()

    //val updateSetWithConditions: mutable.Map[String, Column] = computeNVLMap(snapshot, update, columns)
    val updateSetWithConditions = getNVLMap2(update, columns)

    val updateBuilder = dmb.whenMatched()
      .update(updateSetWithConditions)
    //          /*.whenNotMatched()
    //          .insertAll()*/
    //
    updateBuilder.execute()

    println("here")
    //
    //        val builder= naiveBuilderWithAND(snapshot, update, deltaTable, id_type_condition, id_condition, cdc)
    //        builder.execute()
    //    deltaTable.toDF.show()

  }

  private def builderWithForMatch(update: Format, columns: List[String], dmb: DeltaMergeBuilder) = {
    columns
      .foldLeft(dmb)((d: DeltaMergeBuilder, name: String) => {
        d.whenMatched(s"$update.$name is not null")
          .update(Map(s"$name" -> col(s"$update.$name")))
      })
  }

  private def builderWithSimpleMatch(dmb: DeltaMergeBuilder) = dmb
    .whenMatched("updates.Demographic_MinAge is not null").updateExpr(
    Map("Demographic_MinAge" -> "updates.Demographic_MinAge"))
    .whenMatched("updates.Demographic_MaxAge is not null").updateExpr(
    Map("Demographic_MaxAge" -> "updates.Demographic_MaxAge"))
    .whenMatched("updates.Demographic_Gender is not null").updateExpr(
    Map("Demographic_Gender" -> "updates.Demographic_Gender"))
    .whenNotMatched()
    .insertExpr(Map(
      "Demographic_Gender" -> "updates.Demographic_Gender",
      "Demographic_MaxAge" -> "updates.Demographic_MaxAge",
      "Demographic_MinAge" -> "updates.Demographic_MinAge"))


  private def naiveBuilderWithAND(snapshot: Format, update: Format, deltaTable: DeltaTable, id_type_condition: String, id_condition: String, cdc: DataFrame) = deltaTable
      .as(snapshot)
      .merge(cdc.as(update), s"$id_condition and $id_type_condition")
      .whenMatched("updates.Demographic_MinAge is not null and updates.Demographic_MaxAge is not null and updates.Demographic_Gender is not null")
      .updateExpr(
        Map(
          "Demographic_MinAge" -> "updates.Demographic_MinAge",
          "Demographic_MaxAge" -> "updates.Demographic_MaxAge",
          "Demographic_Gender" -> "updates.Demographic_Gender"
        ))


  private def getNVLMap2(update: Format, columns: List[(String, DataType)]) = columns
      .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => {
        d += (s"${name._1}" ->
          when(col(s"${update}.${name._1}") === lit(null).cast(name._2), col(s"${snapshot}.${name._1}"))
            .otherwise(col(s"${update}.${name._1}"))
          )
      })


  private def computeNVLMap(snapshot: Format, update: String, columns: List[String]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: String) => {
      d += (s"${name}" -> when(col(s"${update}.${name}") === null, col(s"${snapshot}.${name}"))
        .otherwise(col(s"${update}.${name}")))
    })

  def joinCondition(snapshot: String, updates: String, column: String): String = s"$snapshot.$column = $updates.$column"

  def createDeltaTable(sparkSession: SparkSession, path: String, format: Format): BlobPath = {
    format match {
      case "avro" =>
        val df = sparkSession.read.format("avro").load(path).write.format("delta")
        df.save("/Users/joydeep/IdeaProjects/data-partner-merge/src/main/resources/delta/onaudience/dpm/base")
    }
    path
  }
}
