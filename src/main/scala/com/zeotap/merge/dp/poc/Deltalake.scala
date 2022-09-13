package com.zeotap.merge.dp.poc

import com.zeotap.merge.dp.poc.util.SparkUDFOps.commaSeparatedStringStandardiser
import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable


object Deltalake {

  type BlobPath = String
  type Format = String

  val basePath = "/Users/joydeep/IdeaProjects/data-partner-merge/src/main/resources/delta/onaudience/dpm/base"

  //createDeltaTable(spark, "/Users/joydeep/Downloads/ael/onaudience", "avro")
  val snapshot = "snapshot"
  val update = "updates"

  val snapshotRow = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 25, 35, "Female")
  val allFilledUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 45, 75, "Male")
  val allNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", null, null, null)
  val genderNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 15, 55, null)
  val minAgeNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", null, 65, "Female")
  val maxAgeNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 20, null, "Male")
  val ageNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", null, null, "Female")
  val MaxAgeAndGenderNullUpdate = Row("id_mid_10", "133900b3-44f1-43fe-873f-f4bde3dfd6af", 23, null, null)
  val columns = List(("Demographic_MinAge", IntegerType), ("Demographic_MaxAge", IntegerType),
    ("Demographic_Gender", StringType), ("Interest_IAB", StringType), ("Device_DeviceOS", StringType)
  )

  def createDeltaTableFromSingleRow(path: String, r: Row)(implicit spark: SparkSession) = {
    createDeltaTableFromDataframe(createDataframeFromSingleRow(r), path)
    path
  }


  def createDataframeFromSingleRow(r: Row)(implicit spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("id_type", StringType, true),
      StructField("id", StringType, true),
      StructField("Demographic_MinAge", IntegerType, true),
      StructField("Demographic_MaxAge", IntegerType, true),
      StructField("Demographic_Gender", StringType, true),
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(r)), schema)
  }

  val id_type_condition = joinCondition(snapshot, update, "id_type")
  val id_condition = joinCondition(snapshot, update, "id")

  def generatedDataframeUseCase(path: String)(implicit spark: SparkSession) = {
    val deltaTable = DeltaTable.forPath(spark, createDeltaTableFromSingleRow(path, snapshotRow)).as(snapshot)
    println(">>>>>>>>>>STARTED AT>>>>>>>>>>>>>>>>")
    deltaTable.toDF.show(false)
    println(">>>>>>>>>>STARTED AT>>>>>>>>>>>>>>>>")
    val rows = List(
      allFilledUpdate,
      allNullUpdate,
      genderNullUpdate,
      minAgeNullUpdate,
      maxAgeNullUpdate,
      ageNullUpdate,
      MaxAgeAndGenderNullUpdate
    )
    val useCases: List[DataFrame] = rows.map(x => createDataframeFromSingleRow(x))

    useCases.foreach(cdc => {
      val mergeBuilder = deltaTable
        .as(snapshot)
        .merge(cdc.as(update), s"$id_condition and $id_type_condition")

      val updateSetWithConditions = getNVLMapUsingCoalesceAndWhenOnUpdateHasNullWithoutCast(update, columns)

      mergeBuilder.whenMatched()
        .update(updateSetWithConditions)
        .execute()

      println(">>>>>>>>>CDC DATA VIEW>>>>>>>>>>>>>")
      cdc.show(false)

      println(">>>>>>>>>>>>ACTUAL RESULT VIEW>>>>>>>>>>>>>")
      deltaTable.toDF.show(false)
      println(">>>>>>>>>>>>ACTUAL RESULT VIEW>>>>>>>>>>>>>")
    })


  }

  case class SomePair(weight: Long, Common_TS: String)



  def createDeltaTableFromDataframe(df: DataFrame, path: String) = df.write.format("delta").save(path)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    implicit val spark = SparkSession.builder()
      .master("local")
      .appName("DELTA LAKE POC")
      .config(conf)
      .getOrCreate()
    //spark.sparkContext.setLogLevel("ALL")

    spark.udf.register("mapColumnStandardizer", commaSeparatedStringStandardiser)

    createDeltaTable(spark,"/Users/joydeep/Downloads/dpm/onaudience","avro")

    //generatedDataframeUseCase("/Users/joydeep/IdeaProjects/data-partner-merge/src/main/resources/delta/testWhenExp/")

    val deltaTable = DeltaTable.forPath(spark, basePath).as(snapshot)
    val updates = spark.read.format("avro").load("/Users/joydeep/Downloads/ael/onaudience")


    val explodedUpdates = updates.selectExpr("*", "explode_outer(cookies) as (id_type,id)")

    spark.sql(s"CREATE TABLE snapshot USING DELTA LOCATION '$basePath'")
    spark.sql("select * from default.snapshot").show()



    //val intersectionDF = deltaTable.toDF.select("id", "id_type").distinct().intersect(explodedUpdates.select("id", "id_type").distinct())
    //intersectionDF.show(false)
    //intersectionDF.count()

//    val cdc = explodedUpdates.filter("id == '133900b3-44f1-43fe-873f-f4bde3dfd6af'")
//      //.drop("Demographic_Gender")
//      .withColumn("Demographic_Gender",
//        when(col("id").equalTo("133900b3-44f1-43fe-873f-f4bde3dfd6af"), null).otherwise(col("Demographic_Gender")))
//      .withColumn("Demographic_MinAge",
//        when(col("id").equalTo("133900b3-44f1-43fe-873f-f4bde3dfd6af"), lit(null).cast(IntegerType)).otherwise(col("Demographic_MinAge")))
//      .withColumn("Demographic_MaxAge",
//        when(col("id").equalTo("133900b3-44f1-43fe-873f-f4bde3dfd6af"), 78).otherwise(col("Demographic_MaxAge")))
//    //val columns = List("Demographic_MinAge", "Demographic_MaxAge", "Demographic_Gender")
//    //val columns = List(("Demographic_MinAge", IntegerType), ("Demographic_MaxAge", IntegerType), ("Demographic_Gender", StringType))


    val dmb: DeltaMergeBuilder = deltaTable
      .as(snapshot)
      .merge(explodedUpdates.as(update), s"$id_condition and $id_type_condition")


    //    val deltaMergeBuilder = builderWithSimpleMatch(dmb)
    //    deltaMergeBuilder.execute()

    //    val colBuilder: DeltaMergeBuilder = builderWithForMatch(update, columns, dmb)
    //    colBuilder.execute()

    //val updateSetWithConditions: mutable.Map[String, Column] = getNVLMapUsingWhen(snapshot, update, columns)
    val updateSetWithConditions = getNVLMapUsingCoalesceAndWhenOnUpdateHasNullWithoutCast(update, columns)
    val allConditions = getNVLMapForDynamicColumns(updateSetWithConditions)(columns)

    val updateBuilder = dmb.whenMatched()
      .update(allConditions)
    .whenNotMatched()
    .insertAll()

    updateBuilder.execute()

    println("debugger stops here!")

  }


  private def getNVLMapForDynamicColumns(conditions: scala.collection.mutable.Map[String, Column])(columns: List[(String, DataType)]) = columns
    .foldLeft(conditions)((d: mutable.Map[String, Column], name: (String, DataType)) => name._1 match {
      case "brands" | "Interest_IAB" => d += (s"${snapshot}.${name._1}" -> when(col(s"${update}.${name._1}").isNotNull,
        //col(s"${update}.${name._1}"))
        commaSeparatedStringStandardiser(
          col(s"${snapshot}.${name._1}"), col(s"${update}.${name._1}"))
      )
        .otherwise(col(s"${snapshot}.${name._1}")))
      case _ => conditions
    })

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

  private def getNVLMapUsingCoalesceAndWhen(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => name._1 match {
      case "Demographic_MinAge" => d += (s"${name._1}" -> when(col(s"${snapshot}.${name._1}").equalTo(lit(25)), col(s"${update}.${name._1}"))
        .otherwise(col(s"${snapshot}.${name._1}")))
      case _ => addCoalesceToMap(update, d, name)
    })

  private def getNVLMapUsingCoalesceAndWhenOnUpdate(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => name._1 match {
      case "Demographic_MinAge" => d += (s"${name._1}" -> when(col(s"${update}.${name._1}").equalTo(lit(45)), col(s"${update}.${name._1}"))
        .otherwise(col(s"${snapshot}.${name._1}")))
      case _ => addCoalesceToMap(update, d, name)
    })

  private def addCoalesceToMap(update: Format, d: mutable.Map[String, Column], name: (String, DataType)) = d +=
    (s"${name._1}" -> coalesce(col(s"${update}.${name._1}"), col(s"${snapshot}.${name._1}")))


  private def getNVLMapUsingCoalesceAndWhenOnUpdateHasNull(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => name._1 match {
      case "Demographic_MinAge" => d += (s"${name._1}" -> when(col(s"${update}.${name._1}").equalTo(lit(null).cast(name._2)), col(s"${update}.${name._1}"))
        .otherwise(col(s"${snapshot}.${name._1}")))
      case _ => addCoalesceToMap(update, d, name)
    })

  private def getNVLMapUsingCoalesceAndWhenOnUpdateHasNullWithoutCast(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => name._1 match {
      case "Demographic_MinAge" => d += (s"${name._1}" -> when(col(s"${update}.${name._1}").isNull, col(s"${update}.${name._1}"))
        .otherwise(col(s"${snapshot}.${name._1}")))
      case "Interest_IAB" => d
      case "Device_DeviceOS" => d
      case _ => addCoalesceToMap(update, d, name)
    })

  private def getNVLMapUsingCoalesce(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => {
      addCoalesceToMap(update, d, name)
    })

  private def getNVLMapUsingWhenAndCast(update: Format, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => {
      d += (s"${name._1}" ->
        when(col(s"${update}.${name._1}") === lit(null).cast(name._2),
          col(s"${snapshot}.${name._1}"))
          .otherwise(
            col(s"${update}.${name._1}")
          )
        )
    })


  private def getNVLMapUsingWhen(update: String, columns: List[(String, DataType)]) = columns
    .foldLeft(scala.collection.mutable.Map[String, Column]())((d: mutable.Map[String, Column], name: (String, DataType)) => {
      d += (s"${name._1}" ->
        when(col(s"${update}.${name._1}") === null,
          col(s"${snapshot}.${name._1}"))
          .otherwise(
            col(s"${update}.${name._1}")
          )
        )
    })

  def joinCondition(snapshot: String, updates: String, column: String): String = s"$snapshot.$column = $updates.$column"

  def createDeltaTable(sparkSession: SparkSession, path: String, format: Format): BlobPath = {
    format match {
      case "avro" =>
        val df = sparkSession.read.format("avro").load(path).write.partitionBy("id_type").format("delta")
        df.save(basePath)
    }
    path
  }


}
