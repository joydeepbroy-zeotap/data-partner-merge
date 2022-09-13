package com.zeotap.merge.dp.poc.util

import com.zeotap.merge.dp.poc.Deltalake.SomePair
import org.apache.spark.sql.functions.udf

import java.time.LocalDateTime
import scala.collection.mutable

object SparkUDFOps {
  val commaSeparatedStringStandardiser = udf(reconcileMapColumnForString _)
  val arrayOfStringStandardiser = udf(reconcileMapColumnForArray _)

  def reconcileMapColumnForArray(master: mutable.Map[String, SomePair], update: Array[String]): mutable.Map[String, SomePair] =
    reconcileColumn(master,
      update.map(a => (a, (1l, LocalDateTime.now().toString))).toMap)(reconcileMapColumn)

  def reconcileMapColumnForString(master: mutable.Map[String, SomePair], update: String): mutable.Map[String, SomePair] =
    reconcileColumn(master,
      update.split(",").map(a => (a, (1l, LocalDateTime.now().toString))).toMap)(reconcileMapColumn)

  private def reconcileColumn(master: mutable.Map[String, SomePair], update: Map[String, (Long, String)])
                             (f: (mutable.Map[String, SomePair], Map[String, (Long, String)]) => mutable.Map[String, SomePair]): mutable.Map[String, SomePair] = if (update.isEmpty || update.equals(null))
    master
  else if (master.equals(null))
    collection.mutable.Map(update.toSeq: _*)
  else f(master, update)

  def reconcileMapColumn(master: mutable.Map[String, SomePair], update: Map[String, (Long, String)]): mutable.Map[String, SomePair] = if (update.isEmpty)
    master
  else {
    val mapColumnElement = update.head
    val name = mapColumnElement._1
    val weight = mapColumnElement._2._1
    val timestamp = mapColumnElement._2._2
    if (!master.contains(name))
      master.update(name, SomePair(weight, timestamp))
    else {
      val tuple = master(name)
      master.update(name, SomePair(tuple.weight + weight, timestamp))
    }
    reconcileMapColumn(master, update.tail)
  }
}
