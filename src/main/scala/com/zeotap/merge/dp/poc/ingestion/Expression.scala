package com.zeotap.merge.dp.poc.ingestion

trait Expression[A, B] {

  def insert(a: A): B

  def update(a: A): B
}
