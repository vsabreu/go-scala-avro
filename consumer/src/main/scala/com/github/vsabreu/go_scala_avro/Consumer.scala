package com.github.vsabreu.go_scala_avro

object Consumer {
  def main(args: Array[String]): Unit = {
    val msg = Message(id = 1, collected_at = 1515957924, user = "user", key = "key")
    println(msg)
  }
}
