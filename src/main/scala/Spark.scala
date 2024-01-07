package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Spark {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First App")
    println("hello")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext( conf)

    val input = sc.textFile("data3.txt")

    val parsedData = input
      .map(s => s.split(" ")
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .toList)

    // ----------- Task 1 ----------

    runTask(() => Task1.task1BruteForce(parsedData), "Task 1")
    runTask(() => sc.parallelize(Task1.sfs(parsedData)), "Task 1")

    runTask2(() => Task2.task2BruteForce(parsedData, 3), "Task 2")
    runTask2(() => Task2.STD(parsedData, 3, sc), "Task 2")
    runTask2_grid(() => Task2.Top_k_GridDominance(parsedData, 3, sc), "Task 2-Grid")

    runTask3(() => Task3.task3(parsedData, 3), "task 3")
//    runTask2(() => Task3.task33(parsedData, 3, sc), "task 3")
    runTask2(() => Task3.task32(parsedData, 3, sc), "task 3")
  }

  def runTask3(function: () => ArrayBuffer[(List[Double], Long)], taskNumber: String): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply()

    val end = System.currentTimeMillis()

    println("-- " +  taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)
    answer.foreach(arr => println(arr))
  }

  def runTask2(function: () => Array[Tuple2[List[Double], Long]], taskNumber: String): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply()

    val end = System.currentTimeMillis()

    println("-- " +  taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)
    answer.foreach(arr => println(arr))
  }

  def runTask2_grid(function: () => List[Tuple2[List[Double], Int]], taskNumber: String): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply()

    val end = System.currentTimeMillis()

    println("-- " +  taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
//    println("Total skyline points = " + answer.length)
    answer.foreach(arr => println(arr))
  }

  def runTask(function: () => RDD[List[Double]], taskNumber: String): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply()

    val end = System.currentTimeMillis()

    println("-- " +  taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.count())
    answer.collect.foreach(arr => println(arr))
  }
}
