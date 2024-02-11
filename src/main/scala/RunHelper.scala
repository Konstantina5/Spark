package org.example

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object RunHelper {
  def runTask(taskNum:Int, text: String, function: () => Array[(List[Double], Long)], taskNumber: Int, algorithm: String, distribution: String): Array[(List[Double], Long)] = {
    val start = System.currentTimeMillis()
    val answer = function()
    val end = System.currentTimeMillis()

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + algorithm + "Results.txt")

    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }

  def runTask(taskNum: Int, text: String, function: () => RDD[List[Double]], taskNumber: Int, algorithm: String, distribution: String): RDD[List[Double]] = {
    val start = System.currentTimeMillis()
    val answer = function()
    val end = System.currentTimeMillis()

    FileHelper.writeToFile(text, taskNumber, end - start, answer.collect().toList, "task" + taskNumber + algorithm + "Results.txt")

    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }

  def runTask(taskNum: Int, text: String, function: () => Iterator[List[Double]], taskNumber: Int, algorithm: String, distribution: String): List[List[Double]] = {
    val start = System.currentTimeMillis()
    val calculate = function()
    val end = System.currentTimeMillis()

    val answer = calculate.toList

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + algorithm + "Results.txt")

    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }

  def runTask(taskNum: Int, text: String, function: () => ArrayBuffer[List[Double]], taskNumber: Int, algorithm: String, distribution: String): ArrayBuffer[List[Double]] = {
    val start = System.currentTimeMillis()
    val answer = function()
    val end = System.currentTimeMillis()

    FileHelper.writeToFile(text, taskNumber, end - start, answer.toList, "task" + taskNumber + algorithm + "Results.txt")

    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }
}
