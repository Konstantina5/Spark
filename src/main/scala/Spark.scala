package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Spark {

  var cores: Int = 1
  var totalPointsGlobal: Long = 0
  var dimensionsGlobal: Int = 0
  def main(args: Array[String]): Unit = {
//    val coreList = List(2, 4, 8)
//
//    for(core <- coreList) {
//      cores = core
//      runForPerformance(core)
//    }
      runBestAlgorithm(args)
  }

  def runBestAlgorithm(args: Array[String]): Unit = {
    val cores = 8

    val conf = new SparkConf()
    conf.setMaster(s"local[$cores]")
    conf.setAppName("Skyline App")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    if (args.length == 0) {
      println("You need to provide an input file")
      System.exit(1)
    }

    val input = sc.textFile(args.apply(0))

    val parsedData = input
      .map(s => s.split(" ")
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .toList)

    val dimensions = parsedData.first().length
    val totalPoints = parsedData.count()

    FileHelper.createDir("results")
    val distribution = getDistribution(args.apply(0))

    // ----------- Task 1 ----------
    RunHelper.runTask(1, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task1.ALS(parsedData), 1, "ALS", distribution)

    // ----------- Task 2 ----------
    RunHelper.runTask(2, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task2.STD(parsedData, 3), 2, "STD", distribution)

    // ----------- Task 3 ----------
    RunHelper.runTask(3, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKBroadcastSkylines(parsedData, 3, sc), 3, "BroadcastSkylines", distribution)
  }

  private def runForPerformance(cores: Int): Unit = {
    val conf = new SparkConf()
    conf.setMaster(s"local[$cores]")
    conf.setAppName("Skyline App")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    val distributions = List("anticorrelated", "uniform", "correlated", "normal")
    val totalPoints = List(100, 1000, 10000, 100000, 500000, 1000000)

    for(d <- distributions) {
      for(p <- totalPoints) {
        totalPointsGlobal = p
        runForPerformance(d + "" + p, sc)
      }
    }
    sc.stop()
  }

  private def runForPerformance(file: String, sc: SparkContext): Unit = {
    val input = sc.textFile("input/dimensions4/" + file + ".txt")

    val parsedData = input
      .map(s => s.split(" ")
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .toList)

    val dimensions = parsedData.first().length
    dimensionsGlobal = dimensions
    val totalPoints = parsedData.count()

    FileHelper.createDir("results")
    val distribution = getDistribution(file)

    // ----------- Task 1 ----------
    runTask1(parsedData, dimensions, totalPoints, distribution)

    // ----------- Task 2 ----------
    runTask2(parsedData, dimensions, totalPoints, distribution, sc)

    // ----------- Task 3 ----------
    runTask3(parsedData, dimensions, totalPoints, distribution, sc)
  }

  private def runTask1(parsedData: RDD[List[Double]], dimensions: Int, totalPoints: Long, distribution: String): Unit = {
        RunHelper.runTask(1, dimensionsTotalPointsText(dimensions, totalPoints),
          () => Task1.skylinesBruteForce(parsedData), 1, "BruteForce", distribution)
        RunHelper.runTask(1, dimensionsTotalPointsText(dimensions, totalPoints),
          () => Task1.SFS(parsedData), 1, "SFS", distribution)
        RunHelper.runTask(1, dimensionsTotalPointsText(dimensions, totalPoints),
          () => Task1.SALSA(parsedData), 1, "SALSA", distribution)
        RunHelper.runTask(1, dimensionsTotalPointsText(dimensions, totalPoints),
          () => Task1.ALS(parsedData), 1, "ALS", distribution)
  }

  private def runTask2(parsedData: RDD[List[Double]], dimensions: Int, totalPoints: Long, distribution: String, sc: SparkContext): Unit = {
    RunHelper.runTask(2, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task2.task2BruteForce(parsedData, 3), 2, "BruteForce", distribution)
    RunHelper.runTask(2, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task2.topKDominating(parsedData, 3, new ArrayBuffer[(List[Double], Long)]()), 2, "STDWithoutExclusiveRegion", distribution)
    RunHelper.runTask(2, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task2.STD(parsedData, 3), 2, "STD", distribution)
    RunHelper.runTask(2, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task2.STDRecursive(parsedData, 3, sc), 2, "STDRecursive", distribution)
    RunHelper.runTask(2, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task2.topKGridDominance(parsedData, dimensions, 3, sc), 2, "Grid", distribution)
  }

  private def runTask3(parsedData: RDD[List[Double]], dimensions: Int, totalPoints: Long, distribution: String, sc: SparkContext): Unit = {
    RunHelper.runTask(3, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKSkylineBruteForce(parsedData, 3), 3, "BruteForce", distribution)
    RunHelper.runTask(3, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKCartesian(parsedData, 3, sc), 3, "Cartesian", distribution)
    RunHelper.runTask(3, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKBroadcastPoints(parsedData, 3, sc), 3, "BroadcastPoints", distribution)
    RunHelper.runTask(3, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKBroadcastSkylines(parsedData, 3, sc), 3, "BroadcastSkylines", distribution)
    RunHelper.runTask(3, dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKGridDominance(parsedData, dimensions, 3, sc), 3, "Grid", distribution)
  }

  private def dimensionsTotalPointsText(dim: Int, totalPoints: Long): String = {
    "\nPoint's dimensions = " + dim +
      "\nTotal Points = " + totalPoints + "\n"
  }

  private def getDistribution(fileName: String): String = {
    fileName match {
      case s if s.contains("uniform") => "Uniform"
      case s if s.contains("anticorrelated") => "Anticorrelated"
      case s if s.contains("correlated") => "Correlated"
      case s if s.contains("normal") => "Normal"
      case _ => ""
    }
  }
}
