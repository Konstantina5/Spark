package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Task3 {
  def task3BruteForce(data: RDD[List[Double]], top: Int) = {
    //TODO check if size < top
    Task1.task1BruteForce(data)
      .collect()
      .map(point => Tuple2(point, Task2.countDominatedPoints2(point, data.collect())))
      .sortBy(-_._2)
      .take(top)
  }

  def task3(data: RDD[List[Double]], top: Int) = {
    //TODO check if size < top
    Task1.sfs(data)
      .map(point => Tuple2(point, Task2.countDominatedPoints2(point, data.collect())))
      .sortBy(-_._2)
      .take(top)
  }

  def task32(data: RDD[List[Double]], top: Int, sc: SparkContext) = {
    //TODO check if size < top
    sc.parallelize(Task1.sfs(data)).cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()
      .map(point => Tuple2(point._1, Task2.countDominatedPoints3(point._1, point._2)))
      .sortBy(-_._2)
      .take(top)
  }
}
