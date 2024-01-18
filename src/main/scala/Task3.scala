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

  // Given a point (x,y), return the BlockID(in a 5x5 grid) that it belongs
  //    20 21 22 23 24
  //    15 16 17 18 19
  //    10 11 12 13 14
  //    5  6  7  8  9
  //    0  1  2  3  4
  def GetBlockID(point: List[Double]) ={
    val block_id = (BigDecimal(point(1)) / BigDecimal("0.2")).toInt * 5 + (BigDecimal(point(0)) / BigDecimal("0.2")).toInt
    block_id
  }

  // Calculate the minimum dominance of a point that belongs to a specific block
  // For example:
  // If a point belongs to Block 12 then the minimum dominance, that this point will have
  // equals to the number of points that belong to Blocks 18,19,23,24
  //    20 21 22 | 23 24
  //    15 16 17 | 18 19
  //          V    ------
  //    10 11 12 < 13 14
  //    5  6  7    8  9
  //    0  1  2    3  4
  def GetMinCount(point: List[Double], counts_per_block: List[Int]) ={
    var sum = 0
    val start_x = (BigDecimal(point(0)) / BigDecimal("0.2")).toInt + 1
    val start_y = (BigDecimal(point(1)) / BigDecimal("0.2")).toInt + 1
//    for (x <- start_x to 4) {
//      for (y <- start_y to 4) {
//        val block_id = y*5 + x
//        sum = sum + counts_per_block(block_id)
//      }
//    }
    val indices = (start_x to 4).flatMap(x => (start_y to 4).map(y => y * 5 + x))
    sum = sum + indices.map(counts_per_block).sum

    sum
  }

  // Get RDD of points that are dominated by point
  def IsDominatedByPoint(base_point: List[Double], target_point: List[Double]): Boolean = {
    if ( (base_point(0) <=  target_point(0)) && (base_point(1) <=  target_point(1)) ) {
      true
    } else {
      false
    }
  }

  // Compare a given point with the points of the given block_id
  def GetDominance_in_Block(point: List[Double], blocks_to_check: List[Int], points_with_block: RDD[(Int, Iterable[List[Double]])]) = {
    val points_dominated =
      points_with_block
        .filter(pair => blocks_to_check.contains(pair._1))
        .flatMap(_._2)
        .filter(p => !p.equals(point)) // exclude the point we are checking
        .filter(p => IsDominatedByPoint(point, p))
        .count()

    points_dominated.toInt
  }

  // Get the total dominance score of a given point
  def GetTotalCount(point: List[Double], counts_per_block: List[Int], points_with_block: RDD[(Int, Iterable[List[Double]])]) ={
    var sum = GetMinCount(point, counts_per_block)

    var blocks_to_check: List[Int] = List()
    val start_x = (BigDecimal(point(0)) / BigDecimal("0.2")).toInt
    val start_y = (BigDecimal(point(1)) / BigDecimal("0.2")).toInt
    // Get the Dominance score of the point within the block it belongs
    blocks_to_check = blocks_to_check :+ start_y * 5 + start_x

    // Check all the blocks to the right
    var range = (start_x + 1) to 4
    var blocks_to_add = range.map(x => start_y * 5 + x).toList
    blocks_to_check = blocks_to_check ++ blocks_to_add

    // Check all the blocks above
    range = (start_y + 1) to 4
    blocks_to_add = range.map(y => (y * 5 + start_x)).toList
    blocks_to_check = blocks_to_check ++ blocks_to_add

    sum = sum + GetDominance_in_Block(point, blocks_to_check, points_with_block)
    sum
  }

  def Top_k_GridDominance(data: RDD[List[Double]], top: Int, sc: SparkContext): List[(List[Double], Int)] = {

    // Create a 5X5 Grid
    //    20 21 22 23 24
    //    15 16 17 18 19
    //    10 11 12 13 14
    //    5  6  7  8  9
    //    0  1  2  3  4

    // Create an RDD of the data points along with the BLock ID RDD: (point,BlockID)
    val points_with_block =
      data
        .map(point => (point, GetBlockID(point)))
        .groupBy { case (_, blockID) => blockID }
        .mapValues(iter => iter.map { case (point, _) => point })

    // Count how many points belong to each block
    val block_count = data
      .map(point => (GetBlockID(point), 1))
      .reduceByKey(_ + _)
      .collect()
      .toList

    // Create a list holding the count of each block
    val block_counts: List[Int] = List.fill(25)(0)
    val counts_per_block: List[Int] = block_count.foldLeft(block_counts) {
      case (acc, (index, countToAdd)) =>
        acc.updated(index, acc(index) + countToAdd)
    }

    // Get Skyline points
    val skylines = sc.parallelize(Task1.sfs(data)).collect().toList //find skyline points
    // val skylines = Task1.task1BruteForce(data).collect().toList //find skyline points

    // Calculate the actual dominance score for each of the skylines
    // For example:
    // If a point belongs to Block 12 then we know that it surely dominates all the points that belong to blocks 18,19,23,24
    // and we have to CALCULATE which points it dominates(if any) from blocks 22,17,13,14
    //    20 21 22 | 23 24
    //    15 16 17 | 18 19
    //          V    ------
    //    10 11 12 < 13 14
    //    5  6  7    8  9
    //    0  1  2    3  4
    val top_k =
      skylines
        .map(point => (point, GetTotalCount(point, counts_per_block, points_with_block)))
        .sortBy(_._2)(Ordering[Int].reverse)
        .take(top)

    top_k
  }
}
