package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


object Task2 {

  def task2BruteForce(data: RDD[List[Double]], top: Int) = {
    data.cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()
      .map(pair => (pair._1, countDominatedPoints(pair._1, pair._2)))
      .sortBy(_._2) //sort map by value in descending order
      .top(top)(Ordering[Long].on(_._2))
  }

  def countDominatedPoints(key: List[Double], values: Iterable[List[Double]]): Long = {
    var totalPoints = 0
    values
      .filter(p => !p.equals(key))
      .foreach(nums => {
        if(Task1.dominates(key, nums)) totalPoints += 1
      })
    totalPoints
  }

  //TODO remove sc if not needed
  def STD(data: RDD[List[Double]], top: Int,  sc: SparkContext) = {
    if(top == 0) Array()

    val skylines = Task1.sfs(data) //find skyline points

    val skylinePoints = skylines
      .map(point => Tuple2(point, countDominatedPoints2(point, data.collect()))) //for every skyline point calculate its dominance score
      .sortWith(_._2 > _._2) //sort them based on their dominance score


    val point = skylinePoints.apply(0)
    val result = Array(point) //add the point with the max dominance score to the result array
    skylinePoints.remove(0)

    val answer = helper2(top - 1, point._1, data.filter(p => !p.equals(point)).collect(), result, skylinePoints)

    answer
  }

  //TODO rename toCalculatePoints
  def helper2(topK: Int, point: List[Double], data: Array[List[Double]], result: Array[(List[Double], Long)],
             toCalculatePoints: ArrayBuffer[(List[Double], Long)]): Array[(List[Double], Long)] = {
    if(topK == 0)  return result

    var res = result

    data
      .filter(p => !p.equals(point)) //filter the current point
      .filter(p => !toCalculatePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
      .filter(p => Task1.dominates(point, p)) //get only the points that are dominated by 'point'
      .filter(p => isInRegion2(point, p, toCalculatePoints)) //get only the points belonging to region of current point
      .map(p => Tuple2(p, countDominatedPoints2(p, data))) //count the dominated points for each
      .foreach(p => toCalculatePoints.append(p)) //add every point toCalculatePoints array

    //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints
      .sortWith(_._2 > _._2) //sort points based on their dominance score

    val pointToAdd = toCalculatePoints.apply(0)
    res :+= pointToAdd //add first point(the one with the maximum score) to the result array

    newCalculatePoints.remove(0)
    helper2(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), res, newCalculatePoints)

  }

  //Finds if pointB is in the region of pointA
  // TODO should be dominated by pointA
  def isInRegion2(pointA: List[Double], pointB: List[Double], skylines: ArrayBuffer[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1) //get the points, without their dominance score
//      .filter(p => Task1.dominates(pointA, p)) //get only the points that pointA dominates
      .filter(p => !p.equals(pointA))  //get the points that are not equal to pointA
      .map(point => !Task1.dominates(point, pointB)) //map to false or true depending if pointB is not dominated by point
      .reduce(_&&_)
  }

  def countDominatedPoints3(point: List[Double], points: Iterable[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  def countDominatedPoints2(point: List[Double], points: Array[List[Double]]) : Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  def countDominatedPoints(point: List[Double], points: RDD[List[Double]]) : Long = {
//    points
//      .map(p => Task1.dominates(point, p))
//      .count()
    var totalPoints = 0
    points
//      .filter(p => !p.equals(point))
      .foreach(nums => {
        if(Task1.dominates(point, nums)) totalPoints += 1
      })
    totalPoints
  }


  //TODO rename toCalculatePoints
  def helper(topK: Int, point: List[Double], data: RDD[List[Double]], result: Array[(List[Double], Long)],
             toCalculatePoints: RDD[(List[Double], Long)]): Any = {
    if(topK == 0)  return result

      //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints.union(data
//      .filter(p => !result.map(_._1).contains(p))
      .filter(p => isInRegion(point, p, toCalculatePoints))
      .map(p => Tuple2(p, countDominatedPoints(p, data))))
      .sortBy(_._2, ascending = false)

    val pointToAdd = toCalculatePoints.first()
    result :+ pointToAdd
    helper(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), result, newCalculatePoints)

  }

  //Finds if pointB is in the region of pointA
  def isInRegion(pointA: List[Double], pointB: List[Double], skylines: RDD[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1)
      .filter(p => !p.equals(pointA))
      .map(point => !Task1.dominates(point, pointB))
      .reduce(_&&_)
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
    for (x <- start_x to 4) {
      for (y <- start_y to 4) {
        val block_id = y*5 + x
        sum = sum + counts_per_block(block_id)
      }
    }
    sum
  }

  // Get RDD of points that are dominated by point
  def IsDominatedByPoint(base_point: List[Double], target_point: List[Double]): Boolean = {
    if ( (base_point(0) >=  target_point(0)) && (base_point(1) >=  target_point(1)) ) {
      true
    } else {
      false
    }
  }

  // Compare a given point with the points of the given block_id
  def GetDominance_in_Block(point: List[Double], block_id: Int, points_with_block: RDD[(List[Double], Int)]) = {
    val points_dominated =
      points_with_block
        //.filter(p=> GetBlockID(p) == block_id)
        .filter(pair => pair._2 == block_id) // get all points that belong to block_id
        .filter(p => !p._1.equals(point)) // exclude the point we are checking
        .filter(p => IsDominatedByPoint(point, p._1))
        .map(p => 1)
        .reduce(_+_)

    points_dominated
  }

  // Get the total dominance score of a given point
  def GetTotalCount(point: List[Double], counts_per_block: List[Int], points_with_block: RDD[(List[Double], Int)] ) ={
    var sum = GetMinCount(point, counts_per_block)
    val start_x = (BigDecimal(point(0)) / BigDecimal("0.2")).toInt
    val start_y = (BigDecimal(point(1)) / BigDecimal("0.2")).toInt
    // Get the Dominance score of the point within the block it belongs
    var block_to_compare = start_y * 5 + start_x
    sum = sum + GetDominance_in_Block(point, block_to_compare, points_with_block)
    // Check all the blocks to the right
    for (x <- (start_x + 1) to 4) {
      block_to_compare = start_y * 5 + x
      sum = sum + GetDominance_in_Block(point, block_to_compare, points_with_block)
    }
    // check all the blocks above
    for (y <- (start_y + 1) to 4) {
      block_to_compare =  y * 5 + start_x
      sum = sum + GetDominance_in_Block(point, block_to_compare, points_with_block)
    }
    sum
  }

  def Top_k_GridDominance(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Int)] = {

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
    // .map( pair => (pair._2, pair._1) )
    // .groupByKey()
    // .map( pair => (pair._1, pair._2) )

    val block_counts: List[Int] = List.fill(25)(0)

    // Count how many points belong to each block
    val block_count = data
      .map(point => (GetBlockID(point), 1))
      .reduceByKey(_ + _)
//      .map(pair => pair._2)
      .collect()
      .toList

    val counts_per_block: List[Int] = block_count.foldLeft(block_counts) {
      case (acc, (index, countToAdd)) =>
        acc.updated(index, acc(index) + countToAdd)
    }

    // Get Skyline points
    // val skylines = Task1.sfs(data) //find skyline points    TODO: THIS IS NOT AN RDD
    val skylines = Task1.task1BruteForce(data) //find skyline points

    val skylines_with_block =
      skylines
        .map(point => (point, GetBlockID(point)))
//        .map(pair => (pair._2, pair._1))
//        // .groupByKey()
//        .map(pair => (pair._1, pair._2))


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
        .sortBy(_._2)
        .take(top)

    top_k
  }

}
