package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object Task2 {

  // --- Brute Force Solution---

  /**
   * This is the brute force solution to find the top k points based on their dominance score.
   * First, we create a cartesian product for the input rdd with it self,
   * then we group the pairs based on the key and then using the function countDominatedPoints, we calculate its
   * dominanace score. Lastly, we sort them in an descending order and we return the top k
   * @param data input rdd with all the points
   * @param topK number of top elements we want to find
   * @return an array containing the top k elements with their dominance score
   */

  def task2BruteForce(data: RDD[List[Double]], topK: Int): Array[(List[Double], Long)] = {
    data.cartesian(data) //get the cartesian product of the data rdd with it self
      .filter(pair => pair._1 != pair._2) //filter the pairs that are the same point for the key and value
      .groupByKey() //group them based on the key
      .map(pair => (pair._1, countDominatedPoints(pair._1, pair._2))) // map its point to a tuple where the first element is the point and the second its dominance score
      .sortBy(_._2, ascending = false) //sort map by value in descending order
      .top(topK)(Ordering[Long].on(_._2)) //take the first k
  }

  /**
   * This is a helper function used in the brute force solution, that return the number of points the point dominates
   * @param point the point we want to find its dominance score
   * @param values all the other points we have to check if it dominates
   * @return the total number of points the 'point' dominates
   */
  def countDominatedPoints(point: List[Double], values: Iterable[List[Double]]): Long = {
    var totalPoints = 0
    values
      .filter(p => !p.equals(point))
      .foreach(nums => {
        if(Task1.dominates(point, nums)) totalPoints += 1
      })
    totalPoints
  }

  //------ Partitioning ----
  /**
   * Finds top k dominating points using the algorithm STD. It finds the dominating points in parallel for each
   * partition and then combine them to find the global dominating points.
   * @param data input rdd with all the points
   * @param top number of points we want to find
   * @param sc spark context
   * @return an iterator with the top dominating points
   */
  def topKDominatingPoints(data: RDD[List[Double]], top: Int,  acc: ArrayBuffer[(List[Double], Long)], sc: SparkContext): Array[(List[Double], Long)] = {

    val broadcastData = sc.broadcast(data.collect()) //broadcast data rdd

    data.mapPartitions(par => STD(par, top))
       .distinct()
       .map(point => (point._1, Task3.countDominatedPoints(point._1, broadcastData)))
       .reduceByKey(_+_)
       .sortBy(-_._2)
       .take(top)
       .sortBy(-_._2) //sort elements in a descending order based on their dominance score
       .take(top) //take first 'top' elements
  }

  /**
   * Finds top k dominating points using an iterative version of the STD algorithm.
   * Until k = 0, It finds the skyline points, counts the domination score for each skyline point in the partition,
   * and then takes the first element with the highest score, adds it to the result and repeats.
   * @param data the input data
   * @param top number of points we want to find
   * @param result
   * @return
   */
  def topKDominating(data: RDD[List[Double]], top: Int, result: ArrayBuffer[(List[Double], Long)]): Array[(List[Double], Long)] = {
    if(top == 0) {
      result.toArray
    } else {
      val skylines = Task1.ALS(data).toList //find skyline points
      val dominanceScores = data
        .mapPartitions(par =>  calculateDominanceScore(par, skylines)) //finds dominance scores of the skyline points in each partition
        .reduceByKey(_ + _) //adds all the scores for each points
        .sortBy(-_._2) //sorts in descending order

      val point = dominanceScores.take(1)(0)
      result.append(point)
      topKDominating(data.filter(p => p != point._1), top - 1, result)
    }
  }

  def calculateDominanceScore(pointsInPartition: Iterator[List[Double]], skylinePoints: List[List[Double]]): Iterator[(List[Double], Long)] = {
    var result: Map[List[Double], Long] = Map()
    val x1 = pointsInPartition.toList
    skylinePoints
      .map(point => (point, x1.count(p => Task1.dominates(point, p)).toLong))
      .foreach(point => {
        result += point._1 -> (result.getOrElse(point._1, 0L) + point._2)
      })
    result.toIterator
  }


  /**
   * It implements the STD algorithm to find the top-k dominating elements
   * @param data input data with all the points
   * @param top number of points we want to find
   * @param skylines the points that are skylines in the dataset
   * @return an iterator with the top dominating points
   */
  def STD(data: Iterator[List[Double]], top: Int): Iterator[(List[Double], Long)] = {
    if (top == 0) Array()

    val points = data.toList


    var skylinePoints = Task1.sfsForALS(points.toIterator)
      .map(point => Tuple2(point, countDominatedPoints(point, points))) //for every skyline point calculate its dominance score
      .to[ArrayBuffer]
      .sortBy(-_._2) //sort them based on their dominance score

    var result = ArrayBuffer[(List[Double], Long)]() //add the point with the max dominance score to the result array
    var topK = top
    // until all the top-k elements have been found
    breakable {
      while (topK > 0) {
        val pointToAdd = skylinePoints.apply(0) //get the first point of the skylinePoints buffer
        result :+= pointToAdd //add first point(the one with the maximum score) to the result array

        skylinePoints.remove(0) //remove the point since we have already added to the result
        topK -= 1
        if (topK == 0) {
          break()
        }
        val currPoint = pointToAdd._1

        val regionPoints = points
          .filter(p => !p.equals(currPoint)) //filter the current point
          .filter(p => !skylinePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
          .filter(p => Task1.dominates(currPoint, p)) //get only the points that are dominated by 'point'
          .filter(p => isInRegion(currPoint, p, skylinePoints)) //get only the points belonging to region of current point

        if (regionPoints.nonEmpty)
          Task1.sfsForALS(regionPoints.toIterator)
            .map(p => (p, countDominatedPoints(p, points))) //count the dominated points for each
            .foreach(p => skylinePoints.append(p)) //add every point toCalculatePoints array

        //Add points belonging only to region of point to the toCalculatePoints RDD
        skylinePoints = skylinePoints
          .sortWith(_._2 > _._2) //sort points based on their dominance score


      }
    }

    result.toIterator
  }

  def STD(data: RDD[List[Double]], top: Int): Array[(List[Double], Long)] = {
    if (top == 0) Array()

    val skylines = Task1.ALS(data).toList //find skyline points

    val points = data.collect().toList

    var skylinePoints = skylines
      .map(point => Tuple2(point, countDominatedPoints(point, points))) //for every skyline point calculate its dominance score
      .to[ArrayBuffer]
      .sortBy(-_._2) //sort them based on their dominance score

    var result = ArrayBuffer[(List[Double], Long)]() //add the point with the max dominance score to the result array
    var topK = top
    // until all the top-k elements have been found
    breakable {
      while (topK > 0) {
        val pointToAdd = skylinePoints.apply(0) //get the first point of the skylinePoints buffer
        result :+= pointToAdd //add first point(the one with the maximum score) to the result array

        skylinePoints.remove(0) //remove the point since we have already added to the result
        topK -= 1
        val currPoint = pointToAdd._1

        val regionPoints = points
          .filter(p => !p.equals(currPoint)) //filter the current point
          .filter(p => !skylinePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
          .filter(p => Task1.dominates(currPoint, p)) //get only the points that are dominated by 'point'
          .filter(p => isInRegion(currPoint, p, skylinePoints)) //get only the points belonging to region of current point

        Task1.sfsForALS(regionPoints.toIterator) //find the skyline points between the region points
          .map(p => (p, countDominatedPoints(p, points))) //count the dominated points for each
          .foreach(p => skylinePoints.append(p)) //add every point toCalculatePoints array

        //Add points belonging only to region of point to the toCalculatePoints RDD
        skylinePoints = skylinePoints
          .sortWith(_._2 > _._2) //sort points based on their dominance score
      }
    }
    result.toArray
  }


  // ---- Helper Functions ----

  /**
   * Finds if pointB is in the region of pointA
   * @param pointA the pointA
   * @param pointB the pointB
   * @param skylines the skyline points
   * @return true if pointB is in region of pointA, else false
   */
  def isInRegion(pointA: List[Double], pointB: List[Double], skylines: ArrayBuffer[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1) //get the points, without their dominance score
      .filter(p => !p.equals(pointA))  //get the points that are not equal to pointA
      .map(point => !Task1.dominates(point, pointB)) //map to false or true depending if pointB is not dominated by point
      .reduce(_&&_)
  }

  /**
   * Count the points, point dominates from the points dataset
   * @param point the point we want to find how many points dominates
   * @param points the points dataset
   * @return the number of points
   */
  def countDominatedPoints(point: List[Double], points: List[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  //------------------------------------------------------------------------------------------------------------------

  /**
   * Calculate the CellID based from the point coordinates. (Assuming 5 segments of 0.2 length in each dimension)
   * @param point the point we want to find the CellID for
   * @return the CellID coordinates in the format of (0,1,2) <- for a 3D CellID
   */
  def getCellID(point: List[Double]) ={
    val cell_id: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    cell_id
  }

  /**
   * Calculate all the cells coordinates that are greater or equal in each dimension than the given cell
   * @param startingCell the cell we want to examine
   * @param maxIndex the index of the max cell in each dimension (5 per dimension ->index is 4 since we start counting from 0)
   * @param dimensions the Number of total dimensions
   * @return a List of CellID coordinates that are greater or equal in each dimension than the startingCell
   */
  def findCellsGreaterOrEqual(startingCell: List[Int], maxIndex: Int, dimensions: Int): List[List[Int]] = {
    var resultCells = List[List[Int]]()

    def iterate(currentCell: List[Int], currentDimension: Int): Unit = {
      if (currentDimension == dimensions) {
        // Base case: reached the last dimension, add the current cell to the result
        resultCells = resultCells :+ currentCell
      } else {
        for (i <- currentCell(currentDimension) to maxIndex) {
          // Recursive case: iterate through the current dimension
          val nextCell = currentCell.updated(currentDimension, i)
          iterate(nextCell, currentDimension + 1)
        }
      }
    }

    // Start the recursion from the first dimension
    iterate(startingCell, 0)

    resultCells
  }

  /**
   * Calculate the Minimum (worst case scenario) and the Maximum (best case scenario) Dominance score of a given point
   * @param point the point we want to examine
   * @param countsPerCell the map of counts per each cell in the Grid
   * @param dimensions the Number of total dimensions
   * @return (MinCount , MaxCount)
   */
  def getMinMaxCount(point: List[Double], countsPerCell: Map[List[Int], Int], dimensions: Int): (Long, Long) ={

    // MinCount
    var countsForCoordinates_min: List[Int] = List()
    var outwardCoordinates_min: List[List[Int]] = List()
    val starting_cell_min: List[Int] = point.map(elem => ((BigDecimal(elem) / BigDecimal("0.2")).toInt + 1))

    if (!starting_cell_min.exists(elem => elem > 4)) {
      // List of Cells that are definitely dominated by the given point
      outwardCoordinates_min = findCellsGreaterOrEqual(starting_cell_min, 4, dimensions)
      // Number of points that the given point definitely dominates
      countsForCoordinates_min = outwardCoordinates_min.map { coordinates =>
        countsPerCell.getOrElse(coordinates, 0)
      }
    }

    // MaxCount
    val starting_cell_max: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    // List of Cells that MIGHT BE dominated by the given point at the best case scenario
    val outwardCoordinates_max = findCellsGreaterOrEqual(starting_cell_max, 4, dimensions)
    // Number of points that the given point MIGHT dominate at the best case scenario
    val countsForCoordinates_max: List[Int] = outwardCoordinates_max.map { coordinates =>
      countsPerCell.getOrElse(coordinates, 0)
    }

    (countsForCoordinates_min.sum.toLong, countsForCoordinates_max.sum.toLong)
  }

  /**
   * Checks if point_A dominates point_B and returns True if it does or False if it
   * @param point_A the point we want to examine
   * @param point_B the point we want to check if it gets dominated by point_A
   * @return True if if point_A dominates point_B or False if it does not
   */
  def isDominatedByPoint(point_A: List[Double], point_B: List[Double]): Boolean = {
    point_A.zip(point_B).forall(pair => pair._1 <= pair._2)
  }

  /**
   * Count the total number of points that the given point dominates out of the pointsToCheck set of points
   * @param point the point we want to examine
   * @param pointsToCheck the RDD of points to check how many of which, the given point dominates
   * @return the total number of points it dominates
   */
  def countDominanceInCells(point: List[Double], pointsToCheck: RDD[(List[Double], List[Int])]): Long = {
    val pointsDominated =
      pointsToCheck
        .filter(pair => !pair._1.equals(point)) // exclude the point we are checking
        .filter(pair => isDominatedByPoint(point, pair._1))
        .count()

    pointsDominated
  }

  /**
   * Calculate the Dominance Score of a given point
   * @param point the point we want to examine
   * @param minCount the Minimum Count of points that are definitely dominated by the given point
   * @param pointsWithCells the RDD of points to check how many of which, the given point dominates
   * @return the total dominance score of the given point
   */
  def getTotalCount(point: List[Double], minCount: Long , pointsWithCells: RDD[(List[Double], List[Int])]): Long ={
    var sum = minCount
    sum = sum + countDominanceInCells(point, pointsWithCells)
    sum
  }

  /**
   * Find the list of CellIDs that have to be cross-examined to see if a given point dominates any points from those Cells
   * @param point the point we want to examine
   * @param dimensions the number of dimensions
   * @return the list of CellIDs that have a coordinate index same with the CellID of the given point in at least one dimension
   */
  def findNeighbouringCells(point: List[Double], dimensions: Int): List[List[Int]] ={

    // List of Cells that are definitely dominated by the given point
    var outwardCoordinates_min: List[List[Int]] = List(List())
    val starting_cell_min: List[Int] = point.map(elem => ((BigDecimal(elem) / BigDecimal("0.2")).toInt + 1))
    if (!starting_cell_min.exists(elem => elem > 4)) {
      outwardCoordinates_min = findCellsGreaterOrEqual(starting_cell_min, 4, dimensions)
    }

    // List of Cells that MIGHT BE dominated by the given point at the best case scenario
    val starting_cell_max: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    val outwardCoordinates_max = findCellsGreaterOrEqual(starting_cell_max, 4, dimensions)

    // Return the List of Cells we need to check on exactly how many points are dominated by the given point
    outwardCoordinates_max.diff(outwardCoordinates_min)
  }

  /**
   * Algorithm to find the Top-K dominating points out of the given dataset using the Grid Implementation
   * @param data the RDD of points (dataset)
   * @param dimensions the number of dimensions
   * @param top the number of top points we are looking for
   * @param sc spark context
   * @return the list of CellIDs that have a coordinate index same with the CellID of the given point in at least one dimension
   */
  def topKGridDominance(data: RDD[List[Double]],dimensions: Int ,top: Int, sc: SparkContext): Array[(List[Double], Long)] = {

    //  RDD of the data points along with the CellID RDD: (point,CellID)
    val pointsWithCellID =
      data
        .map(point => (point, getCellID(point)))

    val countsPerCell = data
      .map(point => (getCellID(point), 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .collect()
      .toMap

    val pointsWithMinMax =
      data
        .map { point =>
          val (minCount, maxCount) = getMinMaxCount(point, countsPerCell, dimensions)
          (point, minCount, maxCount)
        }
        .sortBy(_._3, ascending= false)

    val minCountOfFirstElement: Long = pointsWithMinMax.first._2

    val candidatePoints =
      pointsWithMinMax
        .filter(  _._3 >=  minCountOfFirstElement)    // I assume that Always gives an RDD greater or equal than top-k and that it fits to the memory
        .collect()
        .toList

    val top_k =
      candidatePoints
        .map( triplet => (triplet, findNeighbouringCells(triplet._1, dimensions))) // point, minCount, maxCount, Neighbouring Cells to check
        .map( triplet => (triplet._1, getTotalCount(triplet._1._1, triplet._1._2, pointsWithCellID.filter(pair => triplet._2.contains(pair._2) ))))
        .map( triplet => (triplet._1._1, triplet._2)) // Keep only point and score

    top_k.sortBy(_._2)(Ordering[Long].reverse)
      .take(top)
      .toArray
  }

}