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

  //------------------------------------------------------------------------------------------------------------------

  // Given a point, return the CellID( Coordinates of cell in D-dimension space) that it belongs assuming that max cell per dim are 5
  def GetCellID(point: List[Double]) ={
    val cell_id: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    cell_id
  }

  // Given a cell in a D-dimension Grid, find all the outward cell that are definitely dominated by it
  def getOutwardCells(startingCell: List[Int], maxDimCell: List[Int], dimensions: Int): List[List[Int]] = {

    var currentCoord = startingCell
    val outwardCoordinates = scala.collection.mutable.ListBuffer[List[Int]]()

    while (!currentCoord.exists(elem => elem > 4)) {
      outwardCoordinates += currentCoord
      // Find the cells that are higher than the currentCoord in each dimension
      for (d <- 0 until dimensions) {
        for (i <- currentCoord(d) + 1 until 5) {
          outwardCoordinates += currentCoord.updated(d, i)
        }
      }
      currentCoord = currentCoord.map(elem => elem + 1)
    }

    outwardCoordinates.toList
  }

  // Calculate the minimum and maximum dominance of a point that belongs to a specific cell
  def GetMinMaxCount(point: List[Double], CountsPerCell: Map[List[Int], Int], dimensions: Int): (Long, Long) ={

    val max_dim_cell: List[Int] = List.fill(dimensions)(4)

    // Add 1 to all dims to take the cell that is definitely dominated by the point and then find all the cells outwards that
    val starting_cell_max: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    val outwardCoordinates_max = getOutwardCells(starting_cell_max, max_dim_cell, dimensions)
    val countsForCoordinates_max: List[Int] = outwardCoordinates_max.map { coordinates =>
      CountsPerCell.getOrElse(coordinates, 0)
    }

    var countsForCoordinates_min: List[Int] = List(0)
    val starting_cell_min: List[Int] = point.map(elem => ((BigDecimal(elem) / BigDecimal("0.2")).toInt + 1))
    if (!starting_cell_min.exists(elem => elem > 4)) {
      val outwardCoordinates_min = getOutwardCells(starting_cell_min, max_dim_cell, dimensions)
      // Extract counts for each list in outwardCoordinates
      countsForCoordinates_min = outwardCoordinates_min.map { coordinates =>
        CountsPerCell.getOrElse(coordinates, 0)
      }
    }

    (countsForCoordinates_min.sum.toLong, countsForCoordinates_max.sum.toLong)
  }


  // Return true if base_point is dominated by target_point
  def IsDominatedByPoint(base_point: List[Double], target_point: List[Double]): Boolean = {
    base_point.zip(target_point).forall(pair => pair._1 <= pair._2)
  }

  // Compare a given point with the points of the given block_id
  def Count_Dominance_in_Cells(point: List[Double], points_to_check: RDD[(List[Double], List[Int])]): Long = {
    val points_dominated =
      points_to_check
//        .flatMap(_._2)
        .filter(pair => !pair._1.equals(point)) // exclude the point we are checking
        .filter(pair => IsDominatedByPoint(point, pair._1))
        .count()
        .toLong

    points_dominated
  }

  // Get the total dominance score of a given point
  def GetTotalCount(point: List[Double], minCount: Long , points_with_cells: RDD[(List[Double], List[Int])]): Long ={
    var sum = minCount
    sum = sum + Count_Dominance_in_Cells(point, points_with_cells)
    sum
  }

  // Get the total dominance score of a given point
  def FindNeighbooringCells(point: List[Double], dimensions: Int): List[List[Int]] ={

    var cells_to_check: List[List[Int]] = List()
    val CellID = GetCellID(point)
    // Add the cell that the point belongs to
    cells_to_check = cells_to_check :+ CellID

    // Add the rest of the cells to the list
    for(d <- 0 until dimensions) {
      for (i <- CellID(d)+1 until 5) {
        val add_cell: List[Int] = CellID.updated(d, i)
        cells_to_check = cells_to_check :+ add_cell
      }
    }

    cells_to_check
  }

  def Top_k_GridDominance(data: RDD[List[Double]],dimensions: Int ,top: Int, sc: SparkContext): List[(List[Double], Long)] = {

    // Create an RDD of the data points along with the BLock ID RDD: (point,BlockID)
    val points_with_cellID =
      data
        .map(point => (point, GetCellID(point)))
//        .groupBy { case (_, cellID) => cellID }
//        .mapValues(iter => iter.map { case (point, _) => point })

    val CountsPerCell = data
      .map(point => (GetCellID(point), 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .collect()
      .toMap

    val points_with_min_max =
      data
        .map { point =>
          val (minCount, maxCount) = GetMinMaxCount(point, CountsPerCell, dimensions)
          (point, minCount, maxCount)
        }
        .sortBy(_._3, ascending= false)

//    val maxCountOfFirstElement: Long = points_with_min_max.first._3
    val minCountOfFirstElement: Long = points_with_min_max.first._2

    val candidate_points =
      points_with_min_max
        .filter(  _._3 >=  minCountOfFirstElement)    // I assume that Always gives an RDD greater or equal than top-k and that it fits to the memory
        .collect()
        .toList

    val top_k =
      candidate_points
//        .collect()
//        .toList
        .map( triplet => (triplet, FindNeighbooringCells(triplet._1, dimensions))) // point, minCount, maxCount, Neighbouring Cells to check
        .map( triplet => (triplet._1, GetTotalCount(triplet._1._1, triplet._1._2, points_with_cellID.filter(pair => triplet._2.contains(pair._2) ))))
        .map( triplet => (triplet._1._1, triplet._2)) // Keep only point and score

    top_k.sortBy(_._2)(Ordering[Long].reverse)
        .take(top)
  }

}
