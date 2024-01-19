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

    val skylines = sc.parallelize(Task1.sfs(data))//.collect().toList //find skyline points

    val points_with_min_max =
      skylines
        .map { point =>
          val (minCount, maxCount) = GetMinMaxCount(point, CountsPerCell, dimensions)
          (point, minCount, maxCount)
        }
        .sortBy(_._3, ascending= false)

    //    val maxCountOfFirstElement: Long = points_with_min_max.first._3
    val minCountOfFirstElement: Long = points_with_min_max.first._2

    val candidate_points =
      points_with_min_max
//        .filter(  _._3 >=  minCountOfFirstElement)    // I assume that Always gives an RDD greater or equal than k and that it fits to the memory
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
