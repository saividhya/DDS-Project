package edu.geospark.frcompute
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geospark.spatialRDD.RectangleRDD
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.enums.GridType
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Envelope
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaRDD
import org.apache.log4j.Level
import java.text._
import java.util.ArrayList
import java.util.Calendar
import java.lang._
import collection.JavaConverters._
import scala.collection.Map

object GetHotSpotswithoutGspark
{
  def loadCSV() : RDD[Row] = 
  {
    val spark = SparkSession.builder().appName("Load CSV").getOrCreate()
    val timeStampDF = spark.read.option("header", "true").csv("/Users/Vivek/Studies/MS/DDS/Phases/3/Dataset/yellow_tripdata_2015-01.csv")
    //val timeStampDF = spark.read.option("header", "true").csv("hdfs://master:54310/user/hduser/dataset/yellow_tripdata_2015-01.csv")
    timeStampDF.select("tpep_pickup_datetime", "pickup_longitude", "pickup_latitude").rdd
  }
  
  def getAllSquares (boundaryEnvelope : Envelope) : ArrayList[(Int, Int)] =
  {
    // val boundaryEnvelope = new Envelope(0,10,0,10)
    val intervalX = 0.01
		val intervalY = 0.01
		val horizontalPartitions = Math.ceil((boundaryEnvelope.getMaxX - boundaryEnvelope.getMinX) / intervalX).toInt
		val verticalPartitions = Math.ceil((boundaryEnvelope.getMaxY - boundaryEnvelope.getMinY) / intervalY).toInt
    val squares = new ArrayList[(Int, Int)]
		for (i <- 0 to (horizontalPartitions - 1))
		{
		  for (j <- 0 to (verticalPartitions - 1))
		  {
		    squares.add((i, j))
		  }
		}
		squares
  }
 
  def main(args: Array[String]): Unit = 
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //val conf = new SparkConf().setAppName("Get Hotspots").setMaster("spark://192.168.0.200:7077").set("spark.driver.host","192.168.0.200").set("spark.ui.port","4040")
    val conf = new SparkConf().setAppName("Get Hotspots").setMaster("local")
    val sc = new SparkContext(conf)
    val interval = 0.01
    
    // Construct the squares for the new york envelope
    val boundaryEnvelope = new Envelope(-74.25, -73.7, 40.5, 40.9)  
    val minX = 0
    val maxX = ((boundaryEnvelope.getMaxX - boundaryEnvelope.getMinX) / interval).toInt
    val minY = 0
    val maxY = ((boundaryEnvelope.getMaxY - boundaryEnvelope.getMinY) / interval).toInt
    println(minX + " " + maxX + " " + minY + " " + maxY)
    val minXBC = sc.broadcast(minX)
    val maxXBC = sc.broadcast(maxX)
    val minYBC = sc.broadcast(minY)
    val maxYBC = sc.broadcast(maxY)
    
    val rectangleGrids = getAllSquares(boundaryEnvelope)
    val intervalBC = sc.broadcast(interval)
    val timeStampRDD = loadCSV
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal = Calendar.getInstance()
    
    // Get the pointRDD containing all the points which fall within the Newyork envelope
    val dayFilteredRDD = timeStampRDD.map 
                         { x => 
                            cal.setTime(format.parse(x.get(0).toString))
                            (x.get(1).toString().toDouble, x.get(2).toString().toDouble, cal.get(Calendar.DAY_OF_MONTH))
                         }
                         .filter(x => x._1 >= boundaryEnvelope.getMinX && x._1 <= boundaryEnvelope.getMaxX && x._2 >= boundaryEnvelope.getMinY && x._2 <= boundaryEnvelope.getMaxY)
                         .cache()
    // dayFilteredRDD.foreach(x => println(x))
    // println(dayFilteredRDD.count())
   
    // Function to find the cell's X and Y value for a given point
    def findCell(x : (Double, Double)) : (Int, Int) = 
                         {
                             val newx = ((x._1 - boundaryEnvelope.getMinX) / intervalBC.value).toInt
                             val newy = ((x._2 - boundaryEnvelope.getMinY) / intervalBC.value).toInt
                             (newx, newy)
                         }
    println(findCell(-74.0075759887695,40.7325363159179))
    println(findCell(-74.0163955688476,40.7064018249511))
    // Construct the map for the cell value
    val cubeAttributeRDD = dayFilteredRDD.map
                           {
                             x => 
                             val cellValue = findCell(x._1, x._2)
                             ((cellValue._1, cellValue._2, x._3), 1.toLong)
                           }
                           .reduceByKey((x, y) => x + y)
       
    //cubeAttributeRDD.takeOrdered(50)(Ordering[Long].reverse.on { x => x._2 }).foreach(x => println(x))
    // println(cubeAttributeRDD.map(x => x._2).reduce((x, y) => x + y))
    // Variables for calculating the neighbour values
    val cubeAttributeMap = cubeAttributeRDD.collectAsMap()
    val pointsCountMapBC = sc.broadcast(cubeAttributeMap)
    val xArrayBC = sc.broadcast(Array(0, 1, -1, 0, 0, -1, 1, 1, -1))
    val yArrayBC = sc.broadcast(Array(0, 0, 0, 1, -1, 1, -1, 1, -1))
    
    // Function to get the neighbour values including itself and the number of neighbours
    def getNeighbourValues(x : Int, y : Int, day : Int) : (Long, Int) =
    {
      var count : scala.Long = 0
      var weight : scala.Int = 0
      val countsMap = pointsCountMapBC.value
      def checkXY(x : Double, y : Double, z: Int) : Boolean = 
        if(x >= minXBC.value && x <= maxXBC.value && y >= minYBC.value && y <= maxYBC.value && z >= 1 && z <= 31) true else false
        
      for( i <- 0 to xArrayBC.value.length -1)
      {
        val newx = x + xArrayBC.value(i)
        val newy = y + yArrayBC.value(i)
        if (checkXY(newx, newy, day)) 
        {
          count += countsMap.getOrElse((newx, newy, day), 0.toLong)
          weight += 1
        }
        if (checkXY(newx, newy, day + 1))
        {
          count += countsMap.getOrElse((newx, newy, day+1), 0.toLong)
          weight += 1
        }
        if (checkXY(newx, newy, day - 1))
        {
          count += countsMap.getOrElse((newx, newy, day-1), 0.toLong)
          weight += 1
        }
      }
      (count, weight)
    }
    // Calculation of Formula parameters
    val horizontalPartitions = Math.ceil((boundaryEnvelope.getMaxX - boundaryEnvelope.getMinX) / 0.01).toInt
  	val verticalPartitions = Math.ceil((boundaryEnvelope.getMaxY - boundaryEnvelope.getMinY) / 0.01).toInt
  	val numCells = horizontalPartitions * verticalPartitions * 31
    val intermean = (cubeAttributeRDD.map(x => x._2).reduce((x, y) =>  x + y)).toDouble 
    val mean = intermean / numCells
    val standardDeviation = Math.sqrt(((cubeAttributeRDD.map(x => x._2 * x._2).reduce((x, y) => x + y)).toDouble / numCells) - (mean * mean))
    
    println("mean : " + mean + " standard deviation : " +  standardDeviation + " num cells " + numCells + " hp " + horizontalPartitions + " vp " + verticalPartitions)
    val meanBC = sc.broadcast(mean)
    val standardDeviationBC = sc.broadcast(standardDeviation)
    val numCellsBC = sc.broadcast(numCells)
  
    // Get the square list
    val squaresRDD = sc.parallelize(rectangleGrids.asScala)
    var finalResults = sc.emptyRDD[((scala.Double, scala.Double, scala.Int), scala.Double)]
    for (i <- 1 to 31)
    {
      finalResults = finalResults.union(squaresRDD.map 
                                  { x => 
                                    val (neighbourValues, weight) = getNeighbourValues(x._1, x._2, i)
                                    val numerator : Double = neighbourValues - (meanBC.value * weight)
                                    val denominatorRight : Double = ((numCellsBC.value * weight) - (weight * weight)) / (numCellsBC.value - 1)
                                    val denominator : Double = standardDeviationBC.value * Math.sqrt(denominatorRight)
                                    val finalValue : Double= numerator / denominator
                                    (((x._1 * 0.01) + boundaryEnvelope.getMinX, (x._2 * 0.01) + boundaryEnvelope.getMinY, i), finalValue)
                                  })
    }
    finalResults.sortBy(_._2, false).take(50).foreach(x => println( x._1._2 + ", " + x._1._1 + ", "+ x._1._3 + ", " + x._2)) 
  }
}