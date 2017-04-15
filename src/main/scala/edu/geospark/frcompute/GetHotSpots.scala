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

object GetHotSpots 
{
  def loadCSV() : RDD[Row] = 
  {
    val spark = SparkSession.builder().appName("Load CSV").getOrCreate()
    //val timeStampDF = spark.read.option("header", "true").csv("/Users/Vivek/Studies/MS/DDS/Phases/3/Dataset/sample-code.csv")
    val timeStampDF = spark.read.option("header", "true").csv("hdfs://master:54310/user/hduser/dataset/yellow_tripdata_2015-01.csv")
    timeStampDF.select("tpep_pickup_datetime", "pickup_longitude", "pickup_latitude").rdd
  }
  
  def getRectangleRDD (boundaryEnvelope : Envelope, geometryFactory : GeometryFactory) : (ArrayList[Polygon], ArrayList[(Double, Double)]) =
  {
    // val boundaryEnvelope = new Envelope(0,10,0,10)
    val intervalX = 0.01
		val intervalY = 0.01
		val horizontalPartitions = Math.ceil((boundaryEnvelope.getMaxX - boundaryEnvelope.getMinX) / intervalX).toInt
		val verticalPartitions = Math.ceil((boundaryEnvelope.getMaxY - boundaryEnvelope.getMinY) / intervalY).toInt
		val polygons = new ArrayList[Polygon]
    val squares = new ArrayList[(Double, Double)]
		for (i <- 0 to (horizontalPartitions - 1))
		{
		  for (j <- 0 to (verticalPartitions - 1))
		  {
		    var x1 = boundaryEnvelope.getMinX()+intervalX*i
		    var x2 = boundaryEnvelope.getMinX()+intervalX*(i+1)
		    var y1 = boundaryEnvelope.getMinY()+intervalY*j
		    var y2 = boundaryEnvelope.getMinY()+intervalY*(j+1)
		    var coordinates = new Array[Coordinate](5);
        coordinates(0) = new Coordinate(x1,y1);
        coordinates(1) = new Coordinate(x1,y2);
        coordinates(2) = new Coordinate(x2,y2);
        coordinates(3) = new Coordinate(x2,y1);
        coordinates(4) = coordinates(0);
		    var polygon = new Polygon(geometryFactory.createLinearRing(coordinates), null, geometryFactory)
		    polygons.add(polygon);
		    squares.add((x1, y1))
		  }
		}
		(polygons, squares)
  }
 
  def main(args: Array[String]): Unit = 
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Get Hotspots").setMaster("spark://192.168.0.200:7077").set("spark.driver.host","192.168.0.200").set("spark.ui.port","4040")
    //val conf = new SparkConf().setAppName("Get Hotspots").setMaster("local")
    val sc = new SparkContext(conf)
    val geometryFactory = new GeometryFactory();
    var pointsCountRDD : Map[(scala.Double, scala.Double, scala.Int), scala.Long] = Map()
    val timeStampRDD = loadCSV
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal = Calendar.getInstance()
    
    // Get the pointRDD containing all the points
    val dayRDD = timeStampRDD.map 
                 { x => 
                    cal.setTime(format.parse(x.get(0).toString))
                    (cal.get(Calendar.DAY_OF_MONTH), geometryFactory.createPoint(new Coordinate(Double.parseDouble(x.get(1).toString()), Double.parseDouble(x.get(2).toString()))))
                 }.cache()
    val pointRDD = new PointRDD(dayRDD.map(x => x._2))
    
    // Filter the RDD to contain points only within the New york envelope
    val newyorkEnvelope = new Envelope(-74.25, -73.7, 40.5, 40.9)
    pointRDD.buildIndex(IndexType.RTREE, false)
    val filteredPointRDD = new PointRDD(RangeQuery.SpatialRangeQuery(pointRDD, newyorkEnvelope, true, true))
    
    // Get the rectangle RDD
    val boundaryEnvelope = filteredPointRDD.boundary()
    val minX = boundaryEnvelope.getMinX
    val minY = boundaryEnvelope.getMinY
    val maxX = boundaryEnvelope.getMaxX
    val maxY = boundaryEnvelope.getMaxY
    val rectangleGrids = getRectangleRDD(boundaryEnvelope, geometryFactory)
    val rectangleRDD = new RectangleRDD(new JavaRDD(sc.parallelize(rectangleGrids._1.asScala)))
    
    // Do the spatial join in order to build each cube's attribute values
    var joinResults = sc.emptyRDD[((scala.Double, scala.Double, scala.Int), scala.Long)]
    for (i <- 1 to 31)
    {
      val pointDayRdd = new PointRDD(dayRDD
                                        .filter(x => x._1 == i)
                                        .map(x => x._2))
      pointDayRdd.spatialPartitioning(GridType.RTREE);
      rectangleRDD.spatialPartitioning(pointDayRdd.grids);
      joinResults = joinResults.union(JoinQuery.SpatialJoinQueryCountByKey(pointDayRdd, rectangleRDD, false, true).rdd
                                    .map(x => ((x._1.getCoordinates()(0).x, x._1.getCoordinates()(0).y, i), x._2.toLong)))
    }
    dayRDD.unpersist();
    pointsCountRDD = joinResults.collectAsMap()
    // Variables for calculating the neighbour values
    val pointsCountMapBC = sc.broadcast(pointsCountRDD)
    val minXBC = sc.broadcast(minX)
    val minYBC = sc.broadcast(minY)
    val maxXBC = sc.broadcast(maxX)
    val maxYBC = sc.broadcast(maxY)
    val xArrayBC = sc.broadcast(Array(0, 0.01, -0.01, 0, 0, -0.01, 0.01, 0.01, -0.01))
    val yArrayBC = sc.broadcast(Array(0, 0, 0, 0.01, -0.01, 0.01, -0.01, 0.01, -0.01))
    def getNeighbourValues(x : Double, y : Double, day : Int) : (Long, Int) =
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
    val mean = (joinResults.map(x => x._2).reduce((x, y) =>  x + y)).toDouble / numCells
    val standardDeviation = Math.sqrt(((joinResults.map(x => x._2 * x._2).reduce((x, y) => x + y)).toDouble / numCells) - (mean * mean))
    
    
    val meanBC = sc.broadcast(mean)
    val standardDeviationBC = sc.broadcast(standardDeviation)
    val numCellsBC = sc.broadcast(numCells)

    // Get the square list
    val squaresRDD = sc.parallelize(rectangleGrids._2.asScala)
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
                                    ((x._1, x._2, i), finalValue)
                                  })
    }
    finalResults.sortBy(_._2, false).take(50).foreach(x => println( x._1._2 + ", " + x._1._1 + ", "+ x._1._3 + ", " + x._2))
  }
  
}