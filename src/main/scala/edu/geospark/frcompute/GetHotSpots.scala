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
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Envelope
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.text._
import java.util._
import java.lang._

object GetHotSpots 
{
  def loadCSV() : RDD[Row] = 
  {
    val spark = SparkSession.builder().appName("Load CSV").getOrCreate()
    val timeStampDF = spark.read.option("header", "true").csv("/Users/Vivek/Studies/MS/DDS/Phases/3/Dataset/sample-code.csv")
    timeStampDF.select("tpep_pickup_datetime", "pickup_longitude", "pickup_latitude").rdd
  }
  
  def getRectangleRDD (pointRDD : PointRDD) : ArrayList[Envelope] =
  {
    val boundaryEnvelope = pointRDD.boundary()
    //val boundaryEnvelope = new Envelope(0,1,0,1)
    val intervalX = 0.5;
		val intervalY = 0.5;
		val horizontalPartitions = Math.ceil((boundaryEnvelope.getMaxX - boundaryEnvelope.getMinX) / intervalX).toInt
		val verticalPartitions = Math.ceil((boundaryEnvelope.getMaxY - boundaryEnvelope.getMinY) / intervalY).toInt
		val grids = new ArrayList[Envelope]
		for (i <- 0 to (horizontalPartitions - 1))
		{
		  for (j <- 0 to (verticalPartitions - 1))
		  {
		    val grid=new Envelope(boundaryEnvelope.getMinX()+intervalX*i,boundaryEnvelope.getMinX()+intervalX*(i+1),boundaryEnvelope.getMinY()+intervalY*j,boundaryEnvelope.getMinY()+intervalY*(j+1));
		    grids.add(grid);
		  }
		}
		grids
  }
  def main(args: Array[String]): Unit = 
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Get Hotspots").setMaster("local")
    val sc = new SparkContext(conf)
    val geometryFactory = new GeometryFactory();
    var pointRddArray: Array[PointRDD] = new Array[PointRDD](31)
    val timeStampRDD = loadCSV
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal = Calendar.getInstance()
    val dayRDD = timeStampRDD.map 
                 { x => 
                    cal.setTime(format.parse(x.get(0).toString))
                    (cal.get(Calendar.DAY_OF_MONTH), geometryFactory.createPoint(new Coordinate(Double.parseDouble(x.get(1).toString()), Double.parseDouble(x.get(2).toString()))))
                 }
    val pointRDD = new PointRDD(dayRDD.map(x => x._2))
    val rectangleGrids = getRectangleRDD(pointRDD)
    val rectangleRDD = sc.parallelize(rectangleGrids.toArray())
    val list = rectangleRDD.take(rectangleGrids.size()).foreach { x => println(x) }
    for (i <- 1 to 31)
    {
      pointRddArray(i-1) = new PointRDD(dayRDD
                                        .filter(x => x._1 == i)
                                        .map(x => x._2))
      //val list = pointRddArray(i-1).getRawSpatialRDD.rdd.foreach { x => println(x.toString()) }
    }
  }
}