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
import org.apache.spark.{SparkConf, SparkContext};
import org.apache.spark.rdd.RDD


object geosparktest {
  def main(args: Array[String]): Unit = {
    println("test")
    val conf=new SparkConf().setAppName("Geo Test").setMaster("spark://192.168.0.200:7077").set("spark.driver.host","192.168.0.200").set("spark.ui.port","4040")
    val sc=new SparkContext(conf)
    val geometryFactory=new GeometryFactory();
    val pointRDDRtreeGrid = new PointRDD(sc, "hdfs://master:54310/user/hduser/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, 10); 
    val rectangleRDDRtreeGrid = new RectangleRDD(sc, "hdfs://master:54310/user/hduser/dataset/zcta510.csv", 0, FileDataSplitter.CSV, false); 
    pointRDDRtreeGrid.spatialPartitioning(GridType.RTREE);
    rectangleRDDRtreeGrid.spatialPartitioning(pointRDDRtreeGrid.grids);
    val timestamp1: Long = System.currentTimeMillis;
    val spatialJoinResultSize = JoinQuery.SpatialJoinQuery(pointRDDRtreeGrid,rectangleRDDRtreeGrid,true, true)
    val timestamp2: Long = System.currentTimeMillis;
    
  }
}