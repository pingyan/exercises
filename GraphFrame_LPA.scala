import org.graphframes._
import org.graphframes.examples
import org.apache.spark._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
val sqlContx = new org.apache.spark.sql.SQLContext(sc)
import sqlContx.implicits._

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import scala.util.MurmurHash
import org.apache.spark.sql.functions._


val filenames = sc.wholeTextFiles("baseline/*").map{ case (filename, content) => filename}.collect() ++ sc.wholeTextFiles("testing/*").map{ case (filename, content) => filename}.collect()

case class Record(orgId: String,userId: String,IP: String,Br: String,chad: String)
def efficacyTuning (duration: Int) = {
  val baseline = sc.textFile("baseline.df,"+filenames.slice(0,duration).mkString(","))
  /* EFG: Entity-feature bipartite graph*/
  //case class Record(orgId: String,userId: String,IP: String,Br: String,chad: String)
  val refdf = baseline.map(_.split("`")).
                  map(p => Record(p(0),p(1),p(2),p(3),p(4))).toDF()
  refdf.show()

  val fromTo = refdf.select("orgId","Br").withColumn("endPointType",lit("browserId")).
                  unionAll(refdf.select("orgId","IP").
                  withColumn("endPointType",lit("IP"))).
                  withColumn("entityType",lit("Org"))

  fromTo.show(5)
  val dummyCopy = fromTo.distinct().withColumnRenamed("orgId","dummy_orgId")
  //get joined pair (A,B) or (B,A),but never (A,A)
  val EGPairs = fromTo.distinct().join(dummyCopy,"Br").
                  filter("dummy_orgId > orgId").cache()
  EGPairs.show(5)
  val EGEdge = EGPairs.map(x => (MurmurHash.stringHash(x(1).toString),
                  MurmurHash.stringHash(x(4).toString),1)).
                  toDF("src","dst","weight")
  EGEdge.take(3)

  val vertexRdd = fromTo.distinct().flatMap{ x => Iterable((x(0).toString,x(3).toString),(x(1).toString,x(2).toString))}
  val orgFeatureVertex = vertexRdd.
          map(x =>(MurmurHash.stringHash(x._1),x._1,x._2)).
          toDF("id","entity","entityType")
  val orgFeatureEdge = fromTo.map(x => ((MurmurHash.stringHash(x(0).toString),
          MurmurHash.stringHash(x(1).toString)),1)).reduceByKey(_ + _).
          map(x => Edge(x._1._1,x._1._2,x._2)).
          toDF("src","dst","weight")

  orgFeatureEdge.take(2)

  val gfAlerts = GraphFrame(orgFeatureVertex,orgFeatureEdge.unionAll(EGEdge))
  val LPA = gfAlerts.labelPropagation.maxIter(5).run().cache()
  LPA.show()
  val ERresults = LPA.filter("entityType='Org'").distinct().cache()
  ERresults.distinct.select("entity","label").distinct().
       groupBy("label").count().
       distinct.orderBy($"count".desc).
       take(50).foreach(println)

  val testlog = sc.textFile(filenames.slice(duration,filenames.size).mkString(","))
  val testdf = testlog.map(_.split("`")).
                 map(p => Record(p(0),p(1),p(2),p(3),p(4))).toDF()

  val withGroupLabel = testdf.distinct().
                          join(ERresults.withColumnRenamed("entity","orgId").
                          distinct(),"orgId")
  //withGroupLabel.show(5)

  val oldAlerts = withGroupLabel.select("orgId","Br").
                          distinct().map(r => (r(1),(r(0),1))).
                          reduceByKey((x,y) => (x._1+";"+y._1,x._2+y._2)).
                          sortBy(_._2._2, false)

  val newAlerts = withGroupLabel.select("label","Br").distinct().
                          map(r => (r(1),(r(0),1))).distinct().
                          reduceByKey((x,y) => (x._1+";"+y._1,x._2+y._2)).
                          sortBy(_._2._2, false)
    
  val impr = oldAlerts.join(newAlerts).map(r =>(r._1,r._2._1._2,r._2._2._2)).
      sortBy(_._2,false).cache()
  val oldCnt = impr.filter(_._2>1).count()
  val newCnt = impr.filter(_._3>1).count()
  val imprCnt = impr.filter(x=>x._2 > x._3).count()
  (oldCnt,newCnt,imprCnt,impr.filter(_._2>1).collect())
}
val result1 = efficacyTuning(5)
