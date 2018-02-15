import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
val sqlContx = new org.apache.spark.sql.SQLContext(sc)
import sqlContx.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import scala.util.MurmurHash
import scala.io.Source

%AddJar file:/Users/../Software/lightning-scala/target/scala-2.10/lightning-scala_2.10-0.1.0-SNAPSHOT.jar -f
%AddJar file:/Users/../.ivy2/cache/org.scalaj/scalaj-http_2.10/jars/scalaj-http_2.10-1.1.4.jar -f
%AddJar file:/Users/../.ivy2/cache/org.json4s/json4s-native_2.10/jars/json4s-native_2.10-3.2.9.jar -f
%AddJar file:/Users/../.ivy2/cache/org.scalatest/scalatest_2.10/bundles/scalatest_2.10-2.2.1.jar -f

import org.viz.lightning._
val lgn = Lightning("http://localhost:3000")
lgn.enableNotebook()

lgn.line(Array(Array(1.0,1.0,2.0),Array(3.0,9.0,20.0)), label=Array(1,2))

def buildGraph (df: DataFrame): Graph[String, Int] = {
    /*
     * read data as DataFrame and build Graph
     * */

    val fromTo = df
    val lookup = df.flatMap(x => Iterable(x(0).toString, x(1).toString)).distinct()
    val id = 1 to lookup.count().toInt
    // create a Map of node attributes and sequential ids 
    val idMap = lookup.collect() zip id toMap
    
    val orgFeatureVertex: RDD[(VertexId, String)] = lookup.map(x =>(idMap(x),x))

    val orgFeatureEdge = fromTo.map(x => ((idMap(x(0).toString),
        idMap(x(1).toString)),1)).reduceByKey(_ + _).
        map(x => Edge(x._1._1,x._1._2, x._2))

    val defaultOrgFeature = ("Missing")
    val orgFeatureGraph = Graph(orgFeatureVertex, orgFeatureEdge)
    orgFeatureGraph
}.persist()

val base_dir = ""

val refIn = base_dir + "demo-02-25.csv.gz"
val testIn = base_dir + "demo-02-23.csv.gz"
val headerIn = base_dir + "resources/header.csv"
val linkFeature = "linkId"
val idField = "entityId"
//val TH = args(5).toInt
val TH = 3
case class FV(linkId:String, entityId:String)
//get header indexes
val header = Source.fromFile(headerIn).getLines.toList(0).split(",")
println(header)
val indOrg = header.indexOf(idField)
val indFeature = header.indexOf(linkFeature)
println(indOrg)

val base_dir = ""

val refIn = base_dir + "linkIdDataMore/applogLong-02-25.csv.gz,linkIdDataMore/applogLong-02-26.csv.gz,linkIdDataMore/applogLong-02-27.csv.gz,linkIdDataMore/applogLong-02-28.csv.gz"
val testIn = base_dir + "linkIdDataMore/applogLong-02-23.csv.gz"
val headerIn = base_dir + "resources/header_linkIdDataMore.csv"
val linkFeature = "linkId" /* one feature supported as of 04.20.16 */
val idField = "entityId"
//val TH = args(5).toInt
val TH = 3
case class FV(linkId:String, entityId:String)
val indOrg = 3 
val indFeature = 0

val sqlContx = new org.apache.spark.sql.SQLContext(sc)
import sqlContx.implicits._
val refDF = sc.textFile(refIn).map(_.toString.replace("\"","").split(",")).
                               map(r=>FV(r(indFeature).toString,r(indOrg).toString)).toDF()
                               
val testDF = sc.textFile(testIn).map(_.toString.replace("\"","").split(",")).
                                 map(r=>FV(r(indFeature).toString,r(indOrg).toString)).toDF()
val orgFeatureRGDF = refDF.select(idField, linkFeature)
val orgFeatureDGDF = testDF.select(idField, linkFeature)
orgFeatureRGDF.take(2)

val orgFeatureRGDFDedup = orgFeatureRGDF.distinct() // dedup, otherwise, orgPairs contains many duplicates
val orgFeatureRGDFDummycopy = orgFeatureRGDFDedup.withColumnRenamed("entityId","renamed_entityId")
orgFeatureRGDFDummycopy.columns

val orgPairs = orgFeatureRGDFDedup.join(
                   orgFeatureRGDFDummycopy,"linkId").
                   filter($"renamed_entityId" > $"entityId").
                   select($"entityId",$"renamed_entityId")//to keep one pair (A,B) but not (B,A)

// Build Graph 

val O2OG = buildGraph(orgPairs)

http://localhost:3000/visualizations/9f4c4b22-847a-4d0e-ad15-5784c5f11376/

//Fill in the degree information

case class DegreeCnt(feature: String, outDeg: Int)
// Create a degreeInfo Graph
val initDegreeInfo: Graph[DegreeCnt, Int] = O2OG.mapVertices{ case (id, feature) => DegreeCnt(feature, 0) }

val O2ODegreeGraph = initDegreeInfo.outerJoinVertices(O2OG.inDegrees) {
  case (id, u, inDegOpt) => DegreeCnt(u.feature, inDegOpt.getOrElse(0))
}

O2ODegreeGraph.triplets.take(2)

val O2Osub=O2ODegreeGraph.subgraph(vpred= (id, degreeCnt) => degreeCnt.outDeg >10)
val ccsub = O2Osub.connectedComponents()
val connectedComponents = ccsub.vertices.sortBy(_._1).map(_._2.toInt).collect()
val links = O2Osub.edges.collect().map(e => Array(e.srcId.toInt, e.dstId.toInt))
val viz =  lgn.force(links, label=connectedComponents)

// take out the hub orgs

// org similarity can be modeled as weights generated via parallel edges in a multigraph - not something graphX currently supports though

// to build RGDG' and RG', need to map the ccLabels back to the DataFrames
// make ccLabels tuple of (orgId, cclabelOrgId)
// join on dataframe or join on vertexRDD?
val orgFeatureRGDF = refDF.select(idField, linkFeature) //  flatMap().join()
val orgFeatureDGDF = testDF.select(idField, linkFeature)
val orgFeatureRGDGDF = orgFeatureDGDF.unionAll(orgFeatureRGDF)


val df = orgPairs
val fromTo = df
val lookup = df.flatMap(x => Iterable(x(0).toString, x(1).toString)).distinct()
val id = 1 to lookup.count().toInt
// create a Map of node attributes and sequential ids 
val idMap = lookup.collect() zip id toMap
val orgFeatureVertex: RDD[(VertexId, String)] = lookup.map(x =>(idMap(x),x))

val orgFeatureEdge = fromTo.map(x => ((idMap(x(0).toString),
    idMap(x(1).toString)),1)).reduceByKey(_ + _).
    map(x => Edge(x._1._1,x._1._2, x._2))

val defaultOrgFeature = ("Missing")
val O2OG = Graph(orgFeatureVertex, orgFeatureEdge)
val cc = O2OG.connectedComponents()

val orgBycc = orgFeatureVertex.join(cc.vertices).map{case (vid, (org, cc)) => (cc, org)}
// a.join(b) is inner join by default, it therefore returns 1146 entries 
// a.rightOuterJoin(b) keeps the duplicate entries in the right table when there are duplicates on the same join key.
// no actually, the join part is fine, but be careful with the order of tuple elements: the bad example is val ccOrgMap = orgFeatureVertex.rightOuterJoin(orgBycc).map{case (vid, (ccOrg, org)) => (ccOrg, org)}.collect() toMap

// create another map
val ccOrgMap = orgFeatureVertex.join(orgBycc).map{case (vid, (ccOrg, org)) => (org, ccOrg)}.collect() toMap

val CCorgFeatureRGDF = orgFeatureRGDF.map{case Row(x,y) =>(ccOrgMap.getOrElse(x.toString,x).toString,y.toString)}.toDF
val CCorgFeatureDGDF = orgFeatureDGDF.map{case Row(x,y) =>(ccOrgMap.getOrElse(x.toString,x).toString,y.toString)}.toDF


def buildGraph (df: DataFrame): Graph[String, Int] = {
    /*
     * read data as DataFrame and build Graph
     * */

    val fromTo = df
    val lookup = df.flatMap(x => Iterable(x(0).toString, x(1).toString))

    val orgFeatureVertex: RDD[(VertexId, String)] = lookup.map(x =>(MurmurHash.stringHash(x),x))

    val orgFeatureEdge = fromTo.map(x => ((MurmurHash.stringHash(x(0).toString),
        MurmurHash.stringHash(x(1).toString)),1)).reduceByKey(_ + _).
        map(x => Edge(x._1._1,x._1._2, x._2))

    val defaultOrgFeature = ("Missing")
    val orgFeatureGraph = Graph(orgFeatureVertex, orgFeatureEdge)
    orgFeatureGraph
}.persist()


def naiveDetector(RGDG: Graph[String, Int], RG: Graph[String, Int], Threshold: Int) {
    println(s"======================")
    val verticesCnt = RGDG.numVertices
    println(s"number of RGDG vertices $verticesCnt")
    val edgesCnt = RGDG.numEdges
    println(s"number of RGDG edges $edgesCnt")
    println(s"======================")

    case class DegreeCnt(feature: String, outDegRG: Int, outDegRGDG: Int)
    val initDegreeInfo: Graph[DegreeCnt, Int] = RGDG.mapVertices{ case (id, feature) => DegreeCnt(feature, 0, 0) }
    val DegreeGraph = initDegreeInfo.outerJoinVertices(RG.inDegrees) {
        case (id, u, inDegOpt) => DegreeCnt(u.feature, inDegOpt.getOrElse(0), u.outDegRGDG)
      }.outerJoinVertices(RGDG.inDegrees) {
        case (id, u, outDegOpt) => DegreeCnt(u.feature, u.outDegRG, outDegOpt.getOrElse(0))
      }.persist()
    DegreeGraph.vertices.filter{
        case (id, deg) => (deg.outDegRGDG - deg.outDegRG) >= Threshold
      }.collect.foreach(println)
}

val orgCCMap = sc.textFile("tmp/ccMap")
val tmpPair = orgCCMap.map{ x => (x.drop(1).dropRight(1).split(",")(0),x.drop(1).dropRight(1).split(",")(1))}
val ccOrgMap = tmpPair.collect() toMap

val CCorgFeatureRGDF = orgFeatureRGDF.map{case Row(x,y) =>(ccOrgMap.getOrElse(x.toString,x).toString,y.toString)}.toDF
val CCorgFeatureDGDF = orgFeatureDGDF.map{case Row(x,y) =>(ccOrgMap.getOrElse(x.toString,x).toString,y.toString)}.toDF

case class DegreeCnt(feature: String, outDegRG: Int, outDegRGDG: Int)
val initDegreeInfo: Graph[DegreeCnt, Int] = detectionGraph.mapVertices{ case (id, feature) => DegreeCnt(feature, 0, 0) }
val DegreeGraph = initDegreeInfo.outerJoinVertices(referenceGraph.inDegrees) {
    case (id, u, inDegOpt) => DegreeCnt(u.feature, inDegOpt.getOrElse(0), u.outDegRGDG)
  }.outerJoinVertices(detectionGraph.inDegrees) {
    case (id, u, outDegOpt) => DegreeCnt(u.feature, u.outDegRG, outDegOpt.getOrElse(0))
  }
DegreeGraph.vertices.filter{
    case (id, deg) => (deg.outDegRGDG - deg.outDegRG) >= 3
  }.collect.foreach(println)

val referenceGraph = buildGraph(orgFeatureRGDF)

referenceGraph.triplets.take(2)

val links = referenceGraph.edges.collect().map(e => Array(e.srcId.toInt, e.dstId.toInt))
links

val viz = lgn.force(links)
viz.getPermalinkURL


val fromTo = df
val lookup = df.flatMap(x => Iterable(x(0).toString, x(1).toString)).distinct()
val id = 1 to lookup.count().toInt
// create a Map of node attributes and sequential ids 
val idMap = lookup.collect() zip id toMap
