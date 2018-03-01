import scala.Predef._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object ASAScorer {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Anomalous Sequence Scoring App")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext.sql

    val trainpath = "~/univ.model"
    val traindf = sqlContext.read.parquet(trainpath)
    
    val testpath = "~/testing.seqs/*"

    val testdf = sqlContext.read.parquet(testpath)

    val ngram = new NGram().setInputCol("token")
                               .setOutputCol("Bigrams")

    val df_sampled = ngram.transform(testdf)
    val bgdf_sampled = df_sampled.withColumn("bigram",explode(df_sampled("Bigrams"))).select('bigram,'key)
    val neg_sampled = bgdf_sampled

    // ###################### NEGATIVE TESTING #######################
    // calculate perplexity/sequence prob 
    // add-one smoothing 

    val testdf_neg = neg_sampled.join(traindf,Seq("bigram"),"left_outer").na.fill(0.000001)

    // ###################### PERPLEXITY SCORE CALCULATION ###########
    // given bigram, cp, now we calculate the perlexity score
    // perplexity = pow(2.0, -1/N*(SUM(log(p(wi/wi-1)))))  
    // the higher the perplexity, the more anomalous 

    val tmp = testdf_neg.select('key,'cp,log(2.0,"cp").alias("logp"))

    // concat cp of all bigrams that make a session 
    
    //val testdf_neg_wcp = testdf_neg.select(concat($"bigram",$"cp")).alias('bicp'),"key").groupBy('key).foreach(println)
    val result_neg = tmp.groupBy('key).agg(pow(2,avg('logp)).alias("score"))
    result_neg.take(4)
    //+--------------------+--------------------+
    //|                 key|               score|
    //+--------------------+--------------------+
    //|aaaaa               |1.000000000000000...|
    //|bbbb                |  0.7669138479057729|
    //+--------------------+--------------------+

    
    //result_neg = tmp.groupby('key').agg(pow(sum('logp'),((-1)*avg('len'))).alias('score'))
    result_neg.sort($"score").rdd.coalesce(10).saveAsTextFile("~/test_Univ")

    //testdf_neg.saveAsParquetFile('data/testdf_neg')
    /*
    result_neg.select(split(result_neg.key,'-')[0].alias('key'),'score')
      .sort('score',ascending=True)
      .join(testdf_neg.select(split(testdf_neg.key,'-')[0].alias('key'),'bigram','cp'),['key'])
      .saveAsParquetFile('data/results_neg_org62_wSeq_wTraining')
    */
    //result_neg.join(kvpair_sampled,['key']).sort(col('score').desc()).saveAsParquetFile('data/results_Univ_wSeq')

    //result_neg.join(kvpair_sampled,['key']).join(testdf_neg_wcp,['key']).sort(col('score')).saveAsParquetFile('data/results_Univ_wSeq_wCP')

  }
}
