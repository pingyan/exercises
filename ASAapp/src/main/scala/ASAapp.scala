import scala.Predef._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column


object ASA {
  def main(args: Array[String]) {
    val logFile = "~/training.seqs/*"
    val conf = new SparkConf().setAppName("ASA Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext.sql
    println(logFile)
    val seqDF = sqlContext.read.parquet(logFile)
    seqDF.show(3)
    //compress repetitive events
    

    val ngram = new NGram().setInputCol("token")
                           .setOutputCol("Bigrams")
    val bgdf = ngram.transform(seqDF)
    bgdf.show(2)
    val bgdfCnt = bgdf.withColumn("bigram", explode(bgdf("Bigrams")))
               .select('bigram,'key).groupBy('bigram).count()
    //val bgdfCnt = bgdf.select(explode('Bigrams).alias("bigram"),'key).groupBy('bigram).count()
    val df1x2 = bgdfCnt.select(expr("(split(bigram,' '))[0]")
                  .cast("string").as("unigram"),$"bigram",$"count".cast("double"))
    val df1 = df1x2.groupBy("unigram").agg((sum("count").cast("double")).as("ugcount"))

    val traindf = df1x2.join(df1,"unigram")
                    .select('bigram,'unigram,('count/'ugcount).as("cp"))
    traindf.show(3)
    traindf.coalesce(200).write.parquet("~/univ.model")

    sc.stop()
  }
}
