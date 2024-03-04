import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set
import org.apache.spark.rdd.RDD

object SparkPageRank {
  //构建边
  def readFromTxtByLine(sc: SparkContext, resultArray: scala.collection.mutable.ArrayBuffer[(String, String)], allLines: scala.collection.mutable.Set[String]) = {
    var textFile = sc.textFile("hdfs://master:9000/final/pagerank/resultURL.txt")
    val lines = textFile.collect().toList
    var foundLine: String = ""
    var saveNextLine = false

    lines.foreach { line =>
      if (saveNextLine) {
        foundLine = line  // 将下一行保存到 foundLine 变量
        allLines += line
        saveNextLine = false
      }else if (line.contains("🍎")) {
        println(s"符号🍎在该行中存在：$line")
        saveNextLine = true  // 设置标识以保存下一行
      } else {
        println(s"该行中不存在符号🍎：$line")
        allLines += line
        resultArray += ((foundLine,line))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val links1 = scala.collection.mutable.ArrayBuffer[(String, String)]()
    val allLines = scala.collection.mutable.Set[String]()

    readFromTxtByLine(sc,links1,allLines)
    
    val groupedLinks: RDD[(String, List[String])] = sc.parallelize(links1).groupBy(_._1).mapValues(_.map(_._2).toList)
    val allLinesRDD: RDD[String] = sc.parallelize(allLines.toSeq)

    val iters = 8
    var ranks: RDD[(String, Double)]= groupedLinks.mapValues(v => 1.0)
    // ranks.foreach(println)

    for(i<-1 to iters){
      val contribs = groupedLinks.join(ranks).values.flatMap{
        case (urls,rank)=>
        val size = urls.size
        urls.map(url=>(url, rank/size))
      }
      ranks=contribs.reduceByKey(_+_).mapValues(0.15+0.85*_) 
      val missingKeysRdd = allLinesRDD.subtract(ranks.keys)
      val defaultRanksRdd = missingKeysRdd.map(key => (key, 0.15))
      ranks = ranks.union(defaultRanksRdd)
    }
    // 每次迭代结束后，将 ranks 写入 result.txt
    ranks
    .coalesce(1)  // 将结果合并为一个分区
    .map { case (key, value) => s"$key,$value" }
    .saveAsTextFile("hdfs://master:9000/final/pagerank/result")
  }
}