import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

class WordProb (word1:String, word2:String, prob:Float) extends java.io.Serializable {
    var w1: String = word1
    var w2: String = word2
    var p: Float = prob
}

class Sentence (dId:Long, word1:String, word2:String) extends java.io.Serializable {
    var sId: Long = dId
    var w1: String = word1
    var w2: String = word2
}

object IbmModelDriver {

    def parseSegmentation(line: String): Tuple3[Long, Array[String], Array[String]] = {
        val arr = line.split("\t");
        val sourceWords = arr(0).split("\\s+");
        val targetWords = arr(1).split("\\s+");
        var sId = scala.util.Random.nextLong; 
        sId = if(sId > 0) sId else -1 * sId;
        return (sId, sourceWords, targetWords)
    }
    
    def genCombination(arr1: Array[String], arr2: Array[String]): ArrayBuffer[WordProb] = {
        var combinations = ArrayBuffer[WordProb]()
        for (w1 <- arr1) {
            for (w2 <- arr2) {
                combinations += new WordProb(w1, w2, 0.0f)
            }
        }
        return combinations
    }

    def genSentenceCombination(sId: Long, arr1: Array[String], arr2: Array[String]): ArrayBuffer[Sentence] = {
        var combinations = ArrayBuffer[Sentence]()
        for (w1 <- arr1) {
            for (w2 <- arr2) {
                combinations += new Sentence(sId, w1, w2)
            }
        }
        return combinations
    }

    def normalizeProb(p_e_f: Iterable[WordProb]): Iterable[WordProb] = {
        var total = 0.0f
        for (e <- p_e_f) {
            total += e.p
        }
        total = if (total <= 0) 1 else total
        for (e <- p_e_f) {
            e.p = e.p / total
        }

        return p_e_f
    }

    def main(args: Array[String]) {
        println("### INPUT = " + args(0))
        println("### OUTPUT = " + args(1))

        val conf = new SparkConf().setAppName("Ibm model")
        val sc = new SparkContext(conf)

        val f = sc.textFile(args(0)) 

        val sentence = f.map(line => parseSegmentation(line))
        val sentencePairs = sentence.flatMap(s => genSentenceCombination(s._1, s._2, s._3))
        val uniq_e_f = sentence.flatMap(elements => genCombination(elements._2, elements._3)).distinct()
        uniq_e_f.saveAsTextFile(args(1))
//        val uniq_f = uniq_e_f.map(pair => pair.w1).distinct().map( w => (w, 0.0f))
//        val p_e_f = uniq_e_f.map(pair => {pair.p = scala.util.Random.nextFloat; (pair.w1, pair)}).groupByKey().flatMap(e => normalizeProb(e._2)).map(e_f => ((e_f.w1, e_f.w2), e_f))
//        for (i <- 0 to 5) {
//            val s_e_f = sentencePairs.map(pair => ((pair.w1, pair.w2), pair)).join(p_e_f).(e => (e._2._1.sId, e._2._2)) )
//            val tmp = s_e_f.groupBy()
//            val total_f = 
//
//        }
//        p_e_f.saveAsTextFile(args(1))

        sc.stop()
    }   
}
