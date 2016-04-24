//
// kuromoji Japanese Tokenizer Sample
//   Lucene ver
//

import java.io.StringReader
import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.lucene.analysis.ja.JapaneseTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.ja.tokenattributes.BaseFormAttribute
import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute

object kuromojitest {
  def main(args: Array[String]) {

    //
    val conf = new SparkConf();
    conf.set("spark.app.name", "Kuromoji Test");
    val sc = new SparkContext(conf)

    def Mtokenize(sentence: String) :Seq[String] = {
      val word: ArrayBuffer[String] = new ArrayBuffer[String]()
      lazy val stream = new JapaneseTokenizer (
        new StringReader(sentence), null, false, JapaneseTokenizer.Mode.NORMAL)

      try {
        while(stream.incrementToken()) {
          var charAtt = stream.getAttribute(classOf[CharTermAttribute]).toString
          var baseAtt = stream.getAttribute(classOf[BaseFormAttribute]).getBaseForm
          var partOS  = stream.getAttribute(classOf[PartOfSpeechAttribute]).getPartOfSpeech().split("-")(0)

          (partOS, baseAtt) match {
            case("名詞", _)		=> word += charAtt
            case("動詞", null)		=> word += charAtt
            //case("動詞", baseForm)	=> word += baseForm
            case("動詞", baseForm)	=> word += baseAtt
            case("動詞", _)		=> word += baseAtt
            case(_, _)			=> word += stream.getAttribute(classOf[PartOfSpeechAttribute]).getPartOfSpeech()
          }
        }
      } finally { stream.close }
      word.toSeq
    }
    
    val input = sc.textFile("input.txt")
    val m = input.map(x => Mtokenize(x))
    m.collect().foreach(x => println(x))
  }

}
