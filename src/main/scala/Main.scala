import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkTextAnalysis {
  def main(args: Array[String]): Unit = {

    // Создаем SparkContext
    val NODES = 4
    val spark = SparkSession.builder.appName("Simple Application").master(s"local[$NODES]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // Читаем текст из файла
    val textRDD: RDD[String] = sc.textFile("C:\\bigdata\\turgenev.txt")

    // Загружаем список стоп-слов из файла
    val stopWords: Set[String] = sc.textFile("C:\\bigdata\\RussianStopWords.txt").collect().toSet

    // 1. Очистка текста (удаление стоп-слов и другие преобразования)
    val cleanedTextRDD: RDD[String] = textRDD
      .flatMap(line => "\\p{L}+".r.findAllIn(line.toLowerCase)) // Используем Unicode-совместимое регулярное выражение
      .filter(word => !stopWords.contains(word))
      .map(word => word.replaceAll("[^а-яА-Я]", "")) // Удаляем не-буквенные символы
      .filter(word => word.length > 0)

    // 2. Word Count
    val wordCountRDD: RDD[(String, Int)] = cleanedTextRDD
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // 3. Вывод Топ-50 наиболее и наименее употребляемых слов
    val Most50Words: Array[(String, Int)] = wordCountRDD
      .sortBy(_._2, ascending = false)
      .take(50)

    val Least50Words: Array[(String, Int)] = wordCountRDD
      .sortBy(_._2)
      .take(50)

    println("Most 50 common words:")
    Most50Words.foreach(println)

    println("Least 50 common words:")
    Least50Words.foreach(println)
    // 4 Стемминг
    val stemmed = cleanedTextRDD.filter(x => x.nonEmpty).flatMap(line => line.split(" "))
      .map(word => (RussianStemmer.stem(word)))
    val exp = ""
    var filtered = cleanedTextRDD.filter(f => f.startsWith(exp)).distinct()
    var ee = filtered.collect().mkString(",")
    val counts_stem = stemmed.filter(x => x.nonEmpty).flatMap(line =>
      line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // 5. Вывод Топ-50 наиболее и наименее употребляемых слов после стемминга
    val sorted_stem = counts_stem.map({ case (v1, v2) => (v2, v1) })
    val stem_asc = sorted_stem.sortByKey(ascending = false).take(50)
    val top_count = stem_asc.map({ case (v1, v2) => v1 })
    val wordf = stem_asc.map({ case (v1, v2) => (v2, v1) }).map({ case (v1, v2)
    => v1
    })
    var iterator = 0
    println("Top 50 most common words by Stemming:")
    wordf.foreach(a => {
      filtered = cleanedTextRDD.filter(f => f.startsWith(a)).distinct()
      ee = filtered.collect().mkString(",")
      println(top_count(iterator), a, ee)
      iterator = iterator + 1
    })
    val stem_desc = sorted_stem.sortByKey(ascending = true).take(50)
    val low_count = stem_desc.map({ case (v1, v2) => v1 })
    val wordflow = stem_desc.map({ case (v1, v2) => (v2, v1) }).map({ case (v1,
    v2) => v1
    })
    iterator = 0
    println("Top 50 least words by stemming")
    wordflow.foreach(a => {
      val strr = cleanedTextRDD.filter(f => f.startsWith(a)).first()
      println(low_count(iterator), a, strr)
      iterator = iterator + 1
    })
    // Закрываем SparkContext
    sc.stop()
  }
}
