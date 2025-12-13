package spark.batch.tp21;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountTask {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: WordCountTask <input-file> <output-dir>");
            System.exit(1);
        }

        String inputFilePath = args[0];
        String outputDir = args[1];

        // إعداد Spark في وضع local على جهازك
        SparkConf conf = new SparkConf()
                .setAppName(WordCountTask.class.getName())
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // قراءة الملف النصي
        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        // WordCount
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        // حفظ النتيجة في مجلد output
        counts.saveAsTextFile(outputDir);

        sc.close();
    }
}
