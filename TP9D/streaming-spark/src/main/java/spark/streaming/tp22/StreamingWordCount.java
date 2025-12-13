package spark.streaming.tp22;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class StreamingWordCount {

    public static void main(String[] args) throws InterruptedException {

        // بدلنا localhost بـ spark-master
        String host = "spark-master";
        int port = 9999;

        if (args.length == 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        SparkConf conf = new SparkConf()
                .setAppName("StreamingWordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        // قراءة النص من الـ socket
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(host, port);

        // تقسيم السطور إلى كلمات
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String line) {
                        return Arrays.asList(line.split("\\s+")).iterator();
                    }
                });

        // (كلمة, 1)
        JavaPairDStream<String, Integer> wordOnes = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) {
                        return new Tuple2<>(word, 1);
                    }
                });

        // تجميع التكرارات
        JavaPairDStream<String, Integer> wordCounts = wordOnes.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                });

        // 👇 هنا نخلي النتيجة تطبع بشكل واضح جدًا
        wordCounts.foreachRDD(rdd -> {
            List<Tuple2<String, Integer>> result = rdd.collect();
            System.out.println("===== NEW BATCH =====");
            if (result.isEmpty()) {
                System.out.println("No data in this batch.");
            } else {
                for (Tuple2<String, Integer> t : result) {
                    System.out.println(t._1 + " -> " + t._2);
                }
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
