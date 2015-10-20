package eu.dnetlib.iis.core.examples.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.google.common.base.Preconditions;

/**
 * 
 * Simple service counting words in the specified file and writing the results to the given directory
 * 
 * @author Łukasz Dumiszewski
 */

public class FileWordCounter {
  
    public static void main(String[] args) {
        
        Preconditions.checkArgument(args.length > 1, "You must enter the input file and output directory");
        
        SparkConf conf = new SparkConf();
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> lines = sc.textFile(args[0], 2);
         
        JavaPairRDD<String, Integer> words = lines.flatMap(line->Arrays.asList(line.split("\\W+"))).mapToPair(word->new Tuple2<String, Integer>(word, 1));
        
        JavaPairRDD<String, Integer> wordCounts = words.reduceByKey((x, y) -> x+y);
        
        
        wordCounts.saveAsTextFile(args[1]);
        
        sc.close(); 
    }
    
}