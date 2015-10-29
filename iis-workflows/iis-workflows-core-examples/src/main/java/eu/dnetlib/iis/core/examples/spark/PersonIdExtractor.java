package eu.dnetlib.iis.core.examples.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.clearspring.analytics.util.Preconditions;

/**
 * Service for extracting ids from person saved in comma separated file and
 * storing results in the given directory
 * 
 * @author madryk
 *
 */
public class PersonIdExtractor {

    public static void main(String[] args) {
        
        Preconditions.checkArgument(args.length == 2, "You must provide input file and output directory");
        
        
        SparkConf conf = new SparkConf();
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        
        JavaRDD<String> personRDD = sc.textFile(args[0]);
        
        JavaRDD<String> personIdRDD = personRDD.map(person -> StringUtils.split(person, ",")[0]);
        
        
        personIdRDD.coalesce(1).saveAsTextFile(args[1]);
        
        sc.close();
    }
    
}
