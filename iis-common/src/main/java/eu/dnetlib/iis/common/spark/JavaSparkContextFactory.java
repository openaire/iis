package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class JavaSparkContextFactory {

    private JavaSparkContextFactory() {
    }

    public static JavaSparkContext withConfAndKryo(SparkConf conf) {
        return JavaSparkContext
                .fromSparkContext(SparkSession.builder()
                        .config(SparkConfHelper.withKryo(conf))
                        .getOrCreate()
                        .sparkContext());
    }
}
