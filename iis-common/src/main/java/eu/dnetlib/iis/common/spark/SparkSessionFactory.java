package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession withConfAndKryo(SparkConf conf) {
        return SparkSession.builder()
                .config(SparkConfHelper.withKryo(conf))
                .getOrCreate();
    }
}
