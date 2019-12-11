package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class JavaSparkContextFactory {

    private JavaSparkContextFactory() {

    }

    public static JavaSparkContext withConfAndKryo(SparkConf conf) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        return JavaSparkContext
                .fromSparkContext(SparkSession.builder().config(conf).getOrCreate().sparkContext());
    }
}
