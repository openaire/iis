package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;

public class SparkConfHelper {
    private SparkConfHelper() {
    }

    public static SparkConf withKryo(SparkConf conf) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        return conf;
    }
}
