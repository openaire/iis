package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession withConfAndKryo(SparkConf conf) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }
}
