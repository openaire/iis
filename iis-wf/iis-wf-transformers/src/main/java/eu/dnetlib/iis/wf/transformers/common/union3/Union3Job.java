package eu.dnetlib.iis.wf.transformers.common.union3;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.javamapreduce.hack.AvroSchemaGenerator;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;

/**
 * Generic Spark job that unions three Avro datasets sharing the same schema.
 */
public class Union3Job {

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (SparkSession sparkSession = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sparkSession.sparkContext().hadoopConfiguration(), params.output);

            Schema schema = AvroSchemaGenerator.getSchema(params.schemaClass);
            AvroDataFrameReader reader = new AvroDataFrameReader(sparkSession);

            Dataset<Row> a = reader.read(params.inputA, schema);
            Dataset<Row> b = reader.read(params.inputB, schema);
            Dataset<Row> c = reader.read(params.inputC, schema);

            new AvroDataFrameWriter(a.union(b).union(c)).write(params.output, schema);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputA", required = true)
        private String inputA;

        @Parameter(names = "-inputB", required = true)
        private String inputB;

        @Parameter(names = "-inputC", required = true)
        private String inputC;

        @Parameter(names = "-output", required = true)
        private String output;

        @Parameter(names = "-schemaClass", required = true)
        private String schemaClass;
    }
}
