package eu.dnetlib.iis.wf.transformers.common.uniondedup;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.javamapreduce.hack.AvroSchemaGenerator;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;

/**
 * Generic Spark job that unions two Avro datasets sharing the same schema and deduplicates
 * the result by two grouping fields, keeping only the record with the highest confidenceLevel
 * per group.
 */
public class UnionDedupJob {

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

            WindowSpec window = Window
                    .partitionBy(col(params.groupByField1), col(params.groupByField2))
                    .orderBy(col("confidenceLevel").desc());

            Dataset<Row> unioned = a.union(b);
            Dataset<Row> output = unioned
                    .withColumn("_rn", row_number().over(window))
                    .filter(col("_rn").equalTo(1))
                    .drop("_rn");

            new AvroDataFrameWriter(output).write(params.output, schema);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputA", required = true)
        private String inputA;

        @Parameter(names = "-inputB", required = true)
        private String inputB;

        @Parameter(names = "-output", required = true)
        private String output;

        @Parameter(names = "-schemaClass", required = true)
        private String schemaClass;

        @Parameter(names = "-groupByField1", required = true)
        private String groupByField1;

        @Parameter(names = "-groupByField2", required = true)
        private String groupByField2;
    }
}
