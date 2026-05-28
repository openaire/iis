package eu.dnetlib.iis.wf.transformers.idreplacer;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.javamapreduce.hack.AvroSchemaGenerator;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;

/**
 * Generic Spark job that replaces one or two id fields in an Avro dataset using an
 * IdentifierMapping lookup. Records with no mapping entry retain a null id value
 * in the replaced field (left-join semantics, matching PIG OUTER JOIN behaviour).
 */
public class IdReplacerJob {

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (SparkSession sparkSession = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sparkSession.sparkContext().hadoopConfiguration(), params.output);

            Schema schema = AvroSchemaGenerator.getSchema(params.schemaClass);
            AvroDataFrameReader reader = new AvroDataFrameReader(sparkSession);

            Dataset<Row> main = reader.read(params.input, schema);
            Dataset<Row> mapping = reader.read(params.inputIdMapping, IdentifierMapping.SCHEMA$);

            Dataset<Row> result = replaceId(main, schema, mapping, params.idFieldToReplace1);

            if (params.idFieldToReplace2 != null && !params.idFieldToReplace2.isEmpty()) {
                result = replaceId(result, schema, mapping, params.idFieldToReplace2);
            }

            new AvroDataFrameWriter(result).write(params.output, schema);
        }
    }

    //------------------------ PRIVATE --------------------------

    /**
     * Performs a left join of {@code data} with {@code mapping} on {@code fieldToReplace},
     * replacing that field's value with {@code newId} from the mapping. Records with no
     * mapping entry retain their original field value (matching IdReplacerUDF behaviour).
     */
    private static Dataset<Row> replaceId(Dataset<Row> data, Schema schema,
            Dataset<Row> mapping, String fieldToReplace) {
        Dataset<Row> renamedMapping = mapping.select(
                col("originalId").alias("_orig"),
                col("newId").alias("_new"));

        Dataset<Row> joined = data.join(renamedMapping,
                data.col(fieldToReplace).equalTo(renamedMapping.col("_orig")), "left");

        Column[] selectCols = schema.getFields().stream()
                .map(f -> f.name().equals(fieldToReplace)
                        ? coalesce(col("_new"), data.col(f.name())).alias(f.name())
                        : data.col(f.name()))
                .toArray(Column[]::new);

        return joined.select(selectCols);
    }

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-input", required = true)
        private String input;

        @Parameter(names = "-inputIdMapping", required = true)
        private String inputIdMapping;

        @Parameter(names = "-output", required = true)
        private String output;

        @Parameter(names = "-schemaClass", required = true)
        private String schemaClass;

        @Parameter(names = "-idFieldToReplace1", required = true)
        private String idFieldToReplace1;

        @Parameter(names = "-idFieldToReplace2", required = false)
        private String idFieldToReplace2;
    }
}
