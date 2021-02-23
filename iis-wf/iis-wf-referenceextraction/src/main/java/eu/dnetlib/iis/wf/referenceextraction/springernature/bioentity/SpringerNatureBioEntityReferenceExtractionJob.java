package eu.dnetlib.iis.wf.referenceextraction.springernature.bioentity;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.SparkConfHelper;
import eu.dnetlib.iis.common.spark.SparkSessionSupport;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.springernature.bioentity.schemas.DocumentToBioEntity;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

//todo: add logging
//todo: add report ?
//todo: add tests
public class SpringerNatureBioEntityReferenceExtractionJob {

    //todo: tara also uses this, consider making this generic
    private static final StructType PIPE_RESULT_SCHEMA = StructType$.MODULE$.apply(
            Collections.singletonList(
                    StructField$.MODULE$.apply("value", DataTypes.StringType, true, Metadata.empty())
            )
    );

    public static void main(String[] args) {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkSessionSupport
                .runWithSparkSession(SparkConfHelper.withKryo(new SparkConf()), params.isSparkSessionShared, spark -> {
                    HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), params.outputDocumentToBioEntity);

                    Dataset<Row> documentText = readDocumentText(spark, params.inputDocumentText);

                    SpringerNatureBioEntityReferenceExtractionPipeExecutionEnvironment environment =
                            new SpringerNatureBioEntityReferenceExtractionPipeExecutionEnvironment(spark.sparkContext(),
                                    params.scriptsDir, params.bioEntityDbFile);
                    Dataset<Row> documentToBioEntity = runReferenceExtraction(spark, documentText, environment);

                    storeInOutput(spark, documentToBioEntity, params.outputDocumentToBioEntity);
                });
    }

    private static Dataset<Row> readDocumentText(SparkSession spark, String inputDocumentText) {
        return new AvroDataFrameSupport(spark).read(inputDocumentText, DocumentText.SCHEMA$);
    }


    //todo: this method is the same as for tara mining so we could make it generic
    public static Dataset<Row> runReferenceExtraction(SparkSession spark,
                                                      Dataset<Row> documentText,
                                                      PipeExecutionEnvironment environment) throws IOException {
        String pipeCommandStr = environment.pipeCommand();
        JavaRDD<Row> piped = documentText
                .toJSON()
                .javaRDD()
                .pipe(pipeCommandStr)
                .map(RowFactory::create);
        StructType schema = (StructType) SchemaConverters.toSqlType(DocumentToBioEntity.SCHEMA$).dataType();
        Dataset<Row> resultAsNullableDF = spark.createDataFrame(piped, PIPE_RESULT_SCHEMA)
                .withColumn("json_struct", from_json(col("value"), schema.asNullable()))
                .select(expr("json_struct.*"));
        return spark.createDataFrame(resultAsNullableDF.javaRDD(), schema);
    }

    private static void storeInOutput(SparkSession spark,
                                      Dataset<Row> documentToBioEntity,
                                      String outputDocumentToBioEntity) {
        new AvroDataFrameSupport(spark).write(documentToBioEntity, outputDocumentToBioEntity, DocumentToBioEntity.SCHEMA$);
    }

    @Parameters(separators = "=")
    public static class JobParameters {

        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;

        @Parameter(names = "-inputDocumentText", required = true)
        private String inputDocumentText;

        @Parameter(names = "-scriptsDir", required = true)
        private String scriptsDir;

        @Parameter(names = "-bioEntityDbFile", required = true)
        private String bioEntityDbFile;

        @Parameter(names = "-outputDocumentToBioEntity", required = true)
        private String outputDocumentToBioEntity;
    }
}
