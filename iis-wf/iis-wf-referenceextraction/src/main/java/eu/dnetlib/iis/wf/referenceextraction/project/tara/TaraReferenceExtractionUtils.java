package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

public class TaraReferenceExtractionUtils {

    private static final Logger logger = LoggerFactory.getLogger(TaraReferenceExtractionUtils.class);

    private static final StructType PIPE_RESULT_SCHEMA = StructType$.MODULE$.apply(
            Collections.singletonList(
                    StructField$.MODULE$.apply("value", DataTypes.StringType, true, Metadata.empty())
            )
    );

    public static Dataset<Row> buildDocumentMetadata(Dataset<Row> documentTextDF,
                                                     Dataset<Row> extractedDocumentMetadataMergedWithOriginalDF) {
        logger.info("Building document metadata for input data.");
        Column joinExprs = documentTextDF.col("id").equalTo(
                extractedDocumentMetadataMergedWithOriginalDF.col("id"));
        return documentTextDF
                .join(extractedDocumentMetadataMergedWithOriginalDF, joinExprs, "left_outer")
                .select(
                        documentTextDF.col("id"),
                        extractedDocumentMetadataMergedWithOriginalDF.col("title"),
                        extractedDocumentMetadataMergedWithOriginalDF.col("abstract"),
                        documentTextDF.col("text")
                );
    }

    public static Dataset<Row> runReferenceExtraction(SparkSession spark,
                                                      Dataset<Row> documentMetadataDF,
                                                      PipeExecutionEnvironment environment) throws IOException {
        logger.info("Running reference extraction for input document metadata.");
        String pipeCommandStr = environment.pipeCommand();
        JavaRDD<Row> piped = documentMetadataDF
                .toJSON()
                .javaRDD()
                .pipe(pipeCommandStr)
                .map(RowFactory::create);
        StructType schema = (StructType) SchemaConverters.toSqlType(DocumentToProject.SCHEMA$).dataType();
        Dataset<Row> resultAsNullableDF = spark.createDataFrame(piped, PIPE_RESULT_SCHEMA)
                .withColumn("json_struct", from_json(col("value"), schema.asNullable()))
                .select(expr("json_struct.*"));
        return spark.createDataFrame(resultAsNullableDF.javaRDD(), schema);
    }

}
