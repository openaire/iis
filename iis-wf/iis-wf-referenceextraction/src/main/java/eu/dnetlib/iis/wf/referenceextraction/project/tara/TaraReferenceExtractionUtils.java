package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHash;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHashToProject;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

public class TaraReferenceExtractionUtils {

    private static final Logger logger = LoggerFactory.getLogger(TaraReferenceExtractionUtils.class);

    private static final String SEP = "|";

    private static final StructType PIPE_RESULT_SCHEMA = StructType$.MODULE$.apply(
            Collections.singletonList(
                    StructField$.MODULE$.apply("value", DataTypes.StringType, true, Metadata.empty())
            )
    );

    private static final StructType REFERENCE_EXTRACTION_RESULT_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("documentId", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("projectId", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("confidenceLevel", DataTypes.FloatType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("textsnippet", DataTypes.StringType, true, Metadata.empty())
            )
    );

    public static Dataset<Row> buildDocumentMetadataById(Dataset<Row> documentTextDF,
                                                         Dataset<Row> extractedDocumentMetadataMergedWithOriginalDF) {
        logger.info("Building document metadata by id for input data.");
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

    public static Dataset<Row> buildDocumentMetadata(Dataset<Row> documentMetadataByIdDF) {
        return buildDocumentMetadata(documentMetadataByIdDF, new DocumentMetadataHashColumnCreator());
    }

    public static Dataset<Row> buildDocumentMetadata(Dataset<Row> documentMetadataByIdDF,
                                                     DocumentMetadataHashColumnCreator hashColumnCreator) {
        logger.info("Building document metadata with hash column.");
        return documentMetadataByIdDF
                .withColumn("hashValue",
                        hashColumnCreator.hashCol("title", "abstract", "text"));
    }

    public static Dataset<Row> documentMetadataByHashToBeProcessed(Dataset<Row> documentMetadataDF,
                                                                   Dataset<Row> documentHashFromCacheDF) {
        logger.info("Finding document metadata to be processed.");
        Column joinExprs = documentMetadataDF.col("hashValue").equalTo(
                documentHashFromCacheDF.col("hashValue"));
        return documentMetadataDF
                .join(documentHashFromCacheDF, joinExprs, "left_anti")
                .select(
                        col("hashValue"),
                        col("title"),
                        col("abstract"),
                        col("text")
                )
                .distinct();
    }

    public static Dataset<Row> runReferenceExtraction(SparkSession spark,
                                                      Dataset<Row> documentMetadataByHashDF,
                                                      PipeExecutionEnvironment environment) throws IOException {
        logger.info("Running reference extraction for input document metadata.");
        String pipeCommandStr = environment.pipeCommand();
        JavaRDD<Row> piped = documentMetadataByHashDF
                .withColumnRenamed("hashValue", "id")
                .toJSON()
                .javaRDD()
                .pipe(pipeCommandStr)
                .map(RowFactory::create);
        return spark.createDataFrame(piped, PIPE_RESULT_SCHEMA)
                .withColumn("json_struct", from_json(col("value"), REFERENCE_EXTRACTION_RESULT_SCHEMA))
                .select(expr("json_struct.*"))
                .withColumnRenamed("documentId", "hashValue");
    }

    public static Dataset<Row> documentHashToProjectToBeCached(SparkSession spark,
                                                               Dataset<Row> documentHashToProjectDF,
                                                               Dataset<Row> documentHashToProjectFromCacheDF) {
        logger.info("Finding reference extraction results to be cached.");
        Dataset<Row> toBeCached = documentHashToProjectFromCacheDF.union(documentHashToProjectDF);
        return dataFrameWithSchema(spark, toBeCached, DocumentHashToProject.SCHEMA$);
    }

    public static Dataset<Row> documentHashToBeCached(SparkSession spark,
                                                      Dataset<Row> documentHashFromCacheDF,
                                                      Dataset<Row> documentMetadataDF) {
        logger.info("Finding processed documents to be cached.");
        Dataset<Row> documentHashDF = documentMetadataDF
                .select("hashValue");
        Dataset<Row> toBeCached = documentHashFromCacheDF.union(documentHashDF).distinct();
        return dataFrameWithSchema(spark, toBeCached, DocumentHash.SCHEMA$);
    }

    public static Dataset<Row> documentToProjectToOutput(SparkSession spark,
                                                         Dataset<Row> documentHashToProjectDF,
                                                         Dataset<Row> documentMetadataDF) {
        logger.info("Finding reference extraction results to be saved to output.");
        Column joinExprs = documentHashToProjectDF.col("hashValue").equalTo(
                documentMetadataDF.col("hashValue"));
        Dataset<Row> toBeOutput = documentHashToProjectDF
                .join(documentMetadataDF, joinExprs, "inner")
                .select(
                        col("id").as("documentId"),
                        col("projectId"),
                        col("confidenceLevel"),
                        col("textsnippet")
                );
        return dataFrameWithSchema(spark, toBeOutput, DocumentToProject.SCHEMA$);
    }

    private static Dataset<Row> dataFrameWithSchema(SparkSession spark,
                                                    Dataset<Row> df,
                                                    Schema avroSchema) {
        return spark.createDataFrame(df.javaRDD(), (StructType) SchemaConverters.toSqlType(avroSchema).dataType());
    }

    public static class DocumentMetadataHashColumnCreator {
        public Column hashCol(String titleColName, String abstractColName, String textColName) {
            return sha1(concat_ws(SEP, col(titleColName), col(abstractColName), col(textColName)));
        }
    }
}
