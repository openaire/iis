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
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

public class TaraReferenceExtractionUtils {

    private static final Logger logger = LoggerFactory.getLogger(TaraReferenceExtractionUtils.class);

    public static Dataset<Row> buildDocumentMetadata(Dataset<Row> documentTextDF,
                                                     Dataset<Row> extractedDocumentMetadataMergedWithOriginalDF) {
        logger.info("Building document metadata for input data.");
        Column joinExprs = documentTextDF.col("id").equalTo(
                extractedDocumentMetadataMergedWithOriginalDF.col("id"));
        return documentTextDF
                .join(extractedDocumentMetadataMergedWithOriginalDF, joinExprs, "left_outer")
                .select(
                        documentTextDF.col("id"),
                        documentTextDF.col("text"),
                        extractedDocumentMetadataMergedWithOriginalDF.col("title"),
                        extractedDocumentMetadataMergedWithOriginalDF.col("abstract")
                );
    }

    public static Dataset<Row> buildDocumentMetadataWithHash(Dataset<Row> documentMetadataDF) {
        return buildDocumentMetadataWithHash(documentMetadataDF, new DocumentMetadataHashColumnCreator());
    }

    public static Dataset<Row> buildDocumentMetadataWithHash(Dataset<Row> documentMetadataDF,
                                                             DocumentMetadataHashColumnCreator hashColumnCreator) {
        logger.info("Building document metadata with hash column.");
        return documentMetadataDF
                .withColumn("hashValue",
                        hashColumnCreator.hashCol("title", "abstract", "text"));
    }

    public static Dataset<Row> documentMetadataToBeProcessed(Dataset<Row> documentMetadataWithHashDF,
                                                             Dataset<Row> documentHashFromCacheDF) {
        logger.info("Finding document metadata to be processed.");
        Column joinExprs = documentMetadataWithHashDF.col("hashValue").equalTo(
                documentHashFromCacheDF.col("hashValue"));
        return documentMetadataWithHashDF
                .join(documentHashFromCacheDF, joinExprs, "left_anti")
                .select(
                        col("id"),
                        col("title"),
                        col("abstract"),
                        col("text")
                );
    }

    private static final StructType PIPE_RESULT_SCHEMA = StructType$.MODULE$.apply(
            Collections.singletonList(
                    StructField$.MODULE$.apply("value", DataTypes.StringType, true, Metadata.empty())
            )
    );

    public static Dataset<Row> runReferenceExtraction(SparkSession spark,
                                                      Dataset<Row> documentMetadataDF,
                                                      PipeExecutionEnvironment environment) throws IOException {
        logger.info("Running reference extraction for input document metadata.");
        String pipeCommandStr = environment.pipeCommand();
        JavaRDD<Row> piped = documentMetadataDF.toJSON().javaRDD().pipe(pipeCommandStr).map(RowFactory::create);
        return spark.createDataFrame(piped, PIPE_RESULT_SCHEMA)
                .withColumn("json_struct", from_json(col("value"),
                        (StructType) SchemaConverters.toSqlType(DocumentToProject.SCHEMA$).dataType()))
                .select(expr("json_struct.*"));
    }

    public static Dataset<Row> documentHashToProjectToBeCached(SparkSession spark,
                                                               Dataset<Row> documentToProjectDF,
                                                               Dataset<Row> documentHashToProjectFromCacheDF,
                                                               Dataset<Row> documentMetadataWithHashDF) {
        logger.info("Finding reference extraction results to be cached.");
        Column joinExprs = documentToProjectDF.col("documentId").equalTo(
                documentMetadataWithHashDF.col("id"));
        Dataset<Row> documentHashToProjectDF = documentToProjectDF
                .join(documentMetadataWithHashDF, joinExprs)
                .select(
                        col("hashValue"),
                        col("projectId"),
                        col("confidenceLevel"),
                        col("textsnippet")
                );
        Dataset<Row> toBeCached = documentHashToProjectFromCacheDF.union(documentHashToProjectDF);
        return dataFrameWithSchema(spark, toBeCached, DocumentHashToProject.SCHEMA$);
    }

    public static Dataset<Row> documentHashToBeCached(SparkSession spark,
                                                      Dataset<Row> documentHashFromCacheDF,
                                                      Dataset<Row> documentMetadataWithHashDF) {
        logger.info("Finding processed documents to be cached.");
        Dataset<Row> documentHashDF = documentMetadataWithHashDF
                .select("hashValue");
        Dataset<Row> toBeCached = documentHashFromCacheDF.union(documentHashDF).distinct();
        return dataFrameWithSchema(spark, toBeCached, DocumentHash.SCHEMA$);
    }

    public static Dataset<Row> documentToProjectToOutput(SparkSession spark,
                                                         Dataset<Row> documentHashToProjectDF,
                                                         Dataset<Row> documentMetadataWithHashDF) {
        logger.info("Finding reference extraction results to be saved to output.");
        Column joinExprs = documentHashToProjectDF.col("hashValue").equalTo(
                documentMetadataWithHashDF.col("hashValue"));
        Dataset<Row> toBeOutput = documentHashToProjectDF
                .join(documentMetadataWithHashDF, joinExprs)
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
            return hash(col(titleColName), col(abstractColName), col(textColName));
        }
    }
}
