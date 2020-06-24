package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.lock.LockManagerUtils;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionIOUtils.*;
import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionUtils.*;

public class CachedTaraReferenceExtractionJob {

    private static final Logger logger = LoggerFactory.getLogger(CachedTaraReferenceExtractionJob.class);

    public enum CacheRecordType {
        documentHashToProject, documentHash
    }

    public static final StructType DOCUMENT_METADATA_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("id", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("title", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("abstract", DataTypes.FloatType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("text", DataTypes.StringType, true, Metadata.empty())
            )
    );

    public static final StructType DOCUMENT_METADATA_WITH_HASH_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("id", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("title", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("abstract", DataTypes.FloatType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("text", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("hashValue", DataTypes.IntegerType, false, Metadata.empty())
            )
    );

    public static void main(String[] args) throws Exception {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        logger.info("inputExtractedDocumentMetadata: {}", params.inputExtractedDocumentMetadataMergedWithOriginal);
        logger.info("inputDocumentText: {}", params.inputDocumentText);
        logger.info("lockManagerFactoryClassName: {}", params.lockManagerFactoryClassName);
        logger.info("cacheRootDir: {}", params.cacheRootDir);
        logger.info("scriptsDir: {}", params.scriptsDir);
        logger.info("projectDbFile: {}", params.projectDbFile);
        logger.info("numberOfEmittedFiles: {}", params.numberOfEmittedFiles);
        logger.info("outputDocumentToProject: {}", params.outputDocumentToProject);

        SparkConf conf = new SparkConf();

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(conf)) {
            clearOutput(spark, params.outputDocumentToProject);

            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark);

            Dataset<Row> inputExtractedDocumentMetadataMergedWithOriginalDF = avroDataFrameSupport.read(
                    params.inputExtractedDocumentMetadataMergedWithOriginal,
                    ExtractedDocumentMetadataMergedWithOriginal.SCHEMA$);
            Dataset<Row> inputDocumentTextDF = avroDataFrameSupport.read(params.inputDocumentText,
                    DocumentText.SCHEMA$);
            Dataset<Row> documentMetadataDF = buildDocumentMetadata(inputDocumentTextDF,
                    inputExtractedDocumentMetadataMergedWithOriginalDF);
            Dataset<Row> documentMetadataWithHashDF = buildDocumentMetadataWithHash(documentMetadataDF)
                    .cache();

            String existingCacheId = cacheManager.getExistingCacheId(spark.sparkContext().hadoopConfiguration(),
                    params.cacheRootDir);
            Dataset<Row> documentHashToProjectFromCacheDF = readDocumentHashToProjectFromCacheOrEmpty(spark,
                    params.cacheRootDir,
                    existingCacheId);
            Dataset<Row> documentHashFromCacheDF = readDocumentHashFromCacheOrEmpty(spark,
                    params.cacheRootDir,
                    existingCacheId)
                    .cache();

            Dataset<Row> documentMetadataToBeProcessedDF = documentMetadataToBeProcessed(documentMetadataWithHashDF,
                    documentHashFromCacheDF);

            TaraPipeExecutionEnvironment environment = new TaraPipeExecutionEnvironment(spark.sparkContext(),
                    params.scriptsDir, params.projectDbFile);
            Dataset<Row> documentToProjectDF = runReferenceExtraction(spark,
                    documentMetadataToBeProcessedDF,
                    environment);

            Dataset<Row> documentHashToProjectToBeCachedDF = documentHashToProjectToBeCached(spark,
                    documentToProjectDF,
                    documentHashToProjectFromCacheDF,
                    documentMetadataWithHashDF)
                    .cache();
            Dataset<Row> documentHashToBeCachedDF = documentHashToBeCached(spark,
                    documentHashFromCacheDF,
                    documentMetadataWithHashDF);
            LockManager lockManager = LockManagerUtils.instantiateLockManager(params.lockManagerFactoryClassName,
                    spark.sparkContext().hadoopConfiguration());
            storeInCache(spark,
                    documentHashToProjectToBeCachedDF,
                    documentHashToBeCachedDF,
                    params.cacheRootDir,
                    lockManager,
                    cacheManager,
                    params.numberOfEmittedFiles);

            Dataset<Row> documentToProjectToOutputDF = documentToProjectToOutput(spark,
                    documentHashToProjectToBeCachedDF,
                    documentMetadataWithHashDF);
            storeInOutput(spark,
                    documentToProjectToOutputDF,
                    params.outputDocumentToProject);
        }
    }

    @Parameters(separators = "=")
    public static class JobParameters {

        @Parameter(names = "-inputExtractedDocumentMetadataMergedWithOriginal", required = true)
        private String inputExtractedDocumentMetadataMergedWithOriginal;

        @Parameter(names = "-inputDocumentText", required = true)
        private String inputDocumentText;

        @Parameter(names = "-lockManagerFactoryClassName", required = true)
        private String lockManagerFactoryClassName;

        @Parameter(names = "-cacheRootDir", required = true)
        private String cacheRootDir;

        @Parameter(names = "-scriptsDir", required = true)
        private String scriptsDir;

        @Parameter(names = "-projectDbFile", required = true)
        private String projectDbFile;

        @Parameter(names = "-numberOfEmittedFiles", required = true)
        private int numberOfEmittedFiles;

        @Parameter(names = "-outputDocumentToProject", required = true)
        private String outputDocumentToProject;
    }
}
