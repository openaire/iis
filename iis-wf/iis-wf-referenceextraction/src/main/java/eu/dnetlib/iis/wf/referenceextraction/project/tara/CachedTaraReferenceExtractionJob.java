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

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionIOUtils.*;
import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionUtils.*;

public class CachedTaraReferenceExtractionJob {

    public enum CacheRecordType {
        documentHashToProject, documentHash
    }

    public static final StructType DOCUMENT_METADATA_BY_ID_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("id", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("title", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("abstract", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("text", DataTypes.StringType, false, Metadata.empty())
            )
    );

    public static final StructType DOCUMENT_METADATA_BY_HASH_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("hashValue", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("title", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("abstract", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("text", DataTypes.StringType, false, Metadata.empty())
            )
    );

    public static final StructType DOCUMENT_METADATA_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("id", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("title", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("abstract", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("text", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("hashValue", DataTypes.StringType, false, Metadata.empty())
            )
    );

    public static void main(String[] args) throws Exception {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        Path cacheRoot = new Path(params.cacheRootDir);

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            clearOutput(spark, params.outputDocumentToProject);

            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark);

            Dataset<Row> inputExtractedDocumentMetadataMergedWithOriginalDF = avroDataFrameSupport.read(
                    params.inputExtractedDocumentMetadataMergedWithOriginal,
                    ExtractedDocumentMetadataMergedWithOriginal.SCHEMA$);
            Dataset<Row> inputDocumentTextDF = avroDataFrameSupport.read(params.inputDocumentText,
                    DocumentText.SCHEMA$);
            Dataset<Row> documentMetadataByIdDF = buildDocumentMetadataById(inputDocumentTextDF,
                    inputExtractedDocumentMetadataMergedWithOriginalDF);
            Dataset<Row> documentMetadataDF = buildDocumentMetadata(documentMetadataByIdDF);

            String existingCacheId = cacheManager.getExistingCacheId(spark.sparkContext().hadoopConfiguration(),
                    cacheRoot);
            Dataset<Row> documentHashToProjectFromCacheDF = readDocumentHashToProjectFromCacheOrEmpty(spark, cacheRoot,
                    existingCacheId);
            Dataset<Row> documentHashFromCacheDF = readDocumentHashFromCacheOrEmpty(spark, cacheRoot, existingCacheId);

            Dataset<Row> documentMetadataByHashToBeProcessedDF = documentMetadataByHashToBeProcessed(documentMetadataDF,
                    documentHashFromCacheDF);

            TaraPipeExecutionEnvironment environment = new TaraPipeExecutionEnvironment(spark.sparkContext(),
                    params.scriptsDir, params.projectDbFile);
            Dataset<Row> documentHashToProjectDF = runReferenceExtraction(spark,
                    documentMetadataByHashToBeProcessedDF,
                    environment)
                    .cache();

            Dataset<Row> documentHashToProjectToBeCachedDF = documentHashToProjectToBeCached(spark,
                    documentHashToProjectDF,
                    documentHashToProjectFromCacheDF);
            Dataset<Row> documentHashToBeCachedDF = documentHashToBeCached(spark,
                    documentHashFromCacheDF,
                    documentMetadataDF);
            LockManager lockManager = LockManagerUtils.instantiateLockManager(params.lockManagerFactoryClassName,
                    spark.sparkContext().hadoopConfiguration());
            storeInCache(spark,
                    documentHashToProjectToBeCachedDF,
                    documentHashToBeCachedDF,
                    cacheRoot,
                    lockManager,
                    cacheManager);

            Dataset<Row> documentToProjectToOutputDF = documentToProjectToOutput(spark,
                    documentHashToProjectToBeCachedDF,
                    documentMetadataDF);
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

        @Parameter(names = "-outputDocumentToProject", required = true)
        private String outputDocumentToProject;
    }
}
