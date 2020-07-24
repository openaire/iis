package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHash;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHashToProject;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class TaraReferenceExtractionIOUtils {

    private static final Logger logger = LoggerFactory.getLogger(TaraReferenceExtractionIOUtils.class);

    private TaraReferenceExtractionIOUtils() {
    }

    public static void clearOutput(SparkSession spark,
                                   String outputDocumentToProject) throws IOException {
        clearOutput(outputDocumentToProject, new OutputCleaner(spark));
    }

    public static void clearOutput(String outputDocumentToProject,
                                   OutputCleaner cleaner) throws IOException {
        logger.info("Clearing output location {}.", outputDocumentToProject);
        cleaner.clearOutput(outputDocumentToProject);
    }

    public static Dataset<Row> readDocumentHashToProjectFromCacheOrEmpty(SparkSession spark,
                                                                         String cacheRootDir,
                                                                         String existingCacheId) {
        return readDocumentHashToProjectFromCacheOrEmpty(
                spark,
                cacheRootDir,
                existingCacheId,
                new AvroDataStoreReader(spark));
    }

    public static Dataset<Row> readDocumentHashToProjectFromCacheOrEmpty(SparkSession spark,
                                                                         String cacheRootDir,
                                                                         String existingCacheId,
                                                                         AvroDataStoreReader reader) {
        if (!cacheExist(existingCacheId)) {
            logger.info("Existing cache id is {}. Returning empty datastore.", existingCacheId);
            return new AvroDataFrameSupport(spark).createDataFrame(Collections.emptyList(),
                    DocumentHashToProject.SCHEMA$);
        }
        Path existingCache = new Path(cacheRootDir, existingCacheId);
        logger.info("Existing cache id is {}. Reading datastore from cache path {}.", existingCacheId, existingCache);
        Path path = new Path(existingCache, CachedTaraReferenceExtractionJob.CacheRecordType.documentHashToProject.name());
        return reader.read(path.toString(), DocumentHashToProject.SCHEMA$);
    }

    public static Dataset<Row> readDocumentHashFromCacheOrEmpty(SparkSession spark,
                                                                String cacheRootDir,
                                                                String existingCacheId) {
        return readDocumentHashFromCacheOrEmpty(spark,
                cacheRootDir,
                existingCacheId,
                new AvroDataStoreReader(spark));
    }

    public static Dataset<Row> readDocumentHashFromCacheOrEmpty(SparkSession spark,
                                                                String cacheRootDir,
                                                                String existingCacheId,
                                                                AvroDataStoreReader reader) {
        if (!cacheExist(existingCacheId)) {
            return new AvroDataFrameSupport(spark).createDataFrame(Collections.emptyList(), DocumentHash.SCHEMA$);
        }
        Path existingCache = new Path(cacheRootDir, existingCacheId);
        logger.info("Existing cache id is {}. Reading datastore from cache path {}.", existingCacheId, existingCache);
        Path path = new Path(existingCache, CachedTaraReferenceExtractionJob.CacheRecordType.documentHash.name());
        return reader.read(path.toString(), DocumentHash.SCHEMA$);
    }

    private static Boolean cacheExist(String existingCacheId) {
        return !CacheMetadataManagingProcess.UNDEFINED.equals(existingCacheId);
    }

    public static void storeInCache(SparkSession spark,
                                    Dataset<Row> documentHashToProjectToBeCachedDF,
                                    Dataset<Row> documentHashToBeCachedDF,
                                    String cacheRootDir,
                                    LockManager lockManager,
                                    CacheMetadataManagingProcess cacheManager) throws Exception {
        storeInCache(spark,
                documentHashToProjectToBeCachedDF,
                documentHashToBeCachedDF,
                cacheRootDir,
                lockManager,
                cacheManager,
                new AvroDataStoreWriter(spark));
    }

    public static void storeInCache(SparkSession spark,
                                    Dataset<Row> documentHashToProjectToBeCachedDF,
                                    Dataset<Row> documentHashToBeCachedDF,
                                    String cacheRootDir,
                                    LockManager lockManager,
                                    CacheMetadataManagingProcess cacheManager,
                                    AvroDataStoreWriter writer) throws Exception {
        lockManager.obtain(cacheRootDir);
        try {
            String newCacheId = cacheManager.generateNewCacheId(spark.sparkContext().hadoopConfiguration(), cacheRootDir);
            Path newCachePath = new Path(cacheRootDir, newCacheId);
            FileSystem fileSystem = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            logger.info("Storing cached data in path {}.", newCachePath.toString());
            try {
                storeDocumentHashToProjectInCache(documentHashToProjectToBeCachedDF,
                        newCachePath,
                        writer);
                storeDocumentHashInCache(documentHashToBeCachedDF,
                        newCachePath,
                        writer);
                storeMetaInCache(cacheManager,
                        cacheRootDir,
                        newCacheId,
                        spark.sparkContext().hadoopConfiguration());
            } catch (Exception e) {
                fileSystem.delete(newCachePath, true);
                throw e;
            }
        } finally {
            lockManager.release(cacheRootDir);
        }
    }

    private static void storeDocumentHashToProjectInCache(Dataset<Row> documentHashToProjectToBeCachedDF,
                                                          Path newCachePath,
                                                          AvroDataStoreWriter writer) {
        writer.write(documentHashToProjectToBeCachedDF,
                new Path(newCachePath,
                        CachedTaraReferenceExtractionJob.CacheRecordType.documentHashToProject.name()).toString(),
                DocumentHashToProject.SCHEMA$);
    }

    private static void storeDocumentHashInCache(Dataset<Row> documentHashToBeCachedDF,
                                                 Path newCachePath,
                                                 AvroDataStoreWriter writer) {
        writer.write(documentHashToBeCachedDF,
                new Path(newCachePath,
                        CachedTaraReferenceExtractionJob.CacheRecordType.documentHash.name()).toString(),
                DocumentHash.SCHEMA$);
    }

    private static void storeMetaInCache(CacheMetadataManagingProcess cacheManager,
                                         String cacheRootDir,
                                         String newCacheId,
                                         Configuration conf) throws IOException {
        cacheManager.writeCacheId(conf, cacheRootDir, newCacheId);
    }

    public static void storeInOutput(SparkSession spark,
                                     Dataset<Row> resultsToOutputDF,
                                     String outputDocumentToProject) {
        storeInOutput(resultsToOutputDF, outputDocumentToProject, new AvroDataStoreWriter(spark));
    }

    public static void storeInOutput(Dataset<Row> resultsToOutputDF,
                                     String outputDocumentToProject,
                                     AvroDataStoreWriter writer) {
        logger.info("Storing output data in path {}.", outputDocumentToProject);
        writer.write(resultsToOutputDF, outputDocumentToProject, DocumentToProject.SCHEMA$);
    }

    public static class OutputCleaner {
        private Configuration conf;

        public OutputCleaner(SparkSession spark) {
            this.conf = spark.sparkContext().hadoopConfiguration();
        }

        public void clearOutput(String output) throws IOException {
            HdfsUtils.remove(conf, output);
        }
    }

    public static class AvroDataStoreReader {
        private AvroDataFrameSupport avroDataFrameSupport;

        private AvroDataStoreReader(SparkSession spark) {
            this.avroDataFrameSupport = new AvroDataFrameSupport(spark);
        }

        public Dataset<Row> read(String path, Schema schema) {
            return avroDataFrameSupport.read(path, schema);
        }
    }

    public static class AvroDataStoreWriter {
        private AvroDataFrameSupport avroDataFrameSupport;

        public AvroDataStoreWriter(SparkSession spark) {
            this.avroDataFrameSupport = new AvroDataFrameSupport(spark);
        }

        public void write(Dataset<Row> df, String path, Schema avroSchema) {
            avroDataFrameSupport.write(df, path, avroSchema);
        }
    }
}
