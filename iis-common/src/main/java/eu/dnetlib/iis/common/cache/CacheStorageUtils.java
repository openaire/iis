package eu.dnetlib.iis.common.cache;

import java.io.File;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;


/**
 * Shared methods for managing cache storage.
 * @author mhorst
 *
 */
public class CacheStorageUtils {
    
    private CacheStorageUtils() {}

    // ---------------------------- LOGIC ---------------------------------
    
    /**
     * Removes trailing file separator from the path whenever defined.
     */
    public static String normalizePath(String cacheRootDir) {
        Preconditions.checkArgument(StringUtils.isNotBlank(cacheRootDir), "cache root directory cannot be blank!");

        if (File.separatorChar == cacheRootDir.charAt(cacheRootDir.length() - 1)) {
            return cacheRootDir.substring(0, cacheRootDir.length() - 1);
        } else {
            return cacheRootDir;
        }
    }
    
    /**
     * Builts cache path for given cache coordinates, none of the parameters can be null.
     */
    public static String getCacheLocation(String cacheRootDir, String cacheId, CacheRecordType cacheRecordType) {
        Preconditions.checkArgument(StringUtils.isNotBlank(cacheRootDir), "cache root directory cannot be blank!");
        Preconditions.checkArgument(StringUtils.isNotBlank(cacheId), "cache id cannot be blank!");
        
        return cacheRootDir + '/' + cacheId + '/' + cacheRecordType.name();
    }
    
    /**
     * Reads RDD from cache or returns empty if cache id was set to undefined value
     * what means cache was not created yet.
     */
    public static <T extends GenericRecord> JavaRDD<T> getRddOrEmpty(JavaSparkContext sc, SparkAvroLoader avroLoader,
            String cacheRootDir,String existingCacheId, CacheRecordType cacheRecordType, Class<T> avroRecordClass) {
        return CacheMetadataManagingProcess.UNDEFINED.equals(existingCacheId) ? sc.emptyRDD()
                : avroLoader.loadJavaRDD(sc,
                        CacheStorageUtils.getCacheLocation(cacheRootDir, existingCacheId, cacheRecordType),
                        avroRecordClass);
    }
    
    /**
     * Stores new cache entry on HDFS. 
     * Utilizes lock manager to avoid storing new cache entries in the very same location by two independent job executions.
     */
    public static void storeInCache(SparkAvroSaver avroSaver, JavaRDD<DocumentText> toBeStoredEntities, JavaRDD<Fault> toBeStoredFaults, 
            String cacheRootDir, LockManager lockManager, CacheMetadataManagingProcess cacheManager, 
            Configuration hadoopConf, int numberOfEmittedFiles) throws Exception {

        lockManager.obtain(cacheRootDir);

        try {
            // getting new id for merging
            String newCacheId = cacheManager.generateNewCacheId(hadoopConf, cacheRootDir);
            
            try {
                // store in cache
                avroSaver.saveJavaRDD(toBeStoredEntities.coalesce(numberOfEmittedFiles), DocumentText.SCHEMA$, 
                        getCacheLocation(cacheRootDir, newCacheId, CacheRecordType.text));
                avroSaver.saveJavaRDD(toBeStoredFaults.coalesce(numberOfEmittedFiles), Fault.SCHEMA$, 
                        getCacheLocation(cacheRootDir, newCacheId, CacheRecordType.fault));
                // writing new cache id
                cacheManager.writeCacheId(hadoopConf, cacheRootDir, newCacheId);
                
            } catch(Exception e) {
                FileSystem.get(hadoopConf).delete(new Path(cacheRootDir, newCacheId), true);
                throw e;
            }       
            
        } finally {
            lockManager.release(cacheRootDir);
        }
        
    }
    
    /**
     * Type of the records stored within cache subdirectory.
     *
     */
    public enum CacheRecordType {
        text,
        fault
    }
    
    /**
     * Set of output paths used by the job utilizing caches.
     *
     */
    public static class OutputPaths {
        
        private final String result;
        private final String fault;
        private final String report;
        
        public OutputPaths(CachedStorageJobParameters params) {
            this.result = params.outputPath;
            this.fault = params.outputFaultPath;
            this.report = params.outputReportPath;
        }

        public String getResult() {
            return result;
        }

        public String getFault() {
            return fault;
        }

        public String getReport() {
            return report;
        }
    }
    
    /**
     * Parameters to be used by the job utilizing caches.
     *
     */
    public abstract static class CachedStorageJobParameters {
        
        @Parameter(names = "-lockManagerFactoryClassName", required = true)
        private String lockManagerFactoryClassName;
        
        @Parameter(names = "-cacheRootDir", required = true)
        private String cacheRootDir;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
        
        @Parameter(names = "-outputFaultPath", required = true)
        private String outputFaultPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;

        public String getLockManagerFactoryClassName() {
            return lockManagerFactoryClassName;
        }

        public void setLockManagerFactoryClassName(String lockManagerFactoryClassName) {
            this.lockManagerFactoryClassName = lockManagerFactoryClassName;
        }

        public String getCacheRootDir() {
            return cacheRootDir;
        }

        public void setCacheRootDir(String cacheRootDir) {
            this.cacheRootDir = cacheRootDir;
        }

        public String getOutputPath() {
            return outputPath;
        }

        public void setOutputPath(String outputPath) {
            this.outputPath = outputPath;
        }

        public String getOutputFaultPath() {
            return outputFaultPath;
        }

        public void setOutputFaultPath(String outputFaultPath) {
            this.outputFaultPath = outputFaultPath;
        }

        public String getOutputReportPath() {
            return outputReportPath;
        }

        public void setOutputReportPath(String outputReportPath) {
            this.outputReportPath = outputReportPath;
        }
        
    }
}
