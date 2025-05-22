package eu.dnetlib.iis.common.cache;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * {@link CacheStorageUtils} test class.
 *
 */
@ExtendWith(MockitoExtension.class)
public class CacheStorageUtilsTest {
    
    @Mock
    private JavaSparkContext sparkContext;
    
    @Mock
    private SparkAvroLoader avroLoader;
    
    @Mock
    private SparkAvroSaver avroSaver;
    
    @Mock
    private LockManager lockManager;
    
    @Mock
    private CacheMetadataManagingProcess cacheManager;
    
    @Mock
    private JavaRDD<DocumentText> emptyPredefinedRdd;
    
    @Mock
    private JavaRDD<DocumentText> predefinedRdd;
    
    @Mock
    private JavaRDD<DocumentText> toBeStoredEntities;
    
    @Mock
    private JavaRDD<Fault> toBeStoredFaults;
    
    @Mock
    private JavaRDD<DocumentText> toBeStoredRepartitionedEntities;
    
    @Mock
    private JavaRDD<Fault> toBeStoredRepartitionedFaults;

    
    @Test
    public void testGetCacheLocation() {
        // given
        Path cacheRootDir = new Path("root");
        String cacheId = "someId";
        CacheRecordType cacheRecordType = CacheRecordType.data;
        
        // execute
        Path result = CacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, cacheRecordType);
        
        // verify
        assertNotNull(result);
        assertEquals(cacheRootDir.toString() + '/' + cacheId + '/' + cacheRecordType.name(), result.toString());
    }
    
    @Test
    public void testGetCacheLocationForBlanks() {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        String cacheId = "";
        CacheRecordType cacheRecordType = CacheRecordType.data;
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                CacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, cacheRecordType));
    }
    
    @Test
    public void getGetRddOrEmptyForExistingCache() {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        String existingCacheId = "someCacheId";
        CacheRecordType cacheRecordType = CacheRecordType.data;
        Class<DocumentText> avroRecordClass = DocumentText.class;
        doReturn(predefinedRdd).when(avroLoader).loadJavaRDD(sparkContext,
                CacheStorageUtils.getCacheLocation(cacheRootDir, existingCacheId, cacheRecordType).toString(), avroRecordClass);
        
        // execute
        JavaRDD<DocumentText> result = CacheStorageUtils.getRddOrEmpty(sparkContext, avroLoader, 
                cacheRootDir, existingCacheId, cacheRecordType, avroRecordClass);
        
        // assert
        assertNotNull(result);
        assertSame(predefinedRdd, result);
    }
    
    @Test
    public void getGetRddOrEmptyForNonExistingCache() {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        String existingCacheId = CacheMetadataManagingProcess.UNDEFINED;
        CacheRecordType cacheRecordType = CacheRecordType.data;
        Class<DocumentText> avroRecordClass = DocumentText.class;
        doReturn(true).when(emptyPredefinedRdd).isEmpty();
        doReturn(emptyPredefinedRdd).when(sparkContext).emptyRDD();
        
        // execute
        JavaRDD<DocumentText> result = CacheStorageUtils.getRddOrEmpty(sparkContext, avroLoader, 
                cacheRootDir, existingCacheId, cacheRecordType, avroRecordClass);
        
        // assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertSame(emptyPredefinedRdd, result);
    }
    
    @Test
    public void testStoreInCacheValidRecords() throws Exception {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        int numberOfEmittedFiles = 2;
        String predefinedCacheId = "someCacheId";
        Configuration hadoopConf = new Configuration();
        doReturn(predefinedCacheId).when(cacheManager).generateNewCacheId(hadoopConf, cacheRootDir);
        doReturn(toBeStoredRepartitionedEntities).when(toBeStoredEntities).repartition(numberOfEmittedFiles);
        doReturn(toBeStoredRepartitionedFaults).when(toBeStoredFaults).repartition(numberOfEmittedFiles);
        
        // execute
        CacheStorageUtils.storeInCache(avroSaver, DocumentText.SCHEMA$, toBeStoredEntities, toBeStoredFaults, 
                cacheRootDir, lockManager, cacheManager, hadoopConf, numberOfEmittedFiles);
        
        // assert
        verify(lockManager, times(1)).obtain(cacheRootDir.toString());
        verify(lockManager, times(1)).release(cacheRootDir.toString());

        verify(avroSaver, times(1)).saveJavaRDD(toBeStoredRepartitionedEntities, DocumentText.SCHEMA$,
                CacheStorageUtils.getCacheLocation(cacheRootDir, predefinedCacheId, CacheRecordType.data).toString());
        verify(avroSaver, times(1)).saveJavaRDD(toBeStoredRepartitionedFaults, Fault.SCHEMA$,
                CacheStorageUtils.getCacheLocation(cacheRootDir, predefinedCacheId, CacheRecordType.fault).toString());
        
        verify(cacheManager, times(1)).writeCacheId(hadoopConf, cacheRootDir, predefinedCacheId);
    }
    
    
    @Test
    public void testStoreInCacheByCheckingProperLocksHandlingWhenWriteCacheThrowsException() throws Exception {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        int numberOfEmittedFiles = 2;
        String predefinedCacheId = "someCacheId";
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        doReturn(predefinedCacheId).when(cacheManager).generateNewCacheId(hadoopConf, cacheRootDir);
        doReturn(toBeStoredRepartitionedEntities).when(toBeStoredEntities).repartition(numberOfEmittedFiles);
        doReturn(toBeStoredRepartitionedFaults).when(toBeStoredFaults).repartition(numberOfEmittedFiles);
        doThrow(IOException.class).when(cacheManager).writeCacheId(hadoopConf, cacheRootDir, predefinedCacheId);

        // execute
        assertThrows(IOException.class,
                () -> CacheStorageUtils.storeInCache(avroSaver, DocumentText.SCHEMA$, toBeStoredEntities,
                        toBeStoredFaults, cacheRootDir, lockManager, cacheManager, hadoopConf, numberOfEmittedFiles));

        // assert
        // validating proper locks handing
        verify(lockManager, times(1)).obtain(cacheRootDir.toString());
        verify(lockManager, times(1)).release(cacheRootDir.toString());
    }
    
}
