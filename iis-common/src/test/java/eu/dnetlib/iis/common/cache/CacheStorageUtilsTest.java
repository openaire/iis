package eu.dnetlib.iis.common.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * {@link CacheStorageUtils} test class.
 *
 */
@RunWith(MockitoJUnitRunner.class)
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
    private Configuration hadoopConf;
    
    @Mock
    private JavaRDD<DocumentText> emptyPredefinedRdd;
    
    @Mock
    private JavaRDD<DocumentText> predefinedRdd;
    
    @Mock
    private JavaRDD<DocumentText> toBeStoredEntities;
    
    @Mock
    private JavaRDD<Fault> toBeStoredFaults;
    
    @Mock
    private JavaRDD<DocumentText> toBeStoredCoalescedEntities;
    
    @Mock
    private JavaRDD<Fault> toBeStoredCoalescedFaults;
    
    @Captor
    private ArgumentCaptor<String> obtainLockCaptor;
    
    @Captor
    private ArgumentCaptor<String> releaseLockCaptor;

    
    @Test
    public void testNormalizePathNoTrailingSeparator() {
        // given
        String source = "foo" + File.separatorChar + "bar";
        
        // execute
        String result = CacheStorageUtils.normalizePath(source);
        
        // verify
        assertEquals(source, result);
    }
    
    @Test
    public void testNormalizePathWithTrailingSeparator() {
        // given
        String source = "test";
        
        // execute
        String result = CacheStorageUtils.normalizePath(source + File.separatorChar);
        
        // verify
        assertEquals(source, result);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNormalizePathWithBlankPath() {
        CacheStorageUtils.normalizePath("");
    }
    
    @Test
    public void testGetCacheLocation() {
        // given
        String cacheRootDir = "root";
        String cacheId = "someId";
        CacheRecordType cacheRecordType = CacheRecordType.text;
        
        // execute
        String result = CacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, cacheRecordType);
        
        // verify
        assertNotNull(result);
        assertEquals(cacheRootDir + '/' + cacheId + '/' + cacheRecordType.name(), result);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGetCacheLocationForBlanks() {
        // given
        String cacheRootDir = "";
        String cacheId = "";
        CacheRecordType cacheRecordType = CacheRecordType.text;
        
        // execute
        CacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, cacheRecordType);
    }
    
    @Test
    public void getGetRddOrEmptyForExistingCache() {
        // given
        String cacheRootDir = "some/root/dir";
        String existingCacheId = "someCacheId";
        CacheRecordType cacheRecordType = CacheRecordType.text;
        Class<DocumentText> avroRecordClass = DocumentText.class;
        doReturn(true).when(emptyPredefinedRdd).isEmpty();
        doReturn(predefinedRdd).when(avroLoader).loadJavaRDD(sparkContext,
                CacheStorageUtils.getCacheLocation(cacheRootDir, existingCacheId, cacheRecordType), avroRecordClass);
        
        // execute
        JavaRDD<DocumentText> result = CacheStorageUtils.getRddOrEmpty(sparkContext, avroLoader, 
                cacheRootDir, existingCacheId, cacheRecordType, avroRecordClass);
        
        // assert
        assertNotNull(result);
        assertTrue(predefinedRdd == result);
    }
    
    @Test
    public void getGetRddOrEmptyForNonExistingCache() {
        // given
        String cacheRootDir = "some/root/dir";
        String existingCacheId = CacheMetadataManagingProcess.UNDEFINED;
        CacheRecordType cacheRecordType = CacheRecordType.text;
        Class<DocumentText> avroRecordClass = DocumentText.class;
        doReturn(true).when(emptyPredefinedRdd).isEmpty();
        doReturn(emptyPredefinedRdd).when(sparkContext).emptyRDD();
        
        // execute
        JavaRDD<DocumentText> result = CacheStorageUtils.getRddOrEmpty(sparkContext, avroLoader, 
                cacheRootDir, existingCacheId, cacheRecordType, avroRecordClass);
        
        // assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertTrue(emptyPredefinedRdd == result);
    }
    
    @Test
    public void testStoreInCacheValidRecords() throws Exception {
        // given
        String cacheRootDir = "some/root/dir";
        int numberOfEmittedFiles = 1;
        String predefinedCacheId = "someCacheId";
        doReturn(predefinedCacheId).when(cacheManager).generateNewCacheId(hadoopConf, cacheRootDir);
        doReturn(toBeStoredCoalescedEntities).when(toBeStoredEntities).coalesce(numberOfEmittedFiles);
        doReturn(toBeStoredCoalescedFaults).when(toBeStoredFaults).coalesce(numberOfEmittedFiles);
        
        // execute
        CacheStorageUtils.storeInCache(avroSaver, toBeStoredEntities, toBeStoredFaults, 
                cacheRootDir, lockManager, cacheManager, hadoopConf, numberOfEmittedFiles);
        
        // assert
        verify(lockManager, times(1)).obtain(cacheRootDir);
        verify(lockManager, times(1)).release(cacheRootDir);

        verify(avroSaver, times(1)).saveJavaRDD(toBeStoredCoalescedEntities, DocumentText.SCHEMA$, 
                CacheStorageUtils.getCacheLocation(cacheRootDir, predefinedCacheId, CacheRecordType.text));
        verify(avroSaver, times(1)).saveJavaRDD(toBeStoredCoalescedFaults, Fault.SCHEMA$, 
                CacheStorageUtils.getCacheLocation(cacheRootDir, predefinedCacheId, CacheRecordType.fault));
        
        verify(cacheManager, times(1)).writeCacheId(hadoopConf, cacheRootDir, predefinedCacheId);
    }
    
    
    @Test(expected = IOException.class)
    public void testStoreInCacheByCheckingProperLocksHandlingWhenWriteCacheThrowsException() throws Exception {
        // given
        String cacheRootDir = "some/root/dir";
        int numberOfEmittedFiles = 1;
        String predefinedCacheId = "someCacheId";
        doReturn(predefinedCacheId).when(cacheManager).generateNewCacheId(hadoopConf, cacheRootDir);
        doReturn(FileSystem.DEFAULT_FS).when(hadoopConf).get(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        doReturn(toBeStoredCoalescedEntities).when(toBeStoredEntities).coalesce(numberOfEmittedFiles);
        doReturn(toBeStoredCoalescedFaults).when(toBeStoredFaults).coalesce(numberOfEmittedFiles);
        doThrow(IOException.class).when(cacheManager).writeCacheId(hadoopConf, cacheRootDir, predefinedCacheId);
        
        try {
            // execute
            CacheStorageUtils.storeInCache(avroSaver, toBeStoredEntities, toBeStoredFaults, 
                    cacheRootDir, lockManager, cacheManager, hadoopConf, numberOfEmittedFiles);            
        } catch (Exception e) {
            // assert
            // validating proper locks handing
            verify(lockManager, times(1)).obtain(cacheRootDir);
            verify(lockManager, times(1)).release(cacheRootDir);
            throw e;
        }
    }
    
}
