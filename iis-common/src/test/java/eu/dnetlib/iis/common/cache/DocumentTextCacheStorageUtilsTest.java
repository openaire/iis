package eu.dnetlib.iis.common.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * {@link DocumentTextCacheStorageUtils} test class.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DocumentTextCacheStorageUtilsTest {
    
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

    
    @Test
    public void testGetCacheLocation() {
        // given
        Path cacheRootDir = new Path("root");
        String cacheId = "someId";
        CacheRecordType cacheRecordType = CacheRecordType.text;
        
        // execute
        Path result = DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, cacheRecordType);
        
        // verify
        assertNotNull(result);
        assertEquals(cacheRootDir.toString() + '/' + cacheId + '/' + cacheRecordType.name(), result.toString());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGetCacheLocationForBlanks() {
        // given
        Path cacheRootDir = new Path("");
        String cacheId = "";
        CacheRecordType cacheRecordType = CacheRecordType.text;
        
        // execute
        DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, cacheRecordType);
    }
    
    @Test
    public void getGetRddOrEmptyForExistingCache() {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        String existingCacheId = "someCacheId";
        CacheRecordType cacheRecordType = CacheRecordType.text;
        Class<DocumentText> avroRecordClass = DocumentText.class;
        doReturn(true).when(emptyPredefinedRdd).isEmpty();
        doReturn(predefinedRdd).when(avroLoader).loadJavaRDD(sparkContext,
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, existingCacheId, cacheRecordType).toString(), avroRecordClass);
        
        // execute
        JavaRDD<DocumentText> result = DocumentTextCacheStorageUtils.getRddOrEmpty(sparkContext, avroLoader, 
                cacheRootDir, existingCacheId, cacheRecordType, avroRecordClass);
        
        // assert
        assertNotNull(result);
        assertTrue(predefinedRdd == result);
    }
    
    @Test
    public void getGetRddOrEmptyForNonExistingCache() {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        String existingCacheId = CacheMetadataManagingProcess.UNDEFINED;
        CacheRecordType cacheRecordType = CacheRecordType.text;
        Class<DocumentText> avroRecordClass = DocumentText.class;
        doReturn(true).when(emptyPredefinedRdd).isEmpty();
        doReturn(emptyPredefinedRdd).when(sparkContext).emptyRDD();
        
        // execute
        JavaRDD<DocumentText> result = DocumentTextCacheStorageUtils.getRddOrEmpty(sparkContext, avroLoader, 
                cacheRootDir, existingCacheId, cacheRecordType, avroRecordClass);
        
        // assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertTrue(emptyPredefinedRdd == result);
    }
    
    @Test
    public void testStoreInCacheValidRecords() throws Exception {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        int numberOfEmittedFiles = 1;
        String predefinedCacheId = "someCacheId";
        doReturn(predefinedCacheId).when(cacheManager).generateNewCacheId(hadoopConf, cacheRootDir);
        doReturn(toBeStoredCoalescedEntities).when(toBeStoredEntities).coalesce(numberOfEmittedFiles);
        doReturn(toBeStoredCoalescedFaults).when(toBeStoredFaults).coalesce(numberOfEmittedFiles);
        
        // execute
        DocumentTextCacheStorageUtils.storeInCache(avroSaver, toBeStoredEntities, toBeStoredFaults, 
                cacheRootDir, lockManager, cacheManager, hadoopConf, numberOfEmittedFiles);
        
        // assert
        verify(lockManager, times(1)).obtain(cacheRootDir.toString());
        verify(lockManager, times(1)).release(cacheRootDir.toString());

        verify(avroSaver, times(1)).saveJavaRDD(toBeStoredCoalescedEntities, DocumentText.SCHEMA$, 
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, predefinedCacheId, CacheRecordType.text).toString());
        verify(avroSaver, times(1)).saveJavaRDD(toBeStoredCoalescedFaults, Fault.SCHEMA$, 
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, predefinedCacheId, CacheRecordType.fault).toString());
        
        verify(cacheManager, times(1)).writeCacheId(hadoopConf, cacheRootDir, predefinedCacheId);
    }
    
    
    @Test(expected = IOException.class)
    public void testStoreInCacheByCheckingProperLocksHandlingWhenWriteCacheThrowsException() throws Exception {
        // given
        Path cacheRootDir = new Path("some/root/dir");
        int numberOfEmittedFiles = 1;
        String predefinedCacheId = "someCacheId";
        doReturn(predefinedCacheId).when(cacheManager).generateNewCacheId(hadoopConf, cacheRootDir);
        doReturn(FileSystem.DEFAULT_FS).when(hadoopConf).get(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        doReturn(toBeStoredCoalescedEntities).when(toBeStoredEntities).coalesce(numberOfEmittedFiles);
        doReturn(toBeStoredCoalescedFaults).when(toBeStoredFaults).coalesce(numberOfEmittedFiles);
        doThrow(IOException.class).when(cacheManager).writeCacheId(hadoopConf, cacheRootDir, predefinedCacheId);
        
        try {
            // execute
            DocumentTextCacheStorageUtils.storeInCache(avroSaver, toBeStoredEntities, toBeStoredFaults, 
                    cacheRootDir, lockManager, cacheManager, hadoopConf, numberOfEmittedFiles);            
        } catch (Exception e) {
            // assert
            // validating proper locks handing
            verify(lockManager, times(1)).obtain(cacheRootDir.toString());
            verify(lockManager, times(1)).release(cacheRootDir.toString());
            throw e;
        }
    }
    
}
