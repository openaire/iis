package eu.dnetlib.iis.common.cache;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.FsShellPermissions;
import eu.dnetlib.iis.common.java.PortBindings;

/**
 * {@link CacheMetadataManagingProcess} test class.
 * 
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class CacheMetadataManagingProcessTest {

    private final PortBindings portBindings = null;
    
    private final Configuration conf = null;
    
    private final String irrelevantCacheDir = "irrelevantCacheDir";
    
    @Captor
    ArgumentCaptor<FsShellPermissions.Op> changePermissionsOp;
    
    @Captor
    ArgumentCaptor<Boolean> changePermissionsRecursive;
    
    @Captor
    ArgumentCaptor<String> changePermissionsGroup;
    
    @Captor
    ArgumentCaptor<String> changePermissionsPath;
    
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Mock
    private FileSystemFacade fsFacade;
    
    private FileSystemFacadeFactory fsFacadeFactory = new FileSystemFacadeFactory() {
        @Override
        public FileSystemFacade create(Configuration conf) throws IOException {
            return fsFacade;
        }
    };
    
    private CacheMetadataManagingProcess process = new CacheMetadataManagingProcess(fsFacadeFactory);
    
    @Before
    public void initEnv() {
        System.setProperty(CacheMetadataManagingProcess.OOZIE_ACTION_OUTPUT_FILENAME, 
                testFolder.getRoot().getAbsolutePath() + File.separatorChar + "test.properties");
    }
    
    //------------------------ TESTS --------------------------
    
    @Test(expected=RuntimeException.class)
    public void testInvalidMode() throws Exception {
        process.run(portBindings, conf, 
                setParams("unknown", null));
    }

    @Test(expected=RuntimeException.class)
    public void testReadCurrentIdWithoutDir() throws Exception {
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_READ_CURRENT_ID, null));
    }
    
    @Test()
    public void testReadCurrentIdNonExistingDir() throws Exception {
        // given
        doReturn(false).when(fsFacade).exists(any());
        
        // execute
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_READ_CURRENT_ID, irrelevantCacheDir));
        
        // verify
        verifyStoredCacheId("$UNDEFINED$");
    }
    
    @Test()
    public void testReadCurrentId() throws Exception {
        // given
        String cacheId = "000021";
        doReturn(true).when(fsFacade).exists(any());
        doReturn(new ByteArrayInputStream(buildCacheJson(cacheId).getBytes())).when(fsFacade).open(any());
        
        // execute
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_READ_CURRENT_ID, irrelevantCacheDir));
        
        // verify
        verifyStoredCacheId(cacheId);
    }
    
    @Test(expected=RuntimeException.class)
    public void testGenerateNewIdWithoutDir() throws Exception {
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_GENERATE_NEW_ID, null));
    }
    
    @Test()
    public void testGenerateNewIdNonExistingDir() throws Exception {
        // given
        doReturn(false).when(fsFacade).exists(any());
        
        // execute
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_GENERATE_NEW_ID, irrelevantCacheDir));
        
        // verify
        verifyStoredCacheId("000001");
    }
    
    @Test()
    public void testGenerateNewId() throws Exception {
        // given
        doReturn(true).when(fsFacade).exists(any());
        doReturn(new ByteArrayInputStream(buildCacheJson("000021").getBytes())).when(fsFacade).open(any());
        
        // execute
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_GENERATE_NEW_ID, irrelevantCacheDir));
        
        // verify
        verifyStoredCacheId("000022");
    }
    
    
    @Test(expected=RuntimeException.class)
    public void testWriteIdWithoutDir() throws Exception {
        String cacheId = "123456";
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_WRITE_ID, null, cacheId));
    }
    
    @Test(expected=RuntimeException.class)
    public void testWriteIdWithoutId() throws Exception {
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_WRITE_ID, irrelevantCacheDir, null));
    }
    
    @Test()
    public void testWriteIdNonExistingDir() throws Exception {
        // given
        String cacheId = "123456";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Path path = new Path(irrelevantCacheDir, CacheMetadataManagingProcess.DEFAULT_METAFILE_NAME);
        
        doReturn(false).when(fsFacade).exists(any());
        doReturn(bos).when(fsFacade).create(
                new Path(irrelevantCacheDir, CacheMetadataManagingProcess.DEFAULT_METAFILE_NAME), true);
        
        // execute
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_WRITE_ID, irrelevantCacheDir, cacheId));
        
        // verify
        verify(fsFacade).changePermissions(any(), 
                changePermissionsOp.capture(), changePermissionsRecursive.capture(), 
                changePermissionsGroup.capture(), changePermissionsPath.capture());
        assertEquals(FsShellPermissions.Op.CHMOD, changePermissionsOp.getValue());
        assertEquals(false, changePermissionsRecursive.getValue());
        assertEquals("0666", changePermissionsGroup.getValue());
        assertEquals(path.toString(), changePermissionsPath.getValue());
        
        assertEquals(buildCacheJson(cacheId), bos.toString("utf8"));
    }
    
    @Test()
    public void tesWriteId() throws Exception {
        // given
        String cacheId = "123456";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Path path = new Path(irrelevantCacheDir, CacheMetadataManagingProcess.DEFAULT_METAFILE_NAME);
        
        doReturn(true).when(fsFacade).exists(any());
        doReturn(new ByteArrayInputStream(buildCacheJson("654321").getBytes())).when(fsFacade).open(any());
        doReturn(bos).when(fsFacade).create(path, true);
        
        // execute
        process.run(portBindings, conf, 
                setParams(CacheMetadataManagingProcess.MODE_WRITE_ID, irrelevantCacheDir, cacheId));
        
        // verify
        verify(fsFacade).changePermissions(any(), 
                changePermissionsOp.capture(), changePermissionsRecursive.capture(), 
                changePermissionsGroup.capture(), changePermissionsPath.capture());
        assertEquals(FsShellPermissions.Op.CHMOD, changePermissionsOp.getValue());
        assertEquals(false, changePermissionsRecursive.getValue());
        assertEquals("0666", changePermissionsGroup.getValue());
        assertEquals(path.toString(), changePermissionsPath.getValue());
        
        assertEquals(buildCacheJson(cacheId), bos.toString("utf8"));
    }
    
    // ----------------------- PRIVATE ----------------------------
    
    private void verifyStoredCacheId(String expectedId) throws FileNotFoundException, IOException {
        Properties properties = getStoredProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals(expectedId, properties.get(CacheMetadataManagingProcess.OUTPUT_PROPERTY_CACHE_ID));
    }
    
    private String buildCacheJson(String cacheId) {
        return "{\"currentCacheId\":\""+cacheId+"\"}";
    }
    
    private Properties getStoredProperties() throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(System.getProperty(CacheMetadataManagingProcess.OOZIE_ACTION_OUTPUT_FILENAME)));
        return properties;
    }
    
    private static Map<String, String> setParams(String mode, String cacheDir) {
       return setParams(mode, cacheDir, null);
    }
    
    private static Map<String, String> setParams(String mode, String cacheDir, String id) {
        Map<String, String> parameters = new HashMap<>();
        if (mode != null) { 
            setParam(parameters, CacheMetadataManagingProcess.PARAM_MODE, mode);
        }
        if (cacheDir != null) {
            setParam(parameters, CacheMetadataManagingProcess.PARAM_CACHE_DIR, cacheDir);
        }
        if (id != null) {
            setParam(parameters, CacheMetadataManagingProcess.PARAM_ID, id);    
        }
        return parameters;
    }
    
    private static Map<String, String> setParam(Map<String, String> parameters, 
            String key, String value) {
        parameters.put(key, value);
        return parameters;
    }
    
}
