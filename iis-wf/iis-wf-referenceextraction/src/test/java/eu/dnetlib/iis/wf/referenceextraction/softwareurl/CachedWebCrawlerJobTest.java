package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.CacheStorageUtils;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.lock.ZookeeperLockManagerFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithSource;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

@SlowTest
public class CachedWebCrawlerJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputPath;
    
    private String input2Path;
    
    private String outputPath;
    
    private String outputFaultPath;
    
    private String outputReportPath;
    
    private String output2Path;
    
    private String outputFault2Path;
    
    private String outputReport2Path;
    
    private Path cacheRootDir;
    
    private static TestingServer zookeeperServer;

    @BeforeAll
    public static void beforeAll() throws Exception {
        zookeeperServer = new TestingServer(true);
    }

    @BeforeEach
    public void beforeEach() {
        inputPath = workingDir + "/spark_webcrawler/input";
        input2Path = workingDir + "/spark_webcrawler/input2";
        outputPath = workingDir + "/spark_webcrawler/output";
        output2Path = workingDir + "/spark_webcrawler/output2";
        outputFaultPath = workingDir + "/spark_webcrawler/fault";
        outputFault2Path = workingDir + "/spark_webcrawler/fault2";
        outputReportPath = workingDir + "/spark_webcrawler/report";
        outputReport2Path = workingDir + "/spark_webcrawler/report2";
        cacheRootDir = new Path(workingDir + "/spark_webcrawler/cache");
    }
    
    @AfterAll
    public static void afterAll() throws IOException {
        zookeeperServer.close();
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void obtainPageSourceAndInitializeCache() throws IOException {
        
        // given
        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl.json");
        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source.json");
        String jsonCacheFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/cache_text1.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report.json");

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath));
        
        // assert
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // text cache contents
        Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), jsonCacheFile, DocumentText.class);
        
        // fault output contents
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputFaultPath).isEmpty());

        // fault cache contents
        Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()).isEmpty());

        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
    }
    
    @Test
    public void obtainPageSourceAndInitializeCacheWithPersistentFault() throws IOException {
        
        // given
        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/document_to_softwareurl.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/report.json");

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath,
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.TestServiceFacadeFactories$PersistentFailureReturningFacadeFactory"));
        
        // assert
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputPath, DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputPath).isEmpty());

        // text cache contents
        Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(textCacheLocation.toString()).isEmpty());

        // fault output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultPath, DataStore.AVRO_FILE_EXT));
        List<Fault> resultFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultPath);
        assertEquals(2, resultFaults.size());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(0).getCode().toString());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(1).getCode().toString());
        Set<String> expectedIds = new TreeSet<>();
        expectedIds.add("id-1");
        expectedIds.add("id-2");
        Set<String> receivedIds = new TreeSet<>();
        receivedIds.add(resultFaults.get(0).getInputObjectId().toString());
        receivedIds.add(resultFaults.get(1).getInputObjectId().toString());
        assertEquals(expectedIds, receivedIds);
        
        // fault cache contents
        Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString());
        assertEquals(1, cachedFaults.size());
        assertEquals(DocumentNotFoundException.class.getName(), cachedFaults.get(0).getCode().toString());
        assertEquals("https://github.com/openaire/invalid", cachedFaults.get(0).getInputObjectId().toString());
        
        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
    }

    @Test
    public void obtainPageSourceAndNotInitializeCacheWithTransientFault() throws IOException {
        // given
        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/document_to_softwareurl.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/report.json");

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class),
                inputPath);

        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath,
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.TestServiceFacadeFactories$TransientFailureReturningFacadeFactory"));

        // assert
        Configuration conf = new Configuration();

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputPath, DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputPath).isEmpty());

        // fault output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultPath, DataStore.AVRO_FILE_EXT));
        List<Fault> resultFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultPath);
        assertEquals(2, resultFaults.size());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(0).getCode().toString());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(1).getCode().toString());
        Set<String> expectedIds = new TreeSet<>();
        expectedIds.add("id-1");
        expectedIds.add("id-2");
        Set<String> receivedIds = new TreeSet<>();
        receivedIds.add(resultFaults.get(0).getInputObjectId().toString());
        receivedIds.add(resultFaults.get(1).getInputObjectId().toString());
        assertEquals(expectedIds, receivedIds);

        // evaluating cache contents
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertEquals(CacheMetadataManagingProcess.UNDEFINED, cacheId);
        assertThrows(IOException.class, () -> HdfsUtils.listDirs(conf, cacheRootDir.toString()));

        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
    }

    @Test
    public void obtainPageSourceAndUpdateCache() throws IOException {
        
        // given
        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl.json");
        String jsonInput2File = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl2.json");
        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source.json");
        String jsonOutput2File = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source2.json");
        String jsonCache2File = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/cache_text2.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report.json");
        String jsonReport2File = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report_cache_update.json");
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInput2File, DocumentToSoftwareUrl.class), 
                input2Path);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath));
        executor.execute(buildWebCrawlerJob(input2Path, output2Path, outputFault2Path, outputReport2Path));
        
        // assert
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        assertEquals(2, HdfsTestUtils.countFiles(conf, output2Path, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Path, jsonOutput2File, DocumentToSoftwareUrlWithSource.class);
        
        // text cache contents
        Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), jsonCache2File, DocumentText.class);

        // fault output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultPath, DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputFaultPath).isEmpty());
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFault2Path, DataStore.AVRO_FILE_EXT));
        assertTrue( AvroTestUtils.readLocalAvroDataStore(outputFault2Path).isEmpty());

        // fault cache contents
        Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()).isEmpty());
        
        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReport2Path, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Path, jsonReport2File, ReportEntry.class);
    }
    
    @Test
    public void obtainPageSourceFromCache() throws IOException {

        // given
        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl.json");
        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source.json");
        String jsonCacheFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/cache_text1.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report.json");
        String jsonReport2File = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report_from_cache.json");
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath));
        executor.execute(buildWebCrawlerJob(inputPath, output2Path, outputFault2Path, outputReport2Path, 
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.TestServiceFacadeFactories$ExceptionThrowingFacadeFactory"));
        
        // assert
        Configuration conf = new Configuration();

        assertEquals(2, HdfsTestUtils.countFiles(conf, outputPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        assertEquals(2, HdfsTestUtils.countFiles(conf, output2Path, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Path, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), jsonCacheFile, DocumentText.class);

        // evaluating faults
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultPath, DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputFaultPath).isEmpty());
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFault2Path, DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputFault2Path).isEmpty());

        Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString());
        assertTrue(cachedFaults.isEmpty());
        
        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportPath, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReport2Path, DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Path, jsonReport2File, ReportEntry.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildWebCrawlerJob(String inputPath, String outputPath, String outputFaultPath,
            String outputReportPath, String httpServiceFacadeFactoryClassName) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName("Spark WebCrawler")
                .setMainClass(CachedWebCrawlerJob.class)
                .addArg("-inputPath", inputPath)
                .addArg("-httpServiceFacadeFactoryClassName", httpServiceFacadeFactoryClassName)
                .addArg("-lockManagerFactoryClassName", ZookeeperLockManagerFactory.class.getName())
                .addArg("-numberOfEmittedFiles", "2")
                .addArg("-numberOfPartitionsForCrawling", "1")
                .addArg("-cacheRootDir", cacheRootDir.toString())
                .addArg("-outputPath", outputPath)
                .addArg("-outputFaultPath", outputFaultPath)
                .addArg("-outputReportPath", outputReportPath)
                .addJobProperty("spark.driver.host", "localhost")
                .addJobProperty(ZKFailoverController.ZK_QUORUM_KEY, "localhost:" + zookeeperServer.getPort())
                .build();
        
        return sparkJob;
    }
    
    private SparkJob buildWebCrawlerJob(String inputPath, 
            String outputPath, String outputFaultPath, String outputReportPath) {
        return buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath,
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.TestServiceFacadeFactories$FileContentReturningFacadeFactory");
    }
}
