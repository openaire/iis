package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.Files;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils.CacheRecordType;
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

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class CachedWebCrawlerJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputPath;
    
    private String input2Path;
    
    private String outputPath;
    
    private String outputFaultPath;
    
    private String outputReportPath;
    
    private String output2Path;
    
    private String outputFault2Path;
    
    private String outputReport2Path;
    
    private Path cacheRootDir;
    
    private TestingServer zookeeperServer;
    
    @Before
    public void before() throws Exception {
        workingDir = Files.createTempDir();
        inputPath = workingDir + "/spark_webcrawler/input";
        input2Path = workingDir + "/spark_webcrawler/input2";
        outputPath = workingDir + "/spark_webcrawler/output";
        output2Path = workingDir + "/spark_webcrawler/output2";
        outputFaultPath = workingDir + "/spark_webcrawler/fault";
        outputFault2Path = workingDir + "/spark_webcrawler/fault2";
        outputReportPath = workingDir + "/spark_webcrawler/report";
        outputReport2Path = workingDir + "/spark_webcrawler/report2";
        cacheRootDir = new Path(workingDir + "/spark_webcrawler/cache");
        zookeeperServer = new TestingServer(true);
    }
    
    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
        zookeeperServer.stop();
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void obtainPageSourceAndInitializeCache() throws IOException {
        
        // given
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source.json";
        String jsonCacheFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/cache_text1.json";
        String jsonReportFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report.json";

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath));
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text).toString(), 
                jsonCacheFile, DocumentText.class);
        
        // evaluating faults
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFaultPath).size());
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault).toString());
        assertEquals(0, cachedFaults.size());
     
        // evaluating report
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
    }
    
    @Test
    public void obtainPageSourceAndInitializeCacheWithFault() throws IOException {
        
        // given
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/document_to_softwareurl.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/document_to_softwareurl_with_source.json";
        String jsonCacheFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/cache_text.json";
        String jsonReportFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/report.json";

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath));
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text).toString(), 
                jsonCacheFile, DocumentText.class);
        // evaluating faults
        List<Fault> resultFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultPath);
        assertEquals(2, resultFaults.size());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(0).getCode().toString());
        assertEquals("id-1", resultFaults.get(0).getInputObjectId().toString());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(1).getCode().toString());
        assertEquals("id-2", resultFaults.get(1).getInputObjectId().toString());
        
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault).toString());
        assertEquals(1, cachedFaults.size());
        assertEquals(DocumentNotFoundException.class.getName(), cachedFaults.get(0).getCode().toString());
        assertEquals("https://github.com/openaire/invalid", cachedFaults.get(0).getInputObjectId().toString());
        
        // evaluating report
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
    }
    
    @Test
    public void obtainPageSourceAndUpdateCache() throws IOException {
        
        // given
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl.json";
        String jsonInput2File = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl2.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source.json";
        String jsonOutput2File = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source2.json";
        String jsonCache2File = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/cache_text2.json";
        String jsonReportFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report.json";
        String jsonReport2File = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report_cache_update.json";
        
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
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Path, jsonOutput2File, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text).toString(), 
                jsonCache2File, DocumentText.class);
        // evaluating faults
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFaultPath).size());
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Path).size());
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault).toString());
        assertEquals(0, cachedFaults.size());
        
        // evaluating report
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Path, jsonReport2File, ReportEntry.class);
    }
    
    @Test
    public void obtainPageSourceFromCache() throws IOException {

        // given
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source.json";
        String jsonCacheFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/cache_text1.json";
        String jsonReportFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report.json";
        String jsonReport2File = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/report_from_cache.json";
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath));
        executor.execute(buildWebCrawlerJob(inputPath, output2Path, outputFault2Path, outputReport2Path, 
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.ExceptionThrowingContentRetriever"));
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Path, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text).toString(), 
                jsonCacheFile, DocumentText.class);
        // evaluating faults
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFaultPath).size());
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Path).size());
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(
                DocumentTextCacheStorageUtils.getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault).toString());
        assertEquals(0, cachedFaults.size());
        
        // evaluating report
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Path, jsonReport2File, ReportEntry.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildWebCrawlerJob(String inputPath, String outputPath, String outputFaultPath,
            String outputReportPath, String contentRetrieverClassName) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName("Spark WebCrawler")
                .setMainClass(CachedWebCrawlerJob.class)
                .addArg("-inputPath", inputPath)
                .addArg("-contentRetrieverClassName", contentRetrieverClassName)
                .addArg("-lockManagerFactoryClassName", ZookeeperLockManagerFactory.class.getName())
                .addArg("-connectionTimeout", "0")
                .addArg("-readTimeout", "0")
                .addArg("-maxPageContentLength", "0")
                .addArg("-numberOfEmittedFiles", "1")
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
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.ClasspathContentRetriever");
    }
}
