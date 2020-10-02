package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.lock.ZookeeperLockManagerFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithSource;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 
 * @author mhorst
 *
 */
@IntegrationTest
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
    
    @BeforeEach
    public void before() throws Exception {
        workingDir = Files.createTempDirectory(CachedWebCrawlerJobTest.class.getSimpleName()).toFile();
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
    
    @AfterEach
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
        zookeeperServer.stop();
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

        assertEquals(2, HdfsUtils.countFiles(conf, outputPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        Path textCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text);
        assertEquals(2,
                HdfsUtils.countFiles(conf, textCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), jsonCacheFile, DocumentText.class);
        
        // evaluating faults
        Path faultCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2,
                HdfsUtils.countFiles(conf, faultCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFaultPath).size());
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString());
        assertEquals(0, cachedFaults.size());
     
        // evaluating report
        assertEquals(1, HdfsUtils.countFiles(conf, outputReportPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
    }
    
    @Test
    public void obtainPageSourceAndInitializeCacheWithFault() throws IOException {
        
        // given
        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/document_to_softwareurl.json");
        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/document_to_softwareurl_with_source.json");
        String jsonCacheFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/cache_text.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/nosource/report.json");

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputPath, outputPath, outputFaultPath, outputReportPath));
        
        // assert
        Configuration conf = new Configuration();

        assertEquals(2, HdfsUtils.countFiles(conf, outputPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        Path textCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text);
        assertEquals(2,
                HdfsUtils.countFiles(conf, textCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), jsonCacheFile, DocumentText.class);

        // evaluating faults
        assertEquals(2, HdfsUtils.countFiles(conf, outputFaultPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        List<Fault> resultFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultPath);
        assertEquals(2, resultFaults.size());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(0).getCode().toString());
        assertEquals("id-1", resultFaults.get(0).getInputObjectId().toString());
        assertEquals(DocumentNotFoundException.class.getName(), resultFaults.get(1).getCode().toString());
        assertEquals("id-2", resultFaults.get(1).getInputObjectId().toString());

        Path faultCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2,
                HdfsUtils.countFiles(conf, faultCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString());
        assertEquals(1, cachedFaults.size());
        assertEquals(DocumentNotFoundException.class.getName(), cachedFaults.get(0).getCode().toString());
        assertEquals("https://github.com/openaire/invalid", cachedFaults.get(0).getInputObjectId().toString());
        
        // evaluating report
        assertEquals(1,
                HdfsUtils.countFiles(conf, outputReportPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
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

        assertEquals(2, HdfsUtils.countFiles(conf, outputPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        assertEquals(2, HdfsUtils.countFiles(conf, output2Path, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Path, jsonOutput2File, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        Path textCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text);
        assertEquals(2,
                HdfsUtils.countFiles(conf, textCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), jsonCache2File, DocumentText.class);

        // evaluating faults
        assertEquals(2, HdfsUtils.countFiles(conf, outputFaultPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFaultPath).size());
        assertEquals(2, HdfsUtils.countFiles(conf, outputFault2Path, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Path).size());
        Path faultCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2,
                HdfsUtils.countFiles(conf, faultCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString());
        assertEquals(0, cachedFaults.size());
        
        // evaluating report
        assertEquals(1, HdfsUtils.countFiles(conf, outputReportPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
        assertEquals(1, HdfsUtils.countFiles(conf, outputReport2Path, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
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
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.ExceptionThrowingContentRetrieverFactory"));
        
        // assert
        Configuration conf = new Configuration();

        assertEquals(2, HdfsUtils.countFiles(conf, outputPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        assertEquals(2, HdfsUtils.countFiles(conf, output2Path, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Path, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
        
        // evaluating cache contents
        String cacheId = cacheManager.getExistingCacheId(conf, cacheRootDir);
        assertNotNull(cacheId);

        Path textCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.text);
        assertEquals(2,
                HdfsUtils.countFiles(conf, textCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), jsonCacheFile, DocumentText.class);

        // evaluating faults
        assertEquals(2, HdfsUtils.countFiles(conf, outputFaultPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFaultPath).size());
        assertEquals(2, HdfsUtils.countFiles(conf, outputFault2Path, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Path).size());

        Path faultCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(cacheRootDir, cacheId, CacheRecordType.fault);
        assertEquals(2,
                HdfsUtils.countFiles(conf, faultCacheLocation.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        List<Fault> cachedFaults = AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString());
        assertEquals(0, cachedFaults.size());
        
        // evaluating report
        assertEquals(1, HdfsUtils.countFiles(conf, outputReportPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonReportFile, ReportEntry.class);
        assertEquals(1, HdfsUtils.countFiles(conf, outputReport2Path, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Path, jsonReport2File, ReportEntry.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildWebCrawlerJob(String inputPath, String outputPath, String outputFaultPath,
            String outputReportPath, String contentRetrieverFactoryClassName) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName("Spark WebCrawler")
                .setMainClass(CachedWebCrawlerJob.class)
                .addArg("-inputPath", inputPath)
                .addArg("-contentRetrieverFactoryClassName", contentRetrieverFactoryClassName)
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
                "eu.dnetlib.iis.wf.referenceextraction.softwareurl.ClasspathContentRetrieverFactory");
    }
}
