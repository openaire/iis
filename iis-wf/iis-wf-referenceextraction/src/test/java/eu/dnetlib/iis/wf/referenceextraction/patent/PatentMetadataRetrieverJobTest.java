package eu.dnetlib.iis.wf.referenceextraction.patent;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.CacheStorageUtils;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.lock.ZookeeperLockManagerFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SlowTest
public class PatentMetadataRetrieverJobTest {
    
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private Path inputDir;
    private Path input2Dir;
    private Path outputDir;
    private Path output2Dir;
    private Path outputFaultDir;
    private Path outputFault2Dir;
    private Path outputReportDir;
    private Path outputReport2Dir;
    private Path cacheRootDir;
    
    private static TestingServer zookeeperServer;

    @BeforeAll
    public static void beforeAll() throws Exception {
        zookeeperServer = new TestingServer(true);
    }

    @BeforeEach
    public void beforeEach() {
        inputDir = workingDir.resolve("input");
        input2Dir = workingDir.resolve("input2");
        outputDir = workingDir.resolve("output");
        output2Dir = workingDir.resolve("output2");
        outputFaultDir = workingDir.resolve("fault");
        outputFault2Dir = workingDir.resolve("fault2");
        outputReportDir = workingDir.resolve("report");
        outputReport2Dir = workingDir.resolve("report2");
        cacheRootDir = workingDir.resolve("cache");
    }

    @AfterAll
    public static void after() throws IOException {
        zookeeperServer.close();
    }

    @Test
    public void testRetrievePatentMetaFromStubbedRetrieverAndInitializeCacheWithPersistentFaults() throws IOException {
        // given
        String inputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input.json");
        String outputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output.json");
        String reportPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report.json");
        String cachePath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/cache_text1.json");
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ImportedPatent.class),
                inputDir.toString());
        
        SparkJob sparkJob = buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(), outputReportDir.toString(),
                "eu.dnetlib.iis.wf.referenceextraction.patent.TestServiceFacadeFactories$StubServiceFacadeFactoryWithPersistentFailure");

        // when
        executor.execute(sparkJob);

        // then
        Configuration conf = new Configuration();

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);

        // fault output contents - need to validate faults programmatically due to dynamic timestamp generation
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultDir.toString(), DataStore.AVRO_FILE_EXT));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));

        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);

        // text cache contents
        org.apache.hadoop.fs.Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), cachePath, DocumentText.class);

        // fault output contents
        org.apache.hadoop.fs.Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()));

        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);
    }

    @Test
    public void testRetrievePatentMetaFromStubbedRetrieverAndNotInitializeCacheWithTransientFaults() throws IOException {
        // given
        String inputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input.json");
        String outputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output.json");
        String reportPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report.json");
        String cachePath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/cache_text12.json");

        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();

        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ImportedPatent.class),
                inputDir.toString());

        SparkJob sparkJob = buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(), outputReportDir.toString(),
                "eu.dnetlib.iis.wf.referenceextraction.patent.TestServiceFacadeFactories$StubServiceFacadeFactoryWithTransientFailure");

        // when
        executor.execute(sparkJob);

        // then
        Configuration conf = new Configuration();

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);

        // fault output contents - need to validate faults programmatically due to dynamic timestamp generation
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultDir.toString(), DataStore.AVRO_FILE_EXT));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));

        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);

        // text cache contents
        org.apache.hadoop.fs.Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), cachePath, DocumentText.class);

        // fault output contents
        org.apache.hadoop.fs.Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()).isEmpty());

        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);
    }

    @Test
    public void testRetrievePatentMetaFromStubbedRetrieverAndUpdateCache() throws IOException {
        // given
        String inputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input.json");
        String input2Path = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input2.json");
        String outputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output.json");
        String output2Path = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output2.json");
        String reportPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report.json");
        String report2Path = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report_update.json");
        String cachePath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/cache_text2.json");
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ImportedPatent.class),
                inputDir.toString());
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(input2Path, ImportedPatent.class),
                input2Dir.toString());
        
        // when
        executor.execute(buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(), outputReportDir.toString(),
                "eu.dnetlib.iis.wf.referenceextraction.patent.TestServiceFacadeFactories$StubServiceFacadeFactoryWithPersistentFailure"));
        executor.execute(buildSparkJob(input2Dir.toString(), output2Dir.toString(), outputFault2Dir.toString(), outputReport2Dir.toString(),
                "eu.dnetlib.iis.wf.referenceextraction.patent.TestServiceFacadeFactories$StubServiceFacadeFactoryWithPersistentFailure"));

        // then
        Configuration conf = new Configuration();

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);

        assertEquals(2, HdfsTestUtils.countFiles(conf, output2Dir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Dir.toString(), output2Path, DocumentText.class);

        // fault output contents - need to validate faults programmatically due to dynamic timestamp generation
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultDir.toString(), DataStore.AVRO_FILE_EXT));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));

        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFault2Dir.toString(), DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputFault2Dir.toString()).isEmpty());
        
        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);

        // text cache contents
        org.apache.hadoop.fs.Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), cachePath, DocumentText.class);

        // fault output contents
        org.apache.hadoop.fs.Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()));

        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);

        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReport2Dir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Dir.toString(), report2Path, ReportEntry.class);
    }

    @Test
    public void testObtainPatentMetaFromCacheWithoutCallingPatentServiceFacade() throws IOException {
        // given
        String inputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input.json");
        String outputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output.json");
        String reportPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report.json");
        String report2Path = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report_from_cache.json");
        String cachePath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/cache_text1.json");
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ImportedPatent.class),
                inputDir.toString());
        
        // when
        executor.execute(buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(), outputReportDir.toString(),
                "eu.dnetlib.iis.wf.referenceextraction.patent.TestServiceFacadeFactories$StubServiceFacadeFactoryWithPersistentFailure"));
        executor.execute(buildSparkJob(inputDir.toString(), output2Dir.toString(), outputFault2Dir.toString(), outputReport2Dir.toString(),
                "eu.dnetlib.iis.wf.referenceextraction.patent.TestServiceFacadeFactories$ExceptionThrowingFacadeFactory"));

        // then
        Configuration conf = new Configuration();

        // text output contents
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);

        assertEquals(2, HdfsTestUtils.countFiles(conf, output2Dir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Dir.toString(), outputPath, DocumentText.class);

        // fault output contents - need to validate faults programmatically due to dynamic timestamp generation
        assertEquals(2, HdfsTestUtils.countFiles(conf, outputFaultDir.toString(), DataStore.AVRO_FILE_EXT));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));

        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);

        // text cache contents
        org.apache.hadoop.fs.Path textCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.data);
        assertEquals(2, HdfsTestUtils.countFiles(conf, textCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), cachePath, DocumentText.class);

        // fault cache contents
        org.apache.hadoop.fs.Path faultCacheLocation = CacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault);
        assertEquals(2, HdfsTestUtils.countFiles(conf, faultCacheLocation.toString(), DataStore.AVRO_FILE_EXT));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()));

        // evaluating report
        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);

        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReport2Dir.toString(), DataStore.AVRO_FILE_EXT));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Dir.toString(), report2Path, ReportEntry.class);

        assertEquals(1, HdfsTestUtils.countFiles(conf, outputReport2Dir.toString(), DataStore.AVRO_FILE_EXT));
        assertTrue(AvroTestUtils.readLocalAvroDataStore(outputFault2Dir.toString()).isEmpty());
    }
    
    private void validateFaults(List<Fault> faults) {
        assertNotNull(faults);
        assertEquals(1, faults.size());
        assertEquals("00000002", faults.get(0).getInputObjectId().toString());
        assertEquals("unable to find element", faults.get(0).getMessage().toString());
        assertEquals(PatentWebServiceFacadeException.class.getCanonicalName(), faults.get(0).getCode().toString());
    }
    
    private SparkJob buildSparkJob(String inputPath, String outputPath, String outputFaultPath,
            String outputReportPath, String patentServiceFacadeFactoryClassName) {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentMetadataRetrieverJob.class)
                .addArg("-inputPath", inputPath)
                .addArg("-numberOfEmittedFiles", String.valueOf(2))
                .addArg("-lockManagerFactoryClassName", ZookeeperLockManagerFactory.class.getName())
                .addArg("-cacheRootDir", cacheRootDir.toString())
                .addArg("-outputPath", outputPath)
                .addArg("-outputFaultPath", outputFaultPath)
                .addArg("-outputReportPath", outputReportPath)
                .addArg("-patentServiceFacadeFactoryClassName", patentServiceFacadeFactoryClassName)
                .addArg("-DtestParam", "testValue")
                .addJobProperty("spark.driver.host", "localhost")
                .addJobProperty(ZKFailoverController.ZK_QUORUM_KEY, "localhost:" + zookeeperServer.getPort())
                .build();
    }
}
