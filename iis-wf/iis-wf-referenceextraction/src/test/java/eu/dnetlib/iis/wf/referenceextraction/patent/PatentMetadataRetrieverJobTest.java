package eu.dnetlib.iis.wf.referenceextraction.patent;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
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
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link PatentMetadataRetrieverJob} test class.
 * 
 * @author mhorst
 *
 */
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
    
    private TestingServer zookeeperServer;

    @BeforeEach
    public void before() throws Exception {
        inputDir = workingDir.resolve("input");
        input2Dir = workingDir.resolve("input2");
        outputDir = workingDir.resolve("output");
        output2Dir = workingDir.resolve("output2");
        outputFaultDir = workingDir.resolve("fault");
        outputFault2Dir = workingDir.resolve("fault2");
        outputReportDir = workingDir.resolve("report");
        outputReport2Dir = workingDir.resolve("report2");
        cacheRootDir = workingDir.resolve("cache");
        zookeeperServer = new TestingServer(true);
    }

    @AfterEach
    public void after() throws IOException {
        zookeeperServer.stop();
    }

    @Test
    public void testRetrievePatentMetaFromMockedRetrieverAndInitializeCache() throws IOException {
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
        
        SparkJob sparkJob = buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(),
                outputReportDir.toString(), PatentFacadeMockFactory.class.getCanonicalName());

        // when
        executor.execute(sparkJob);

        // then
        Configuration conf = new Configuration();

        assertEquals(2,
                HdfsUtils.countFiles(conf, outputDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);

        assertEquals(1,
                HdfsUtils.countFiles(conf, outputReportDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);

        // need to validate faults programmatically due to dynamic timestamp generation
        assertEquals(2,
                HdfsUtils.countFiles(conf, outputFaultDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));
        
        // validating cache
        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);

        org.apache.hadoop.fs.Path textCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.text);
        assertEquals(2,
                HdfsUtils.countFiles(conf, textCacheLocation.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), cachePath, DocumentText.class);

        org.apache.hadoop.fs.Path faultCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault);
        assertEquals(2,
                HdfsUtils.countFiles(conf, faultCacheLocation.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()));
    }
    
    @Test
    public void testRetrievePatentMetaFromMockedRetrieverAndUpdateCache() throws IOException {
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
        executor.execute(buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(),
                outputReportDir.toString(), PatentFacadeMockFactory.class.getCanonicalName()));
        executor.execute(buildSparkJob(input2Dir.toString(), output2Dir.toString(), outputFault2Dir.toString(),
                outputReport2Dir.toString(), PatentFacadeMockFactory.class.getCanonicalName()));

        // then
        Configuration conf = new Configuration();

        assertEquals(2,
                HdfsUtils.countFiles(conf, outputDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);

        assertEquals(2,
                HdfsUtils.countFiles(conf, output2Dir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Dir.toString(), output2Path, DocumentText.class);

        assertEquals(1,
                HdfsUtils.countFiles(conf, outputReportDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);

        assertEquals(1,
                HdfsUtils.countFiles(conf, outputReport2Dir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Dir.toString(), report2Path, ReportEntry.class);

        // need to validate faults programatically due to dynamic timestamp generation
        assertEquals(2,
                HdfsUtils.countFiles(conf, outputFaultDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));

        assertEquals(2,
                HdfsUtils.countFiles(conf, outputFault2Dir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Dir.toString()).size());
        
        // validating cache
        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);

        org.apache.hadoop.fs.Path textCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.text);
        assertEquals(2,
                HdfsUtils.countFiles(conf, textCacheLocation.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), cachePath, DocumentText.class);

        org.apache.hadoop.fs.Path faultCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault);
        assertEquals(2,
                HdfsUtils.countFiles(conf, faultCacheLocation.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()));
    }

    @Test
    public void testObtainPatentMetaFromCacheWithoutCallingPatentFacade() throws IOException {
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
        executor.execute(buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(),
                outputReportDir.toString(), PatentFacadeMockFactory.class.getCanonicalName()));
        executor.execute(buildSparkJob(inputDir.toString(), output2Dir.toString(), outputFault2Dir.toString(),
                outputReport2Dir.toString(), ExceptionThrowingPatentFacadeFactory.class.getCanonicalName()));

        // then
        Configuration conf = new Configuration();

        assertEquals(2,
                HdfsUtils.countFiles(conf, outputDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);

        assertEquals(2,
                HdfsUtils.countFiles(conf, output2Dir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Dir.toString(), outputPath, DocumentText.class);

        assertEquals(1,
                HdfsUtils.countFiles(conf, outputReportDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);

        assertEquals(1,
                HdfsUtils.countFiles(conf, outputReport2Dir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Dir.toString(), report2Path, ReportEntry.class);

        // need to validate faults programatically due to dynamic timestamp generation
        assertEquals(2,
                HdfsUtils.countFiles(conf, outputFaultDir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));

        assertEquals(1,
                HdfsUtils.countFiles(conf, outputReport2Dir.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Dir.toString()).size());
        
        // validating cache
        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);

        org.apache.hadoop.fs.Path textCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.text);
        assertEquals(2,
                HdfsUtils.countFiles(conf, textCacheLocation.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(textCacheLocation.toString(), cachePath, DocumentText.class);

        org.apache.hadoop.fs.Path faultCacheLocation = DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault);
        assertEquals(2,
                HdfsUtils.countFiles(conf, faultCacheLocation.toString(), path -> path.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        validateFaults(AvroTestUtils.readLocalAvroDataStore(faultCacheLocation.toString()));
    }
    
    private void validateFaults(List<Fault> faults) {
        assertNotNull(faults);
        assertEquals(1, faults.size());
        assertEquals("00000002", faults.get(0).getInputObjectId().toString());
        assertEquals("unable to find element", faults.get(0).getMessage().toString());
        assertEquals(java.util.NoSuchElementException.class.getCanonicalName(), faults.get(0).getCode().toString());
    }
    
    private SparkJob buildSparkJob(String inputPath, String outputPath, String outputFaultPath,
            String outputReportPath, String patentFacadeFactoryClassName) {
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
                .addArg("-patentFacadeFactoryClassname", patentFacadeFactoryClassName)
                .addArg("-DtestParam", "testValue")
                .addJobProperty("spark.driver.host", "localhost")
                .addJobProperty(ZKFailoverController.ZK_QUORUM_KEY, "localhost:" + zookeeperServer.getPort())
                .build();
    }
}
