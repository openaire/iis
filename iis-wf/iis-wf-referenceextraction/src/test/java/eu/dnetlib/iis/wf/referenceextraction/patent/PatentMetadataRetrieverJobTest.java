package eu.dnetlib.iis.wf.referenceextraction.patent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.lock.ZookeeperLockManagerFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * {@link PatentMetadataRetrieverJob} test class.
 * 
 * @author mhorst
 *
 */
public class PatentMetadataRetrieverJobTest {
    
    private ClassLoader cl = getClass().getClassLoader();
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
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

    @Before
    public void before() throws Exception {
        workingDir = Files.createTempDirectory("patent");
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

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        zookeeperServer.stop();
    }

    @Test
    public void testRetrievePatentMetaFromMockedRetrieverAndInitializeCache() throws IOException {
        // given
        String inputPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input.json"))
                .getFile();
        String outputPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output.json"))
                .getFile();
        String reportPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report.json"))
                .getFile();
        String cachePath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/cache_text1.json"))
                .getFile();
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ImportedPatent.class),
                inputDir.toString());
        
        SparkJob sparkJob = buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(),
                outputReportDir.toString(), PatentFacadeMockFactory.class.getCanonicalName());

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);
        // need to validate faults programatically due to dynamic timestamp generation
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));
        
        // validating cache
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.text)
                .toString(), cachePath, DocumentText.class);
        validateFaults(AvroTestUtils.readLocalAvroDataStore(DocumentTextCacheStorageUtils.getCacheLocation(
                new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault).toString()));
    }
    
    @Test
    public void testRetrievePatentMetaFromMockedRetrieverAndUpdateCache() throws IOException {
        // given
        String inputPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input.json"))
                .getFile();
        String input2Path = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input2.json"))
                .getFile();
        String outputPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output.json"))
                .getFile();
        String output2Path = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output2.json"))
                .getFile();
        String reportPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report.json"))
                .getFile();
        String report2Path = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report_update.json"))
                .getFile();
        String cachePath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/cache_text2.json"))
                .getFile();
        
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
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Dir.toString(), output2Path, DocumentText.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Dir.toString(), report2Path, ReportEntry.class);
        // need to validate faults programatically due to dynamic timestamp generation
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Dir.toString()).size());
        
        // validating cache
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.text)
                .toString(), cachePath, DocumentText.class);
        validateFaults(AvroTestUtils.readLocalAvroDataStore(DocumentTextCacheStorageUtils.getCacheLocation(
                new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault).toString()));
    }

    @Test
    public void testObtainPatentMetaFromCacheWithoutCallingPatentFacade() throws IOException {
        // given
        String inputPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/input.json"))
                .getFile();
        String outputPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/output.json"))
                .getFile();
        String reportPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report.json"))
                .getFile();
        String report2Path = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/report_from_cache.json"))
                .getFile();
        String cachePath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/cache_text1.json"))
                .getFile();
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ImportedPatent.class),
                inputDir.toString());
        
        // when
        executor.execute(buildSparkJob(inputDir.toString(), outputDir.toString(), outputFaultDir.toString(),
                outputReportDir.toString(), PatentFacadeMockFactory.class.getCanonicalName()));
        executor.execute(buildSparkJob(inputDir.toString(), output2Dir.toString(), outputFault2Dir.toString(),
                outputReport2Dir.toString(), ExceptionThrowingPatentFacadeFactory.class.getCanonicalName()));

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(output2Dir.toString(), outputPath, DocumentText.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReport2Dir.toString(), report2Path, ReportEntry.class);
        // need to validate faults programatically due to dynamic timestamp generation
        validateFaults(AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString()));
        assertEquals(0, AvroTestUtils.readLocalAvroDataStore(outputFault2Dir.toString()).size());
        
        // validating cache
        Configuration conf = new Configuration();
        String cacheId = cacheManager.getExistingCacheId(conf, new org.apache.hadoop.fs.Path(cacheRootDir.toString()));
        assertNotNull(cacheId);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(DocumentTextCacheStorageUtils
                .getCacheLocation(new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.text)
                .toString(), cachePath, DocumentText.class);
        validateFaults(AvroTestUtils.readLocalAvroDataStore(DocumentTextCacheStorageUtils.getCacheLocation(
                new org.apache.hadoop.fs.Path(cacheRootDir.toString()), cacheId, CacheRecordType.fault).toString()));
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
                .addArg("-numberOfEmittedFiles", String.valueOf(1))
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
