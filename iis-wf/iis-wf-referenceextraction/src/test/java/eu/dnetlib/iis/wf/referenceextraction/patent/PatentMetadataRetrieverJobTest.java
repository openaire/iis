package eu.dnetlib.iis.wf.referenceextraction.patent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.audit.schemas.Fault;
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
    private Path outputDir;
    private Path outputFaultDir;
    private Path outputReportDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("patent");
        inputDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
        outputFaultDir = workingDir.resolve("fault");
        outputReportDir = workingDir.resolve("report");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void testRetrievePatentMetaFromMockedRetriever() throws IOException {
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
        
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ImportedPatent.class),
                inputDir.toString());
        
        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentText.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);
        // need to validate faults programatically due to dynamic timestamp generation
        List<Fault> faults = AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString());
        assertNotNull(faults);
        assertEquals(1, faults.size());
        assertEquals("00000002", faults.get(0).getInputObjectId().toString());
        assertEquals("unable to find element", faults.get(0).getMessage().toString());
        assertEquals(java.util.NoSuchElementException.class.getCanonicalName(), faults.get(0).getCode().toString());
    }

    private SparkJob buildSparkJob() {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentMetadataRetrieverJob.class)
                .addArg("-inputPath", inputDir.toString())
                .addArg("-numberOfEmittedFiles", String.valueOf(1))
                .addArg("-metadataRetrieverFacadeFactoryClassname", PatentFacadeMockFactory.class.getCanonicalName())
                .addArg("-outputPath", outputDir.toString())
                .addArg("-outputFaultPath", outputFaultDir.toString())
                .addArg("-outputReportPath", outputReportDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
}
