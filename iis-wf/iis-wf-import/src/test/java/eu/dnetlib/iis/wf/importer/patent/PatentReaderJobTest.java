package eu.dnetlib.iis.wf.importer.patent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

public class PatentReaderJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path outputDir;
    private Path outputReportDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("patent");
        outputDir = workingDir.resolve("output");
        outputReportDir = workingDir.resolve("report");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldReadPatentEPOFileAndStorePatentsAsAvroDatastores() throws IOException {
        // given
        String patentsEpoPath = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/importer/patent/sampletest/oozie_app/input/patents_epo.tsv");
        String patentsEpoMappedPath = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/importer/patent/data/output/patents_epo_output.json");
        String reportPath = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/importer/patent/data/output/report.json");
        SparkJob sparkJob = buildSparkJob(patentsEpoPath);

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), patentsEpoMappedPath, ImportedPatent.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), reportPath, ReportEntry.class);
    }

    private SparkJob buildSparkJob(String patentsEpoPath) {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentReaderJob.class)
                .addArg("-inputTsvLocation", patentsEpoPath)
                .addArg("-outputPath", outputDir.toString())
                .addArg("-outputReportPath", outputReportDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
}
