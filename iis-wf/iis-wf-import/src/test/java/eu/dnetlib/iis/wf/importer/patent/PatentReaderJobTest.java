package eu.dnetlib.iis.wf.importer.patent;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class PatentReaderJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path outputPath;
    private Path outputReportPath;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("patent");
        outputPath = workingDir.resolve("output");
        outputReportPath = workingDir.resolve("report");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldReadPatentEPOFileAndStorePatentsAsAvroDatastores() throws IOException {
        // given
        String patentsEpoPath = Objects
                .requireNonNull(getClass().getClassLoader().getResource("eu/dnetlib/iis/wf/importer/patent/data/input/patents_epo.json"))
                .getFile();
        String patentsEpoMappedPath = Objects
                .requireNonNull(getClass().getClassLoader().getResource("eu/dnetlib/iis/wf/importer/patent/data/output/patents_epo_mapped.json"))
                .getFile();
        String reportPath = Objects
                .requireNonNull(getClass().getClassLoader().getResource("eu/dnetlib/iis/wf/importer/patent/data/output/report.json"))
                .getFile();

        // execute & assert
        executeJobAndAssert(patentsEpoPath, patentsEpoMappedPath, reportPath);
    }

    private void executeJobAndAssert(String patentsEpoPath, String patentsEpoMappedPath, String reportPath) throws IOException {
        SparkJob sparkJob = SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentReaderJob.class)
                .addArg("-inputJSONLocation", patentsEpoPath)
                .addArg("-outputPath", outputPath.toString())
                .addArg("-outputReportPath", outputReportPath.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();

        // execute
        executor.execute(sparkJob);

        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath.toString(), patentsEpoMappedPath, Patent.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath.toString(), reportPath, ReportEntry.class);
    }
}
