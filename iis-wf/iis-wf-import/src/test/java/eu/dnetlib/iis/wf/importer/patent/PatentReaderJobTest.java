package eu.dnetlib.iis.wf.importer.patent;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SlowTest
public class PatentReaderJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private Path outputDir;
    private Path outputReportDir;

    @BeforeEach
    public void before() {
        outputDir = workingDir.resolve("output");
        outputReportDir = workingDir.resolve("report");
    }

    @Test
    public void shouldReadPatentEPOFileAndStorePatentsAsAvroDatastores() throws IOException {
        // given
        String patentsEpoPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/importer/patent/sampletest/oozie_app/input/patents_epo.tsv");
        String patentsEpoMappedPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/importer/patent/data/output/patents_epo_output.json");
        String reportPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/importer/patent/data/output/report.json");
        SparkJob sparkJob = buildSparkJob(patentsEpoPath);

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), patentsEpoMappedPath, ImportedPatent.class);

        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
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
