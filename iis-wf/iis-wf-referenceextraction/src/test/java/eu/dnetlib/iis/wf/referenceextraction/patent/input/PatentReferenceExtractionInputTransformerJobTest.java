package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.PatentReferenceExtractionInput;
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

public class PatentReferenceExtractionInputTransformerJobTest {
    private ClassLoader cl = getClass().getClassLoader();
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path inputDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("patent-transformer");
        inputDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    //------------------------ TESTS --------------------------

    @Test
    public void shouldConvertAvroDatastoreForReferenceExtraction() throws IOException {
        // given
        String inputPatentPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/patent.json"))
                .getFile();
        String outputTransformedPatentPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/patent_transformed.json"))
                .getFile();
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPatentPath, Patent.class), inputDir.toString());

        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputTransformedPatentPath, PatentReferenceExtractionInput.class);
    }

    //------------------------ PRIVATE --------------------------

    private SparkJob buildSparkJob() {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentReferenceExtractionInputTransformerJob.class)
                .addArg("-inputPath", inputDir.toString())
                .addArg("-outputPath", outputDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }

}