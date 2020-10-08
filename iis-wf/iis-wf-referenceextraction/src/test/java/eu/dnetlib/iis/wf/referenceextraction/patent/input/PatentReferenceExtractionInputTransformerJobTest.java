package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.PatentReferenceExtractionInput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;

public class PatentReferenceExtractionInputTransformerJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private Path inputDir;
    private Path outputDir;

    @BeforeEach
    public void before() {
        inputDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
    }

    @Test
    public void shouldConvertAvroDatastoreForReferenceExtraction() throws IOException {
        // given
        String inputPatentPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/input_transformer/imported_patent.json");
        String outputTransformedPatentPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/input_transformer/patent_transformed.json");
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPatentPath, ImportedPatent.class), inputDir.toString());

        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputTransformedPatentPath, PatentReferenceExtractionInput.class);
    }

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