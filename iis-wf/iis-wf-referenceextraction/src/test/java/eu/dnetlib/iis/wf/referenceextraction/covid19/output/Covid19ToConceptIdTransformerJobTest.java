package eu.dnetlib.iis.wf.referenceextraction.covid19.output;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.MatchedDocument;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;

public class Covid19ToConceptIdTransformerJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    Path workingDir;

    private Path inputDir;
    private Path outputDir;

    @BeforeEach
    public void before() {
        inputDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
    }

    @Test
    public void shouldConvertMatchedDocumentAvroDatastore() throws IOException {
        // given
        String inputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/covid19/data/matched_document.json");
        String outputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/covid19/data/matched_document_transformed.json");
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, MatchedDocument.class), inputDir.toString());

        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentToConceptId.class);
    }

    private SparkJob buildSparkJob() {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(Covid19ToConceptIdTransformerJob.class)
                .addArg("-input", inputDir.toString())
                .addArg("-output", outputDir.toString())
                .addArg("-predefinedConceptId", "custom-covid-19")
                .addArg("-predefinedConfidenceLevel", "0.7")
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }

}