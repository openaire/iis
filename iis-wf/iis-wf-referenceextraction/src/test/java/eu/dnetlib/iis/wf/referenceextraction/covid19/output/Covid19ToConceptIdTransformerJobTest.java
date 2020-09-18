package eu.dnetlib.iis.wf.referenceextraction.covid19.output;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.MatchedDocument;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

public class Covid19ToConceptIdTransformerJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path inputDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("covid19-output-transformer");
        inputDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldConvertMatchedDocumentAvroDatastore() throws IOException {
        // given
        String inputPath = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/covid19/data/matched_document.json");
        String outputPath = StaticResourceProvider
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