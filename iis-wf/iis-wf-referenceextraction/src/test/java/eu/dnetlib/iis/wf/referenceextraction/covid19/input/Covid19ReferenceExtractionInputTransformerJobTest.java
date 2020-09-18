package eu.dnetlib.iis.wf.referenceextraction.covid19.input;

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
import eu.dnetlib.iis.referenceextraction.covid19.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

public class Covid19ReferenceExtractionInputTransformerJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path inputDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("covid19-input-transformer");
        inputDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldConvertAvroDatastoreForReferenceExtraction() throws IOException {
        // given
        String inputPath = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/covid19/data/document_metadata.json");
        String outputPath = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/covid19/data/document_metadata_transformed.json");
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputPath, ExtractedDocumentMetadataMergedWithOriginal.class), inputDir.toString());

        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentMetadata.class);
    }

    private SparkJob buildSparkJob() {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(Covid19ReferenceExtractionInputTransformerJob.class)
                .addArg("-input", inputDir.toString())
                .addArg("-output", outputDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }

}