package eu.dnetlib.iis.wf.referenceextraction.covid19.input;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;

@SlowTest
public class Covid19ReferenceExtractionInputTransformerJobTest {
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
        String inputPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/covid19/data/document_metadata.json");
        String outputPath = ClassPathResourceProvider
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