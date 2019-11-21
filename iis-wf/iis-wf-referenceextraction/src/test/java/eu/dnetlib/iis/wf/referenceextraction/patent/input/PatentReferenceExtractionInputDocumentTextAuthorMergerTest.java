package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentTextWithAuthors;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
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

public class PatentReferenceExtractionInputDocumentTextAuthorMergerTest {
    private ClassLoader cl = getClass().getClassLoader();
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path inputDocumentTextDir;
    private Path inputExtractedDocumentMetadataMergedWithOriginalDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("document-text-author_merger");
        inputDocumentTextDir = workingDir.resolve("inputDocumentText");
        inputExtractedDocumentMetadataMergedWithOriginalDir = workingDir.resolve("inputExtractedDocumentMetadataMergedWithOriginalDir");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldMergeDocumentTextWithImportedAuthors() throws IOException {
        // given
        String inputDocumentTextPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/document_text.json"))
                .getFile();
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(inputDocumentTextPath, DocumentText.class),
                inputDocumentTextDir.toString());
        String inputExtractedDocumentMetadataMergedWithOriginalPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/extracted_document_metadata_merged_with_original.json"))
                .getFile();
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(inputExtractedDocumentMetadataMergedWithOriginalPath, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputExtractedDocumentMetadataMergedWithOriginalDir.toString());
        String outputPath = Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/document_text_with_authors.json"))
                .getFile();

        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputPath, DocumentTextWithAuthors.class);
    }

    private SparkJob buildSparkJob() {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentReferenceExtractionInputDocumentTextAuthorMerger.class)
                .addArg("-inputDocumentTextPath", inputDocumentTextDir.toString())
                .addArg("-inputExtractedDocumentMetadataMergedWithOriginalPath", inputExtractedDocumentMetadataMergedWithOriginalDir.toString())
                .addArg("-outputPath", outputDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
}