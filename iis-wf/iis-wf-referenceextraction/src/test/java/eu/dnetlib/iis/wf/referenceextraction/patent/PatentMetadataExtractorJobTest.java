package eu.dnetlib.iis.wf.referenceextraction.patent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * {@link PatentMetadataExtractorJob} test class.
 *
 */
public class PatentMetadataExtractorJobTest {

    static final String xmlResourcesRootClassPath = "/eu/dnetlib/iis/wf/referenceextraction/patent/data/";
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path inputImportedPatentDir;
    private Path inputDocumentTextDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("patent_meta_extraction");
        inputImportedPatentDir = workingDir.resolve("input_imported_patent");
        inputDocumentTextDir = workingDir.resolve("input_document_text");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void testExtractMetadata() throws IOException {
        // given
        String matchedPatentId = "1234";
        List<ImportedPatent> importedPatent = Lists.newArrayList(
                buildImportedPatent("XX", matchedPatentId),
                buildImportedPatent("YY", "5678"));
        List<DocumentText> documentText = Lists.newArrayList(
                buildDocumentText(matchedPatentId, xmlResourcesRootClassPath + "WO.0042078.A1.xml"));

        AvroTestUtils.createLocalAvroDataStore(importedPatent, inputImportedPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(documentText, inputDocumentTextDir.toString());
        
        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        List<Patent> calculatedResult = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertNotNull(calculatedResult);
        assertEquals(1, calculatedResult.size());
        Patent parsedPatent = calculatedResult.get(0);
        assertNotNull(parsedPatent);
        assertEquals(matchedPatentId, parsedPatent.getApplnNr().toString());
        assertEquals("XX", parsedPatent.getApplnAuth().toString());
        assertEquals("WO2000EP00003", parsedPatent.getApplnNrEpodoc().toString());
    }
    
    @Test
    public void testExtractMetadataFromInvalidXmlFile() throws IOException {
        // given
        String patentId = "1234";
        List<ImportedPatent> importedPatent = Lists.newArrayList(
                buildImportedPatent("XX", patentId));
        DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
        documentTextBuilder.setId(patentId);
        documentTextBuilder.setText("");
        List<DocumentText> documentText = Lists.newArrayList(
                documentTextBuilder.build());

        AvroTestUtils.createLocalAvroDataStore(importedPatent, inputImportedPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(documentText, inputDocumentTextDir.toString());
        
        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        List<Patent> calculatedResult = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertNotNull(calculatedResult);
        assertEquals(1, calculatedResult.size());
        Patent parsedPatent = calculatedResult.get(0);
        assertNotNull(parsedPatent);
        assertEquals(patentId, parsedPatent.getApplnNr().toString());
        assertEquals("XX", parsedPatent.getApplnAuth().toString());
        assertNull(parsedPatent.getApplnNrEpodoc());
    }
    
    @Test
    public void testExtractMetadataForNotMatchableDocumentTextDatastore() throws IOException {
        // given
        List<ImportedPatent> importedPatent = Lists.newArrayList(
                buildImportedPatent("XX", "1234"));
        List<DocumentText> documentText = Lists.newArrayList(
                buildDocumentText("5678", xmlResourcesRootClassPath + "WO.0042078.A1.xml"));

        AvroTestUtils.createLocalAvroDataStore(importedPatent, inputImportedPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(documentText, inputDocumentTextDir.toString());
        
        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        List<Patent> calculatedResult = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertNotNull(calculatedResult);
        assertEquals(0, calculatedResult.size());
    }
    
    private ImportedPatent buildImportedPatent(String applnAuth, String applnNr) {
        ImportedPatent.Builder importedPatentBuilder = ImportedPatent.newBuilder();
        importedPatentBuilder.setApplnAuth(applnAuth);
        importedPatentBuilder.setApplnNr(applnNr);
        importedPatentBuilder.setPublnAuth("irrelevant");
        importedPatentBuilder.setPublnNr("irrelevant");
        importedPatentBuilder.setPublnKind("irrelevant");
        return importedPatentBuilder.build();
    }
    
    private DocumentText buildDocumentText(String id, String textClassPathLocation) throws IOException {
        DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
        documentTextBuilder.setId(id);
        String textContent = IOUtils.toString(
                PatentMetadataExtractorJob.class.getResourceAsStream(textClassPathLocation),
                StandardCharsets.UTF_8.name());
        documentTextBuilder.setText(textContent);
        return documentTextBuilder.build();
    }

    private SparkJob buildSparkJob() {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentMetadataExtractorJob.class)
                .addArg("-inputImportedPatentPath", inputImportedPatentDir.toString())
                .addArg("-inputDocumentTextPath", inputDocumentTextDir.toString())
                .addArg("-outputPath", outputDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
    
}
