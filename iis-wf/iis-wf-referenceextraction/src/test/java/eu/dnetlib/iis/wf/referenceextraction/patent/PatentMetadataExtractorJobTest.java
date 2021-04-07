package eu.dnetlib.iis.wf.referenceextraction.patent;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.referenceextraction.patent.parser.PatentMetadataParserException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link PatentMetadataExtractorJob} test class.
 *
 */
@SlowTest
public class PatentMetadataExtractorJobTest {

    static final String xmlResourcesRootClassPath = "/eu/dnetlib/iis/wf/referenceextraction/patent/data/";
    
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private Path inputImportedPatentDir;
    private Path inputDocumentTextDir;
    private Path outputDir;
    private Path outputFaultDir;
    private Path outputReportDir;

    @BeforeEach
    public void before() {
        inputImportedPatentDir = workingDir.resolve("input_imported_patent");
        inputDocumentTextDir = workingDir.resolve("input_document_text");
        outputDir = workingDir.resolve("output");
        outputFaultDir = workingDir.resolve("fault");
        outputReportDir = workingDir.resolve("report");
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
        
        List<Fault> generatedFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString());
        assertNotNull(generatedFaults);
        assertEquals(0, generatedFaults.size());

        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        assertReports(AvroTestUtils.readLocalAvroDataStore(outputReportDir.toString()), 1, 0);
    }
    
    @Test
    public void testExtractMetadataNoCountryCode() throws IOException {
        // given
        String matchedPatentId = "1234";
        List<ImportedPatent> importedPatent = Lists.newArrayList(
                buildImportedPatent("XX", matchedPatentId),
                buildImportedPatent("YY", "5678"));
        List<DocumentText> documentText = Lists.newArrayList(
                buildDocumentText(matchedPatentId, xmlResourcesRootClassPath + "WO.0042078.A1.no_country_code.xml"));

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
        
        List<Fault> generatedFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString());
        assertNotNull(generatedFaults);
        assertEquals(0, generatedFaults.size());

        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        assertReports(AvroTestUtils.readLocalAvroDataStore(outputReportDir.toString()), 1, 0);
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
        assertEquals(0, calculatedResult.size());
        
        List<Fault> generatedFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString());
        assertNotNull(generatedFaults);
        assertEquals(1, generatedFaults.size());
        Fault fault = generatedFaults.get(0);
        assertEquals(patentId, fault.getInputObjectId().toString());
        assertEquals(PatentMetadataParserException.class.getCanonicalName(), fault.getCode().toString());

        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        assertReports(AvroTestUtils.readLocalAvroDataStore(outputReportDir.toString()), 1, 1);
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
        
        List<Fault> generatedFaults = AvroTestUtils.readLocalAvroDataStore(outputFaultDir.toString());
        assertNotNull(generatedFaults);
        assertEquals(0, generatedFaults.size());

        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
        assertReports(AvroTestUtils.readLocalAvroDataStore(outputReportDir.toString()), 0, 0);
    }
    
    private void assertReports(List<ReportEntry> reports, int processedTotal, int processedFault) {
        assertNotNull(reports);
        assertEquals(2, reports.size());
        
        Map<String, String> reportMap = reports.stream()
                .collect(Collectors.toMap(x -> x.getKey().toString(), x -> x.getValue().toString()));
        
        String counterValue = reportMap.get(PatentMetadataExtractorJob.COUNTER_PROCESSED_TOTAL);
        assertNotNull(counterValue);
        assertEquals(processedTotal, Integer.parseInt(counterValue));
        
        counterValue = reportMap.get(PatentMetadataExtractorJob.COUNTER_PROCESSED_FAULT);
        assertNotNull(counterValue);
        assertEquals(processedFault, Integer.parseInt(counterValue));
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
    
    private DocumentText buildDocumentText(String id, String textClassPathLocation) {
        DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
        documentTextBuilder.setId(id);
        String textContent = ClassPathResourceProvider.getResourceContent(textClassPathLocation);
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
                .addArg("-outputFaultPath", outputFaultDir.toString())
                .addArg("-outputReportPath", outputReportDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
    
}
