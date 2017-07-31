package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerJob.TABLE_CITATION;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerJob.TABLE_PUBLICATION;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerJob.TABLE_PUB_CITATION;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerJob.TABLE_PUB_FULLTEXT;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerJob.TABLE_PUB_GRANT;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerJob.TABLE_PUB_KEYWORD;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerJob.TABLE_PUB_PDBCODE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.Files;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

@Category(IntegrationTest.class)
public class AvroToRdbTransformerJobTest {

    private static final String inputJsonResourcesDir = "src/test/resources/eu/dnetlib/iis/wf/ptm/avro2rdb/data/input/";
    
    private static final String outputJsonResourcesDir = "src/test/resources/eu/dnetlib/iis/wf/ptm/avro2rdb/data/output/";
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputMetadataDirPath;
    
    private String inputTextDirPath;
    
    private String inputProjectDirPath;
    
    private String inputDocumentToProjectDirPath;
    
    private String inputDocumentToPdbDirPath;
    
    private String inputCitationDirPath;
    
    private String outputRootDirPath;
    
    private String reportDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        inputMetadataDirPath = workingDir + "/avro2rdb/input_metadata";
        inputTextDirPath = workingDir + "/avro2rdb/input_text";
        inputProjectDirPath = workingDir + "/avro2rdb/input_project";
        inputDocumentToProjectDirPath = workingDir + "/avro2rdb/input_document_to_project";
        inputDocumentToPdbDirPath = workingDir + "/avro2rdb/input_document_to_pdb";
        inputCitationDirPath = workingDir + "/avro2rdb/input_citation";
        
        outputRootDirPath = workingDir + "/avro2rdb/output";
        reportDirPath = workingDir + "/avro2rdb/report";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }

    //------------------------ TESTS --------------------------

    @Test
    public void testEmptyInput() throws IOException {
        
        // given
        createEmptyInputDatastores();
        
        // execute
        
        executor.execute(buildJob(".*", 0, 0, 0));
        
        // assert
        assertTrue(getOutputJsons(TABLE_PUBLICATION).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_GRANT).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_KEYWORD).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_FULLTEXT).isEmpty());
        assertTrue(getOutputJsons(TABLE_CITATION).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_CITATION).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_PDBCODE).isEmpty());
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, outputJsonResourcesDir + "report_empty.json", ReportEntry.class);
    }
    
    @Test
    public void testJobAllProjectsFilteredOut() throws IOException {
        
        // given
        createInputDatastores();
        
        // execute
        
        executor.execute(buildJob("^EC::non-existing", 0, 0, 0));
        
        // assert
        assertTrue(getOutputJsons(TABLE_PUBLICATION).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_GRANT).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_KEYWORD).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_FULLTEXT).isEmpty());
        assertTrue(getOutputJsons(TABLE_CITATION).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_CITATION).isEmpty());
        assertTrue(getOutputJsons(TABLE_PUB_PDBCODE).isEmpty());
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, outputJsonResourcesDir + "report_empty.json", ReportEntry.class);
    }
    
    @Test
    public void testJobAllProjectsAllowed() throws IOException {
        
        // given
        createInputDatastores();
        
        // execute
        
        executor.execute(buildJob(".*", 0.5f, 0.5f, 0.5f));
        
        // assert
        assertThat(getOutputJsons(TABLE_PUBLICATION), containsInAnyOrder(getExpectedJsons("publication.json")));
        assertThat(getOutputJsons(TABLE_PUB_GRANT), containsInAnyOrder(getExpectedJsons("pubgrant.json")));
        assertThat(getOutputJsons(TABLE_PUB_KEYWORD), containsInAnyOrder(getExpectedJsons("pubkeyword.json")));
        assertThat(getOutputJsons(TABLE_PUB_FULLTEXT), containsInAnyOrder(getExpectedJsons("pubfulltext.json")));
        assertThat(getOutputJsons(TABLE_CITATION), containsInAnyOrder(getExpectedJsons("citation.json")));
        assertThat(getOutputJsons(TABLE_PUB_CITATION), containsInAnyOrder(getExpectedJsons("pubcitation.json")));
        assertThat(getOutputJsons(TABLE_PUB_PDBCODE), containsInAnyOrder(getExpectedJsons("pubpdbcode.json")));

        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, outputJsonResourcesDir + "report.json", ReportEntry.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private List<JsonElement> getOutputJsons(String outputName) throws FileNotFoundException, IOException {
        return parseJsonLines(getJsonLines(new File(outputRootDirPath + '/' + outputName + '/')));
    }
    
    private static Object[] getExpectedJsons(String fileName) throws FileNotFoundException, IOException {
        return parseJsonLines(IOUtils.readLines(new FileReader(outputJsonResourcesDir + fileName))).toArray();
    }
    
    private static List<JsonElement> parseJsonLines(List<String> jsonStrings) {
        JsonParser parser = new JsonParser();
        List<JsonElement> results = new ArrayList<>(jsonStrings.size());
        for (String jsonStr : jsonStrings) {
            if (StringUtils.isNotBlank(jsonStr)) {
                results.add(parser.parse(jsonStr));    
            }
        }
        return results;
    }
    
    private static List<String> getJsonLines(File dir) throws FileNotFoundException, IOException {
        List<String> results = new ArrayList<>();
        for (File currentFile : dir.listFiles()) {
            if (!currentFile.isDirectory() && !currentFile.getName().endsWith(".crc")) {
                results.addAll(IOUtils.readLines(new FileReader(currentFile)));    
            }
        }
        return results;
    }
    
    private void createEmptyInputDatastores() throws IOException {
        createMetadataDatastore(inputJsonResourcesDir + "empty.json");
        createDocumentTextDatastore(inputJsonResourcesDir + "empty.json");
        createProjectDatastore(inputJsonResourcesDir + "empty.json");
        createDocumentToProjectDatastore(inputJsonResourcesDir + "empty.json");
        createDocumentToPdbDatastore(inputJsonResourcesDir + "empty.json");
        createCitationDatastore(inputJsonResourcesDir + "empty.json");
    }
    
    private void createInputDatastores() throws IOException {
        createMetadataDatastore(inputJsonResourcesDir + "metadata.json");
        createDocumentTextDatastore(inputJsonResourcesDir + "document_text.json");
        createProjectDatastore(inputJsonResourcesDir + "project.json");
        createDocumentToProjectDatastore(inputJsonResourcesDir + "document_to_project.json");
        createDocumentToPdbDatastore(inputJsonResourcesDir + "document_to_pdb.json");
        createCitationDatastore(inputJsonResourcesDir + "citation.json");
    }
    
    private void createMetadataDatastore(String inputJsonLocation) throws IOException {
        createInputDatastore(inputJsonLocation, inputMetadataDirPath, ExtractedDocumentMetadataMergedWithOriginal.class);
    }
    
    private void createDocumentTextDatastore(String inputJsonLocation) throws IOException {
        createInputDatastore(inputJsonLocation, inputTextDirPath, DocumentText.class);
    }
    
    private void createProjectDatastore(String inputJsonLocation) throws IOException {
        createInputDatastore(inputJsonLocation, inputProjectDirPath, Project.class);
    }
    
    private void createDocumentToProjectDatastore(String inputJsonLocation) throws IOException {
        createInputDatastore(inputJsonLocation, inputDocumentToProjectDirPath, DocumentToProject.class);
    }
    
    private void createDocumentToPdbDatastore(String inputJsonLocation) throws IOException {
        createInputDatastore(inputJsonLocation, inputDocumentToPdbDirPath, DocumentToConceptId.class);
    }
    
    private void createCitationDatastore(String inputJsonLocation) throws IOException {
        createInputDatastore(inputJsonLocation, inputCitationDirPath, Citation.class);
    }
    
    private static <T extends GenericRecord> void createInputDatastore(String inputJsonLocation, String destinationDir, 
            Class<T> recordClass) throws IOException {
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputJsonLocation,
                recordClass), destinationDir, recordClass);
    }
    
    private SparkJob buildJob(String fundingClassWhitelist, float confidenceLevelCitationThreshold, 
            float confidenceLevelDocumentToProjectThreshold, float confidenceLevelDocumentToPdbThreshold) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("avro2rdb")

                .setMainClass(AvroToRdbTransformerJob.class)
                
                .addArg("-inputMetadataAvroPath", inputMetadataDirPath)
                .addArg("-inputTextAvroPath", inputTextDirPath)
                .addArg("-inputProjectAvroPath", inputProjectDirPath)
                .addArg("-inputDocumentToProjectAvroPath", inputDocumentToProjectDirPath)
                .addArg("-inputDocumentToPdbAvroPath", inputDocumentToPdbDirPath)
                .addArg("-inputCitationAvroPath", inputCitationDirPath)
                
                .addArg("-confidenceLevelCitationThreshold", String.valueOf(confidenceLevelCitationThreshold))
                .addArg("-confidenceLevelDocumentToProjectThreshold", String.valueOf(confidenceLevelDocumentToProjectThreshold))
                .addArg("-confidenceLevelDocumentToPdbThreshold", String.valueOf(confidenceLevelDocumentToPdbThreshold))
                .addArg("-fundingClassWhitelist", fundingClassWhitelist)
                
                .addArg("-databaseUrl", outputRootDirPath)
                .addArg("-databaseUserName", "irrelevant")
                .addArg("-databasePassword", "irrelevant")
                
                .addArg("-outputReportPath", reportDirPath)
                .addJobProperty("spark.driver.host", "localhost")
                
                .build();
        
        return sparkJob;
    }
}
