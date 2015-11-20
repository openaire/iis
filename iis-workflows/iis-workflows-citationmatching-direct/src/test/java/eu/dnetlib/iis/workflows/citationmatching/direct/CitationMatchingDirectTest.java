package eu.dnetlib.iis.workflows.citationmatching.direct;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.spark.test.SparkJob;
import eu.dnetlib.iis.common.spark.test.SparkJobBuilder;
import eu.dnetlib.iis.common.spark.test.SparkJobExecutor;
import eu.dnetlib.iis.core.common.AvroAssertTestUtil;
import eu.dnetlib.iis.core.common.AvroTestUtils;
import eu.dnetlib.iis.core.common.JsonAvroTestUtils;

public class CitationMatchingDirectTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        inputDirPath = workingDir + "/spark_citation_matching_direct/input";
        outputDirPath = workingDir + "/spark_citation_matching_direct/output";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingDirect() throws IOException {
        
        
        // given
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/direct/input_data/citation_metadata.json";
//        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/direct/input_data/direct_citation.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentMetadata.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, outputDirPath));
        
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertThat(citations, containsInAnyOrder(
                new Citation("id-1", 1, "id-2"), 
                new Citation("id-1", 2, "id-3"), 
                new Citation("id-4", 5, "id-5"),
                new Citation("id-4", 6, "id-5")));
        
    }
    
    @Test
    public void citationMatchingDirect_MULTIPLE_SAME_DOI() throws IOException {
        
        // given
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/direct/input_data/citation_metadata_multiple_same_doi.json";
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentMetadata.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, outputDirPath));
        
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertEquals(1, citations.size());
        assertEquals(new Utf8("id-1"), citations.get(0).getSourceDocumentId());
        assertEquals(Integer.valueOf(8), citations.get(0).getPosition());
        assertThat(citations.get(0).getDestinationDocumentId(), isOneOf(new Utf8("id-2"), new Utf8("id-3"), new Utf8("id-4")));
        
    }
    
    @Test
    public void citationMatchingDirect_MULTIPLE_SAME_PMID() throws IOException {
        
        // given
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/direct/input_data/citation_metadata_multiple_same_pmid.json";
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentMetadata.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, outputDirPath));
        
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertEquals(1, citations.size());
        assertEquals(new Utf8("id-1"), citations.get(0).getSourceDocumentId());
        assertEquals(Integer.valueOf(8), citations.get(0).getPosition());
        assertThat(citations.get(0).getDestinationDocumentId(), isOneOf(new Utf8("id-2"), new Utf8("id-3"), new Utf8("id-4")));
        
    }
    
    @Test
    public void citationMatchingDirect_MULTIPLE_SAME_PMID_WITH_TYPE() throws IOException {
        
        // given
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/direct/input_data/citation_metadata_multiple_same_pmid_with_type.json";
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentMetadata.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, outputDirPath));
        
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertEquals(1, citations.size());
        assertEquals(new Utf8("id-1"), citations.get(0).getSourceDocumentId());
        assertEquals(Integer.valueOf(8), citations.get(0).getPosition());
        assertThat(citations.get(0).getDestinationDocumentId(), is(new Utf8("id-3")));
        
    }
    
    
    
    private SparkJob buildCitationMatchingDirectJob(String inputDirPath, String outputDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Citation Matching Direct")

                .setMainClass(CitationMatchingDirect.class)
                .addArg("-inputAvroPath", inputDirPath)
                .addArg("-outputAvroPath", outputDirPath)
                
                .build();
        
        return sparkJob;
    }
}
