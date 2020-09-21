package eu.dnetlib.iis.wf.citationmatching.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * 
 * @author madryk
 *
 */
public class CitationMatchingDirectJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String inputCSVFileLocation;
    
    private String outputDirPath;
    
    private String reportDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        inputDirPath = workingDir + "/spark_citation_matching_direct/input";
        outputDirPath = workingDir + "/spark_citation_matching_direct/output";
        reportDirPath = workingDir + "/spark_citation_matching_direct/report";
        inputCSVFileLocation = workingDir + "/PMC-ids.csv";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingDirect() throws IOException {
        
        
        // given

        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/input/documents.json");
        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/expected_output/citations.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/expected_output/report.json");
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);



        String csvInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/input/PMC-ids.csv");

        FileSystem fs = createLocalFileSystem();
        fs.copyFromLocalFile(new Path(csvInputFile), new Path(inputCSVFileLocation));
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, inputCSVFileLocation, outputDirPath, reportDirPath));
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, Citation.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
        
    }
    
    
    @Test
    public void citationMatchingDirect_MULTIPLE_SAME_DOI() throws IOException {
        
        // given

        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/input/documents_multiple_same_doi.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/expected_output/report_one_matched.json");
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, outputDirPath, reportDirPath));
        
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        
        assertEquals(1, citations.size());
        
        assertCitation(citations.get(0), is(new Utf8("id-1")), 8, isOneOf(new Utf8("id-2"), new Utf8("id-3"), new Utf8("id-4")));
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
    }
    
    
    @Test
    public void citationMatchingDirect_MULTIPLE_SAME_PMID() throws IOException {
        
        // given

        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/input/documents_multiple_same_pmid.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/expected_output/report_one_matched.json");
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, outputDirPath, reportDirPath));
        
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        
        assertEquals(1, citations.size());
        
        assertCitation(citations.get(0), is(new Utf8("id-1")), 8, isOneOf(new Utf8("id-2"), new Utf8("id-3"), new Utf8("id-4")));
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
    }
    
    
    @Test
    public void citationMatchingDirect_MULTIPLE_SAME_PMID_WITH_TYPE() throws IOException {
        
        // given

        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/input/documents_multiple_same_pmid_with_type.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/expected_output/report_one_matched.json");
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, outputDirPath, reportDirPath));
        
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        
        assertEquals(1, citations.size());
        
        assertCitation(citations.get(0), is(new Utf8("id-1")), 8, is(new Utf8("id-3")));
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
    }
    
    @Test
    public void citationMatchingDirect_MAPPABLE_PMID_TO_PMC() throws IOException {
        
        // given

        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/input/documents_mappable_pmid_to_pmc.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/expected_output/report_one_matched.json");
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);

        String csvInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/direct/data/input/PMC-ids.csv");

        FileSystem fs = createLocalFileSystem();
        fs.copyFromLocalFile(new Path(csvInputFile), new Path(inputCSVFileLocation));
        
        // execute
        
        executor.execute(buildCitationMatchingDirectJob(inputDirPath, inputCSVFileLocation, outputDirPath, reportDirPath));
        
        
        // assert
        
        List<Citation> citations = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        
        assertEquals(1, citations.size());
        
        assertCitation(citations.get(0), is(new Utf8("id-1")), 8, is(new Utf8("id-2")));
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void assertCitation(Citation citation, Matcher<? super CharSequence> sourceDocumentIdMatcher, Integer position, Matcher<? super CharSequence> destinationDocumentIdMatcher) {
        
        CitationEntry citationEntry = citation.getEntry();
        
        assertThat(citation.getSourceDocumentId(), sourceDocumentIdMatcher);
        assertEquals(position, citationEntry.getPosition());
        assertThat(citationEntry.getDestinationDocumentId(), destinationDocumentIdMatcher);
        
        assertEquals(Float.valueOf(1f), citationEntry.getConfidenceLevel());
        assertNull(citationEntry.getRawText());
        assertThat(citationEntry.getExternalDestinationDocumentIds(), equalTo(Collections.EMPTY_MAP));
    }
    
    private SparkJob buildCitationMatchingDirectJob(String inputDirPath, String outputDirPath, String reportDirPath) {
        return buildCitationMatchingDirectJob(inputDirPath, "", outputDirPath, reportDirPath);
    }
    
    private SparkJob buildCitationMatchingDirectJob(String inputDirPath, String inputPmcIdsMappingCSV, String outputDirPath, String reportDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Citation Matching Direct")

                .setMainClass(CitationMatchingDirectJob.class)
                .addArg("-inputAvroPath", inputDirPath)
                .addArg("-inputPmcIdsMappingCSV", inputPmcIdsMappingCSV)
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-outputReportPath", reportDirPath)
                .addJobProperty("spark.driver.host", "localhost")
                
                .build();
        
        return sparkJob;
    }
    
    private static FileSystem createLocalFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        return FileSystem.get(conf);
    }
}
