package eu.dnetlib.iis.wf.documentsclassification;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * @author Łukasz Dumiszewski
 */

public class DocumentClassificationJobTest {

    private static final Logger log = LoggerFactory.getLogger(DocumentClassificationJobTest.class);
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    private static String scriptDirPath;
    
    
    @BeforeClass
    public static void beforeClass() throws IOException {

        scriptDirPath = DocumentClassificationJobTest.class.getResource("/eu/dnetlib/iis/wf/documentsclassification/oozie_app/lib/scripts").getPath();
        copyMadis(scriptDirPath + "/madis");

    }
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
        inputDirPath = workingDir + "/document_classification/input";
        outputDirPath = workingDir + "/document_classification/output";
        
        
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void documentClassificationJob() throws IOException {
        
        
        // given
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/wf/documentsclassification/data/input/few_documents.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/documentsclassification/data/expected_output/few_document_to_document_classes.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        // execute & assert
        
        executeJobAndAssert(jsonOutputFile);
    }
        
    
    @Test
    public void documentClassificationJob_EMPTY_ABSTRACT() throws IOException {
        
        
        // given
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/wf/documentsclassification/data/input/documents_with_empty_abstract.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/documentsclassification/data/expected_output/empty.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        // execute & assert
        
        executeJobAndAssert(jsonOutputFile);
    }
    
    @Test
    public void documentClassificationJob_EMPTY() throws IOException {
        
        // given
        
        String avroInputFile = "src/test/resources/eu/dnetlib/iis/wf/documentsclassification/data/input/empty.avro";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/documentsclassification/data/expected_output/empty.json";
        
        
        FileUtils.copyFileToDirectory(new File(avroInputFile), new File(inputDirPath));
        

        // execute & assert
        
        executeJobAndAssert(jsonOutputFile);
    }
    

    
    //------------------------ PRIVATE --------------------------
    
    
    private void executeJobAndAssert(String jsonOutputFile) throws IOException {

        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(DocumentClassificationJob.class)
                                           .addArg("-inputAvroPath", inputDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .addArg("-scriptDirPath", scriptDirPath)
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, DocumentToDocumentClasses.class);
    }


    
    private static void copyMadis(String destDirectory) throws IOException {
        
        String directoryToScan = "/eu/dnetlib/iis/3rdparty/scripts/madis/";
        
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + directoryToScan + "**");
        
        for (Resource resource : resources) {
            
            if (resource.exists() & resource.isReadable() & (resource.contentLength() > 0 || resource.getFilename().equals("__init__.py"))) {
                
                URL url = resource.getURL();
                String urlString = url.toExternalForm();
                String targetName = urlString.substring(urlString.indexOf(directoryToScan)+directoryToScan.length());
                File destination = new File(destDirectory, targetName);
                FileUtils.copyURLToFile(url, destination);
            
                log.debug("Copied {} to {}", url, destination.getAbsolutePath());
            
            } else {
                log.debug("Did not copy, seems to be directory: {} ", resource.getDescription());
            
            }
        }
    }
}
