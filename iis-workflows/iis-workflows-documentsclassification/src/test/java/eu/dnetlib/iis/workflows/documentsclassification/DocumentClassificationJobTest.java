package eu.dnetlib.iis.workflows.documentsclassification;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import com.google.common.io.Files;

import eu.dnetlib.iis.core.common.AvroAssertTestUtil;
import eu.dnetlib.iis.core.common.AvroTestUtils;
import eu.dnetlib.iis.core.common.JsonAvroTestUtils;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * @author Łukasz Dumiszewski
 */

public class DocumentClassificationJobTest {

    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void documentClassificationJob() throws IOException {
        
        
        // given
        
        String inputDirPath = workingDir + "/document_classification/input";
        String outputDirPath = workingDir + "/document_classification/output";
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/workflows/documentsclassification/data/input/few_documents.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/workflows/documentsclassification/data/expected_output/few_document_to_document_classes.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        String destDirectory = this.getClass().getResource("/eu/dnetlib/iis/workflows/documentsclassification/oozie_app/lib/scripts").getPath();
        
        
        copyMadis(destDirectory + "/madis");
        
        
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(DocumentClassificationJob.class)
                                           .addArg("-inputAvroPath", inputDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .addArg("-scriptDirPath", destDirectory)
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, DocumentToDocumentClasses.class);
    }
        
    
    
    //------------------------ PRIVATE --------------------------
    
    private void copyMadis(String destDirectory) throws IOException {
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
                System.out.println("Copied " + url + " to " + destination.getAbsolutePath());
            } else {
                System.out.println("Did not copy, seems to be directory: " + resource.getDescription());
            }
        }
    }
}
