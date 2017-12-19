package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithSource;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * 
 * @author mhorst
 *
 */
public class WebCrawlerJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputAvroPath;
    
    private String outputAvroPath;
    
    @Before
    public void before() {
        workingDir = Files.createTempDir();
        inputAvroPath = workingDir + "/spark_webcrawler/input";
        outputAvroPath = workingDir + "/spark_webcrawler/output";
    }
    
    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void obtainPageSource() throws IOException {
        
        // given
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/webcrawler/document_to_softwareurl_with_source.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToSoftwareUrl.class), 
                inputAvroPath);
        
        // execute
        executor.execute(buildWebCrawlerJob(inputAvroPath, outputAvroPath));
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputAvroPath, jsonOutputFile, DocumentToSoftwareUrlWithSource.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildWebCrawlerJob(String inputAvroPath, String outputAvroPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName("Spark WebCrawler")
                .setMainClass(WebCrawlerJob.class)
                .addArg("-inputAvroPath", inputAvroPath)
                .addArg("-contentRetrieverClassName", 
                        "eu.dnetlib.iis.wf.referenceextraction.softwareurl.ClasspathContentRetriever")
                .addArg("-connectionTimeout", "0")
                .addArg("-readTimeout", "0")
                .addArg("-maxPageContentLength", "0")
                .addArg("-outputAvroPath", outputAvroPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
        
        return sparkJob;
    }
}
