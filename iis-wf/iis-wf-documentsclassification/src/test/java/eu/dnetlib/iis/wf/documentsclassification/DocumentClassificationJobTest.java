package eu.dnetlib.iis.wf.documentsclassification;

import eu.dnetlib.iis.common.StaticResourceProvider;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

/**
 * @author Łukasz Dumiszewski
 */

@Ignore
public class DocumentClassificationJobTest {

    private static final Logger log = LoggerFactory.getLogger(DocumentClassificationJobTest.class);

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    private static String scriptDirPath;
    
    private String reportDirPath;

    @BeforeClass
    public static void beforeClass() throws IOException {
        scriptDirPath = StaticResourceProvider
                .getResourcePath("/eu/dnetlib/iis/wf/documentsclassification/oozie_app/lib/scripts");
        copyMadis(scriptDirPath + "/madis");
    }
    
    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("DocumentClassificationJobTest_").toFile();
        inputDirPath = workingDir + "/document_classification/input";
        outputDirPath = workingDir + "/document_classification/output";
        reportDirPath = workingDir + "/document_classification/report";
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }

    //------------------------ TESTS --------------------------
    
    @Test
    public void documentClassificationJob() throws IOException {
        // given
        String jsonInputFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/input/few_documents.json");
        String jsonOutputFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/expected_output/few_document_to_document_classes.json");
        String jsonReportFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/expected_output/few_document_report.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);

        // execute
        executeJob();

        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, DocumentToDocumentClasses.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
    }

    @Test
    public void documentClassificationJob_EMPTY_ABSTRACT() throws IOException {
        // given
        String jsonInputFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/input/documents_with_empty_abstract.json");
        String jsonOutputFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/expected_output/empty.json");
        String jsonReportFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/expected_output/empty_report.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);

        // execute
        executeJob();

        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, DocumentToDocumentClasses.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
    }
    
    @Test
    public void documentClassificationJob_EMPTY() throws IOException {
        // given
        String avroInputFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/input/empty.avro");
        String jsonOutputFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/expected_output/empty.json");
        String jsonReportFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/documentsclassification/data/expected_output/empty_report.json");

        FileUtils.copyFileToDirectory(new File(avroInputFile), new File(inputDirPath));

        // execute
        executeJob();

        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, DocumentToDocumentClasses.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
    }

    //------------------------ PRIVATE --------------------------

    private void executeJob() {
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           .setAppName(getClass().getName())
                                           .setMainClass(DocumentClassificationJob.class)
                                           .addArg("-inputAvroPath", inputDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .addArg("-scriptDirPath", scriptDirPath)
                                           .addArg("-outputReportPath", reportDirPath)
                                           .addJobProperty("spark.driver.host", "localhost")
                                           .build();
        executor.execute(sparkJob);
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
