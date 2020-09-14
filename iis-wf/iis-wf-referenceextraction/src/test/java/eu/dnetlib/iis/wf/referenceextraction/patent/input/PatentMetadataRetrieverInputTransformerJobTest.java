package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import eu.dnetlib.iis.common.string.StringUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * {@link PatentMetadataRetrieverInputTransformerJob} test class.
 * 
 * @author mhorst
 *
 */
public class PatentMetadataRetrieverInputTransformerJobTest {
    
    private ClassLoader cl = getClass().getClassLoader();
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path inputImportedPatentDir;
    private Path inputMatchedPatentDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("patent-transformer");
        inputImportedPatentDir = workingDir.resolve("input-imported");
        inputMatchedPatentDir = workingDir.resolve("input-matched");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldConvertAvroDatastoreForMetadataRetrieval() throws IOException {
        // given
        String inputImportedPatentPath = StringUtils.decodeFromUtf8(Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/transformer/input_imported_patent.json"))
                .getFile());
        String inputMatchedPatentPath = StringUtils.decodeFromUtf8(Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/transformer/input_matched_patent.json"))
                .getFile());

        String outputTransformedPatentPath = StringUtils.decodeFromUtf8(Objects
                .requireNonNull(cl.getResource("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/transformer/output.json"))
                .getFile());
        
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputImportedPatentPath, ImportedPatent.class), inputImportedPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(inputMatchedPatentPath, DocumentToPatent.class), inputMatchedPatentDir.toString());

        SparkJob sparkJob = buildSparkJob();

        // when
        executor.execute(sparkJob);

        // then
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), outputTransformedPatentPath, ImportedPatent.class);
    }

    private SparkJob buildSparkJob() {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentMetadataRetrieverInputTransformerJob.class)
                .addArg("-inputImportedPatentPath", inputImportedPatentDir.toString())
                .addArg("-inputMatchedPatentPath", inputMatchedPatentDir.toString())
                .addArg("-outputPath", outputDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }

}