package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;

/**
 * {@link PatentMetadataRetrieverInputTransformerJob} test class.
 * 
 * @author mhorst
 *
 */
public class PatentMetadataRetrieverInputTransformerJobTest {
    
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private Path inputImportedPatentDir;
    private Path inputMatchedPatentDir;
    private Path outputDir;

    @BeforeEach
    public void before() {
        inputImportedPatentDir = workingDir.resolve("input-imported");
        inputMatchedPatentDir = workingDir.resolve("input-matched");
        outputDir = workingDir.resolve("output");
    }

    @Test
    public void shouldConvertAvroDatastoreForMetadataRetrieval() throws IOException {
        // given
        String inputImportedPatentPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/transformer/input_imported_patent.json");
        String inputMatchedPatentPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/transformer/input_matched_patent.json");

        String outputTransformedPatentPath = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/patent/data/retriever/transformer/output.json");

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