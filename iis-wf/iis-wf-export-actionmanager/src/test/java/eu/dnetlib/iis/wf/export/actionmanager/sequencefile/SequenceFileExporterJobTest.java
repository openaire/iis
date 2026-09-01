package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.IteratorUtils;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionDeserializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link SequenceFileExporterJob}.
 *
 * @author mhorst
 */
@SlowTest
public class SequenceFileExporterJobTest {

    private static final String FACTORY_MOCK =
            MockDocumentProjectActionBuilderFactory.class.getName();

    private static final String FACTORY_REAL =
            "eu.dnetlib.iis.wf.export.actionmanager.module.DocumentToProjectActionBuilderModuleFactory";

    private static final String SCHEMA_DOCUMENT_TO_PROJECT =
            DocumentToProject.class.getName();

    private final SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private String inputPath;
    private String outputPath;

    // ------------------------------ SETUP --------------------------------

    @BeforeEach
    public void before() {
        inputPath  = workingDir.resolve("input").toString();
        outputPath = workingDir.resolve("output").toString();
    }

    // ------------------------------ TESTS --------------------------------

    @Test
    public void exportProducesAtomicActionForEachRecord() throws Exception {
        // given
        DocumentToProject record = DocumentToProject.newBuilder()
                .setDocumentId("doc-1").setProjectId("proj-1").setConfidenceLevel(0.9f).build();
        AvroTestUtils.createLocalAvroDataStore(Collections.singletonList(record), inputPath);

        // execute
        executor.execute(buildJob(inputPath, outputPath, FACTORY_MOCK, SCHEMA_DOCUMENT_TO_PROJECT));

        // assert
        List<AtomicAction<Relation>> actions = readRelationActions(outputPath);
        assertEquals(1, actions.size());

        Relation payload = actions.get(0).getPayload();
        assertEquals(Relation.class, actions.get(0).getClazz());
        assertEquals("doc-1", payload.getSource());
        assertEquals("proj-1", payload.getTarget());
    }

    @Test
    public void exportMultipleRecordsProducesOneActionPerRecord() throws Exception {
        // given
        DocumentToProject r1 = DocumentToProject.newBuilder()
                .setDocumentId("doc-1").setProjectId("proj-1").setConfidenceLevel(0.9f).build();
        DocumentToProject r2 = DocumentToProject.newBuilder()
                .setDocumentId("doc-2").setProjectId("proj-2").setConfidenceLevel(0.8f).build();
        AvroTestUtils.createLocalAvroDataStore(List.of(r1, r2), inputPath);

        // execute
        executor.execute(buildJob(inputPath, outputPath, FACTORY_MOCK, SCHEMA_DOCUMENT_TO_PROJECT));

        // assert
        assertEquals(2, readRelationActions(outputPath).size());
    }

    @Test
    public void undefinedInputSkipsExecution() {
        // execute
        executor.execute(buildJob("$UNDEFINED$", outputPath, FACTORY_MOCK, SCHEMA_DOCUMENT_TO_PROJECT));

        // assert — output directory must not be created when input is $UNDEFINED$
        assertFalse(new File(outputPath).exists(),
                "Output directory must not be created when inputPath is $UNDEFINED$");
    }

    @Test
    public void exportAboveTrustLevelThresholdRetainsRecord() throws Exception {
        // given — confidence 0.9, threshold below that → record should pass
        DocumentToProject record = DocumentToProject.newBuilder()
                .setDocumentId("doc-3").setProjectId("proj-3").setConfidenceLevel(0.9f).build();
        AvroTestUtils.createLocalAvroDataStore(Collections.singletonList(record), inputPath);

        // execute — real factory with threshold well below confidence level
        executor.execute(buildJobWithDynamicParam(inputPath, outputPath, FACTORY_REAL,
                SCHEMA_DOCUMENT_TO_PROJECT,
                "export.trust.level.threshold", "0.5"));

        // assert — real factory produces 2 bidirectional relations per record
        assertEquals(2, readRelationActions(outputPath).size());
    }

    @Test
    public void exportBelowTrustLevelThresholdFiltersRecord() throws Exception {
        // given — confidence 0.5, threshold above that → record should be filtered
        DocumentToProject record = DocumentToProject.newBuilder()
                .setDocumentId("doc-4").setProjectId("proj-4").setConfidenceLevel(0.5f).build();
        AvroTestUtils.createLocalAvroDataStore(Collections.singletonList(record), inputPath);

        // execute — real factory with threshold higher than the record's effective trust level
        executor.execute(buildJobWithDynamicParam(inputPath, outputPath, FACTORY_REAL,
                SCHEMA_DOCUMENT_TO_PROJECT,
                "export.trust.level.threshold", "1.0"));

        // assert — all records filtered, no actions emitted
        assertEquals(0, readRelationActions(outputPath).size());
    }

    @Test
    public void exportWithNumberOfOutputFilesRepartitionsOutput() throws Exception {
        // given
        DocumentToProject record = DocumentToProject.newBuilder()
                .setDocumentId("doc-5").setProjectId("proj-5").setConfidenceLevel(0.9f).build();
        AvroTestUtils.createLocalAvroDataStore(Collections.singletonList(record), inputPath);

        // execute — repartition into exactly 1 output file
        executor.execute(SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(SequenceFileExporterJob.class)
                .addArg("-inputPath",                      inputPath)
                .addArg("-outputPath",                     outputPath)
                .addArg("-actionBuilderFactoryClassName",  FACTORY_MOCK)
                .addArg("-inputAvroSchemaClass",           SCHEMA_DOCUMENT_TO_PROJECT)
                .addArg("-numberOfOutputFiles",            "1")
                .addJobProperty("spark.driver.host",       "localhost")
                .build());

        // assert — action is still present despite repartitioning
        List<AtomicAction<Relation>> actions = readRelationActions(outputPath);
        assertEquals(1, actions.size());
        assertEquals("doc-5", actions.get(0).getPayload().getSource());
    }

    // ------------------------------ PRIVATE --------------------------------

    private SparkJob buildJob(String in, String out, String factory, String schema) {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(SequenceFileExporterJob.class)
                .addArg("-inputPath",                     in)
                .addArg("-outputPath",                    out)
                .addArg("-actionBuilderFactoryClassName", factory)
                .addArg("-inputAvroSchemaClass",          schema)
                .addJobProperty("spark.driver.host",      "localhost")
                .build();
    }

    private SparkJob buildJobWithDynamicParam(String in, String out, String factory, String schema,
                                              String paramKey, String paramValue) {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(SequenceFileExporterJob.class)
                .addArg("-inputPath",                     in)
                .addArg("-outputPath",                    out)
                .addArg("-actionBuilderFactoryClassName", factory)
                .addArg("-inputAvroSchemaClass",          schema)
                .addArg("-D" + paramKey,                  paramValue)
                .addJobProperty("spark.driver.host",      "localhost")
                .build();
    }

    private static List<AtomicAction<Relation>> readRelationActions(String path) throws Exception {
        return IteratorUtils.toList(
                SequenceFileTextValueReader.fromFile(path),
                text -> AtomicActionDeserializationUtils.deserializeAction(text.toString()));
    }
}
