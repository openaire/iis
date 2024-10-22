package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.IteratorUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionDeserializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SlowTest
public class PatentExporterJobTest {

    private final SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private String inputDocumentToPatentPath;
    private String inputPatentPath;
    private String outputRelationPath;
    private String outputEntityPath;
    private String outputReportPath;

    private static final String INPUT_DOCUMENT_TO_PATENT_PATH =
            "eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default/input/document_to_patent.json";
    private static final String INPUT_PATENT_PATH =
            "eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default/input/patent.json";

    private static final String INPUT_DOCUMENT_TO_PATENT_NULLCHECK_PATH =
            "eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default/input/nullcheck/document_to_patent.json";
    private static final String INPUT_PATENT_NULLCHECK_PATH =
            "eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default/input/nullcheck/patent.json";

    private static final String PATENT_DATE_OF_COLLECTION = "2019-11-20T23:59";
    private static final String PATENT_EPO_URL_ROOT = "https://register.epo.org/application?number=";
    
    private static final String RELATION_COLLECTED_FROM_VALUE = "someRepo";

    @BeforeEach
    public void before() {
        inputDocumentToPatentPath = workingDir.resolve("patent_exporter").resolve("input_document_to_patent").toString();
        inputPatentPath = workingDir.resolve("patent_exporter").resolve("input_patent").toString();
        outputRelationPath = workingDir.resolve("patent_exporter").resolve("output_relation").toString();
        outputEntityPath = workingDir.resolve("patent_exporter").resolve("output_entity").toString();
        outputReportPath = workingDir.resolve("patent_exporter").resolve("output_report").toString();
    }

    @Test
    public void shouldNotExportEntitiesWhenConfidenceLevelIsBelowThreshold() throws IOException {
        //given
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_DOCUMENT_TO_PATENT_PATH), DocumentToPatent.class),
                inputDocumentToPatentPath);
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(ClassPathResourceProvider.getResourcePath(INPUT_PATENT_PATH), Patent.class),
                inputPatentPath);
        SparkJob sparkJob = buildSparkJob("0.99");

        //when
        executor.execute(sparkJob);

        //then
        List<AtomicAction<Relation>> actualRelationActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputRelationPath),
                x -> AtomicActionDeserializationUtils.deserializeAction(x.toString()));
        assertEquals(0, actualRelationActions.size());

        List<AtomicAction<Publication>> actualEntityActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputEntityPath),
                x -> AtomicActionDeserializationUtils.deserializeAction(x.toString()));
        assertEquals(0, actualEntityActions.size());

        assertCountersInReport(0, 0, 0);
    }

    @Test
    public void shouldExportEntitiesWhenConfidenceLevelIsAboveThreshold() throws IOException {
        //given
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_DOCUMENT_TO_PATENT_PATH), DocumentToPatent.class),
                inputDocumentToPatentPath);
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_PATENT_PATH), Patent.class),
                inputPatentPath);
        SparkJob sparkJob = buildSparkJob("0.5");

        //when
        executor.execute(sparkJob);

        //then
        //relations
        List<AtomicAction<Relation>> actualRelationActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputRelationPath),
                x -> AtomicActionDeserializationUtils.deserializeAction(x.toString()));
        assertEquals(6, actualRelationActions.size());

        actualRelationActions.forEach(action -> verifyAction(action, Relation.class));

        // entities
        List<AtomicAction<Publication>> actualEntityActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputEntityPath),
                x -> AtomicActionDeserializationUtils.deserializeAction(x.toString()));
        assertEquals(2, actualEntityActions.size());

        actualEntityActions.forEach(action -> verifyAction(action, Publication.class));

        //report
        assertCountersInReport(3, 2, 2);
    }

    @Test
    public void shouldNotExportEntitiesNorRelationsWhenEntityTitleIsNull() throws IOException {
        //given
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_DOCUMENT_TO_PATENT_NULLCHECK_PATH), DocumentToPatent.class),
                inputDocumentToPatentPath);
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_PATENT_NULLCHECK_PATH), Patent.class),
                inputPatentPath);
        SparkJob sparkJob = buildSparkJob("0.5");

        //when
        executor.execute(sparkJob);

        //then - checking only if no exception is thrown
        assertCountersInReport(0, 0, 0);
    }

    private SparkJob buildSparkJob(String trustLevelThreshold) {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentExporterJob.class)
                .addArg("-inputDocumentToPatentPath", inputDocumentToPatentPath)
                .addArg("-inputPatentPath", inputPatentPath)
                .addArg("-trustLevelThreshold", trustLevelThreshold)
                .addArg("-collectedFromValue", RELATION_COLLECTED_FROM_VALUE)
                .addArg("-patentDateOfCollection", PATENT_DATE_OF_COLLECTION)
                .addArg("-patentEpoUrlRoot", PATENT_EPO_URL_ROOT)
                .addArg("-outputRelationPath", outputRelationPath)
                .addArg("-outputEntityPath", outputEntityPath)
                .addArg("-outputReportPath", outputReportPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }

    private void verifyAction(AtomicAction<?> action, Class<?> clazz) {
        assertEquals(clazz, action.getClazz());
        assertNotNull(action.getPayload());
        assertEquals(clazz, action.getPayload().getClass());
        // comparing action payload is out of the scope of this test
    }

    private void assertCountersInReport(Integer expectedReferencesCount,
                                        Integer expectedEntitiesCount,
                                        Integer expectedDistinctPubsReferencesCount) throws IOException {
        List<ReportEntry> reportEntries = AvroTestUtils.readLocalAvroDataStore(outputReportPath);
        assertEquals(3, reportEntries.size());

        assertEquals(ReportEntryType.COUNTER, reportEntries.get(0).getType());
        assertEquals(PatentExportCounterReporter.PATENT_REFERENCES_COUNTER, reportEntries.get(0).getKey().toString());
        assertEquals(expectedReferencesCount, Integer.valueOf(reportEntries.get(0).getValue().toString()));

        assertEquals(ReportEntryType.COUNTER, reportEntries.get(1).getType());
        assertEquals(PatentExportCounterReporter.EXPORTED_PATENT_ENTITIES_COUNTER, reportEntries.get(1).getKey().toString());
        assertEquals(expectedEntitiesCount, Integer.valueOf(reportEntries.get(1).getValue().toString()));

        assertEquals(ReportEntryType.COUNTER, reportEntries.get(2).getType());
        assertEquals(PatentExportCounterReporter.DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER, reportEntries.get(2).getKey().toString());
        assertEquals(expectedDistinctPubsReferencesCount, Integer.valueOf(reportEntries.get(2).getValue().toString()));
    }

}
