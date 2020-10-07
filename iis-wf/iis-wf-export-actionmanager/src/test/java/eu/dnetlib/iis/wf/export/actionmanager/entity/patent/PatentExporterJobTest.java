package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.common.utils.ListTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.export.actionmanager.entity.AtomicActionSerDeUtils;
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

@IntegrationTest
public class PatentExporterJobTest {
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    Path workingDir;

    private Path inputDocumentToPatentDir;
    private Path inputPatentDir;
    private Path outputRelationDir;
    private Path outputEntityDir;
    private Path outputReportDir;

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

    @BeforeEach
    public void before() {
        inputDocumentToPatentDir = workingDir.resolve("input").resolve("document_to_patent");
        inputPatentDir = workingDir.resolve("input").resolve("patent");
        outputRelationDir = workingDir.resolve("output").resolve("relation");
        outputEntityDir = workingDir.resolve("output").resolve("entity");
        outputReportDir = workingDir.resolve("output").resolve("report");
    }

    @Test
    public void shouldNotExportEntitiesWhenConfidenceLevelIsBelowThreshold() throws IOException {
        //given
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_DOCUMENT_TO_PATENT_PATH), DocumentToPatent.class),
                inputDocumentToPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(ClassPathResourceProvider.getResourcePath(INPUT_PATENT_PATH), Patent.class),
                inputPatentDir.toString());
        SparkJob sparkJob = buildSparkJob(0.99);

        //when
        executor.execute(sparkJob);

        //then
        List<AtomicAction<Relation>> actualRelationActions = ListTestUtils
                .readValues(outputRelationDir.toString(), text -> {
                    try {
                        return AtomicActionSerDeUtils.deserializeAction(text.toString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        assertEquals(0, actualRelationActions.size());

        List<AtomicAction<Publication>> actualEntityActions = ListTestUtils
                .readValues(outputEntityDir.toString(), text -> {
                    try {
                        return AtomicActionSerDeUtils.deserializeAction(text.toString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        assertEquals(0, actualEntityActions.size());

        assertCountersInReport(0, 0, 0);
    }

    @Test
    public void shouldExportEntitiesWhenConfidenceLevelIsAboveThreshold() throws IOException {
        //given
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_DOCUMENT_TO_PATENT_PATH), DocumentToPatent.class),
                inputDocumentToPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_PATENT_PATH), Patent.class),
                inputPatentDir.toString());
        SparkJob sparkJob = buildSparkJob(0.5);
        
        //when
        executor.execute(sparkJob);

        //then
        //relations
        List<AtomicAction<Relation>> actualRelationActions = ListTestUtils
                .readValues(outputRelationDir.toString(), text -> {
                    try {
                        return AtomicActionSerDeUtils.deserializeAction(text.toString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        assertEquals(6, actualRelationActions.size());

        actualRelationActions.forEach(action -> verifyAction(action, Relation.class));

        // entities
        List<AtomicAction<Publication>> actualEntityActions = ListTestUtils
                .readValues(outputEntityDir.toString(), text -> {
                    try {
                        return AtomicActionSerDeUtils.deserializeAction(text.toString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        assertEquals(actualEntityActions.size(), 2);

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
                inputDocumentToPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(
                        ClassPathResourceProvider.getResourcePath(INPUT_PATENT_NULLCHECK_PATH), Patent.class),
                inputPatentDir.toString());
        SparkJob sparkJob = buildSparkJob(0.5);

        //when
        executor.execute(sparkJob);

        //then - checking only if no exception is thrown
        assertCountersInReport(0, 0, 0);
    }

    private SparkJob buildSparkJob(Double trustLevelThreshold) {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentExporterJob.class)
                .addArg("-inputDocumentToPatentPath", inputDocumentToPatentDir.toString())
                .addArg("-inputPatentPath", inputPatentDir.toString())
                .addArg("-trustLevelThreshold", String.valueOf(trustLevelThreshold))
                .addArg("-patentDateOfCollection", PATENT_DATE_OF_COLLECTION)
                .addArg("-patentEpoUrlRoot", PATENT_EPO_URL_ROOT)
                .addArg("-outputRelationPath", outputRelationDir.toString())
                .addArg("-outputEntityPath", outputEntityDir.toString())
                .addArg("-outputReportPath", outputReportDir.toString())
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
        List<ReportEntry> reportEntries = AvroTestUtils.readLocalAvroDataStore(outputReportDir.toString());
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
