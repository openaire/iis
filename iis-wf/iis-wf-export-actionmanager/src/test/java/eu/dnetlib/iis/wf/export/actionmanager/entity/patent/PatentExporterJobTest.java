package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.actionmanager.ActionManagerConstants;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.RelTypeProtos;
import eu.dnetlib.data.proto.ResultResultProtos;
import eu.dnetlib.data.proto.TypeProtos;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.common.utils.ListTestUtils;
import eu.dnetlib.iis.common.utils.RDDTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class PatentExporterJobTest {
    private ClassLoader cl = getClass().getClassLoader();
    private SparkJobExecutor executor = new SparkJobExecutor();
    private Path workingDir;
    private Path inputDocumentToPatentDir;
    private Path inputPatentDir;
    private Path outputRelationDir;
    private Path outputEntityDir;
    private Path outputReportDir;

    private static final String INPUT_DOCUMENT_TO_PATENT_PATH = "eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default/input/document_to_patent.json";
    private static final String INPUT_PATENT_PATH = "eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default/input/patent.json";

    private static final String RELATION_ACTION_SET_ID = "relation-actionset-id";
    private static final String ENTITY_ACTION_SET_ID = "entity-actionset-id";
    private static final String EPO_BASE_URL = "https://register.epo.org/application?number=";

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("patent_exporter");
        inputDocumentToPatentDir = workingDir.resolve("input").resolve("document_to_patent");
        inputPatentDir = workingDir.resolve("input").resolve("patent");
        outputRelationDir = workingDir.resolve("output").resolve("relation");
        outputEntityDir = workingDir.resolve("output").resolve("entity");
        outputReportDir = workingDir.resolve("output").resolve("report");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldNotExportEntitiesWhenConfidenceLevelIsBelowThreshold() throws IOException {
        //given
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(Objects.requireNonNull(cl.getResource(INPUT_DOCUMENT_TO_PATENT_PATH)).getFile(), DocumentToPatent.class),
                inputDocumentToPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(Objects.requireNonNull(cl.getResource(INPUT_PATENT_PATH)).getFile(), Patent.class),
                inputPatentDir.toString());
        SparkJob sparkJob = buildSparkJob(0.99);

        //when
        executor.execute(sparkJob);

        //then
        List<AtomicAction> actualRelationActions = RDDTestUtils
                .readValues(outputRelationDir.toString(), text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(0, actualRelationActions.size());

        List<AtomicAction> actualEntityActions = RDDTestUtils
                .readValues(outputEntityDir.toString(), text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(0, actualEntityActions.size());

        assertCountersInReport(0, 0, 0);
    }

    @Test
    public void shouldExportEntitiesWhenConfidenceLevelIsAboveThreshold() throws IOException {
        //given
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(Objects.requireNonNull(cl.getResource(INPUT_DOCUMENT_TO_PATENT_PATH)).getFile(), DocumentToPatent.class),
                inputDocumentToPatentDir.toString());
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(Objects.requireNonNull(cl.getResource(INPUT_PATENT_PATH)).getFile(), Patent.class),
                inputPatentDir.toString());
        SparkJob sparkJob = buildSparkJob(0.5);

        //when
        executor.execute(sparkJob);

        //then
        //relations
        List<AtomicAction> actualRelationActions = RDDTestUtils
                .readValues(outputRelationDir.toString(), text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(6, actualRelationActions.size());

        String expectedRelationTargetColumnFamily = String.format("%s_%s_%s", RelTypeProtos.RelType.resultResult.name(),
                RelTypeProtos.SubRelType.relationship.name(), ResultResultProtos.ResultResult.Relationship.RelName.isRelatedTo.name());
        actualRelationActions.forEach(action -> verifyAction(action, RELATION_ACTION_SET_ID, expectedRelationTargetColumnFamily));
        List<Pair<String, String>> expectedRelationsTargetRowKeyAndTargetColumnPairs = Arrays.asList(
                Pair.of("document1", "50|openaire____::9fce164deeabdfd83f697e45b1aeea3d"),
                Pair.of("50|openaire____::9fce164deeabdfd83f697e45b1aeea3d", "document1"),
                Pair.of("document2", "50|openaire____::9fce164deeabdfd83f697e45b1aeea3d"),
                Pair.of("50|openaire____::9fce164deeabdfd83f697e45b1aeea3d", "document2"),
                Pair.of("document2", "50|openaire____::702195de7e0ff019d206b3eef73f0f21"),
                Pair.of("50|openaire____::702195de7e0ff019d206b3eef73f0f21", "document2"));
        List<Pair<String, String>> actualRelationsTargetRowKeyAndTargetColumnPairs = actualRelationActions.stream()
                .map(x -> Pair.of(x.getTargetRowKey(), x.getTargetColumn()))
                .sorted()
                .collect(Collectors.toList());
        ListTestUtils
                .compareLists(
                        actualRelationsTargetRowKeyAndTargetColumnPairs.stream().sorted().collect(Collectors.toList()),
                        expectedRelationsTargetRowKeyAndTargetColumnPairs.stream().sorted().collect(Collectors.toList()));

        // entities
        List<AtomicAction> actualEntityActions = RDDTestUtils
                .readValues(outputEntityDir.toString(), text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(actualEntityActions.size(), 2);

        String expectedEntityTargetColumnFamily = TypeProtos.Type.result.name();
        actualEntityActions.forEach(action -> verifyAction(action, ENTITY_ACTION_SET_ID, expectedEntityTargetColumnFamily));

        String expectedEntityTargetColumn = new String(InfoSpaceConstants.QUALIFIER_BODY, StandardCharsets.UTF_8);
        List<Pair<String, String>> expectedEntityTargetRowKeyAndTargetColumnPairs = Arrays.asList(
                Pair.of("50|openaire____::9fce164deeabdfd83f697e45b1aeea3d", expectedEntityTargetColumn),
                Pair.of("50|openaire____::702195de7e0ff019d206b3eef73f0f21", expectedEntityTargetColumn));
        List<Pair<String, String>> actualEntityTargetRowKeyAndTargetColumnPairs = actualEntityActions.stream()
                .map(x -> Pair.of(x.getTargetRowKey(), x.getTargetColumn()))
                .sorted()
                .collect(Collectors.toList());
        ListTestUtils
                .compareLists(
                        actualEntityTargetRowKeyAndTargetColumnPairs.stream().sorted().collect(Collectors.toList()),
                        expectedEntityTargetRowKeyAndTargetColumnPairs.stream().sorted().collect(Collectors.toList()));

        //report
        assertCountersInReport(3, 2, 2);
    }

    private SparkJob buildSparkJob(Double trustLevelThreshold) {
        return SparkJobBuilder.create()
                .setAppName(getClass().getName())
                .setMainClass(PatentExporterJob.class)
                .addArg("-inputDocumentToPatentPath", inputDocumentToPatentDir.toString())
                .addArg("-inputPatentPath", inputPatentDir.toString())
                .addArg("-relationActionSetId", RELATION_ACTION_SET_ID)
                .addArg("-entityActionSetId", ENTITY_ACTION_SET_ID)
                .addArg("-trustLevelThreshold", String.valueOf(trustLevelThreshold))
                .addArg("-outputRelationPath", outputRelationDir.toString())
                .addArg("-epoBaseUrl", EPO_BASE_URL)
                .addArg("-outputEntityPath", outputEntityDir.toString())
                .addArg("-outputReportPath", outputReportDir.toString())
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }

    private void verifyAction(AtomicAction action, String actionSetId, String expectedTargetColumnFamily) {
        assertEquals(ActionManagerConstants.ACTION_TYPE.aac, action.getActionType());
        assertEquals(actionSetId, action.getRawSet());
        assertTrue(StringUtils.isNotBlank(action.getRowKey()));
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getId(), action.getAgent().getId());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getName(), action.getAgent().getName());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getType(), action.getAgent().getType());
        assertEquals(expectedTargetColumnFamily, action.getTargetColumnFamily());
        assertTrue(action.getTargetValue().length > 0);
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
