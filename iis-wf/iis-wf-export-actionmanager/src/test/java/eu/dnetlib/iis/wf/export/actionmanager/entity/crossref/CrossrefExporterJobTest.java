package eu.dnetlib.iis.wf.export.actionmanager.entity.crossref;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.IteratorUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionDeserializationUtils;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * Test class for {@link CrossrefExporterJob}.
 *
 * @author mhorst
 */
@SlowTest
public class CrossrefExporterJobTest {

    private final SparkJobExecutor executor = new SparkJobExecutor();

    private String inputPath;
    private String outputEntityPath;
    private String outputRelationPath;
    private String reportPath;

    private static final String COUNTER_INPUT_RECORDS = "export.crossref.input.records";

    private static final String COUNTER_EXPORTED_ENTITIES = "export.crossref.entities";

    private static final String COUNTER_EXPORTED_UNIQUE_SOURCE_DOCS = "export.crossref.relations.uniqueSourceDocs";

    @BeforeAll
    static void initWritableTempDir() {
        String writableTmp = System.getenv("TMPDIR");
        if (writableTmp != null) {
            System.setProperty("java.io.tmpdir", writableTmp);
        }
    }

    @BeforeEach
    public void before() throws Exception {
        Path workingDir = Files.createTempDirectory(
                Path.of(System.getProperty("java.io.tmpdir")), "crossrefExporterTest");
        workingDir.toFile().deleteOnExit();
        inputPath = workingDir.resolve("input").toString();
        outputEntityPath = workingDir.resolve("output_entity").toString();
        outputRelationPath = workingDir.resolve("output_relation").toString();
        reportPath = workingDir.resolve("crossref_exporter").resolve("report").toString();
    }

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    @Test
    @DisplayName("Eligible references are exported as Publication entities and Relation links")
    public void exportEligibleReferences() throws IOException {

        // given
        String jsonInputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/export/actionmanager/entity/crossref/input/input_extracted_document_metadata.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadata.class),
                inputPath);

        // execute
        executor.execute(buildJob());

        // then - verify report
        List<ReportEntry> reportEntries = AvroTestUtils.readLocalAvroDataStore(reportPath);
        assertEquals(3, reportEntries.size());
        assertEquals(5L, Long.parseLong(reportEntries.get(0).getValue().toString()));
        assertEquals(COUNTER_INPUT_RECORDS, reportEntries.get(0).getKey().toString());
        assertEquals(5L, Long.parseLong(reportEntries.get(1).getValue().toString()));
        assertEquals(COUNTER_EXPORTED_ENTITIES, reportEntries.get(1).getKey().toString());
        assertEquals(3L, Long.parseLong(reportEntries.get(2).getValue().toString()));
        assertEquals(COUNTER_EXPORTED_UNIQUE_SOURCE_DOCS, reportEntries.get(2).getKey().toString());

        // then - verify entities
        List<AtomicAction<Publication>> capturedEntityActions = IteratorUtils
                .toList(SequenceFileTextValueReader.fromFile(outputEntityPath),
                        text -> AtomicActionDeserializationUtils.deserializeAction(text.toString()));
        assertEquals(5, capturedEntityActions.size());

        // verify each entity action has correct clazz and payload
        for (AtomicAction<Publication> action : capturedEntityActions) {
            assertEquals(Publication.class, action.getClazz());
            assertNotNull(action.getPayload());
        }

        // --- Verify first entity (pub1, ref 1: "Introduction to AI") ---
        AtomicAction<Publication> entity0 = findEntityByTitle(capturedEntityActions, "Introduction to AI");
        assertNotNull(entity0, "expected entity for 'Introduction to AI'");
        Publication pub0 = entity0.getPayload();
        assertTrue(pub0.getId().startsWith("50||mutecitation::"),
                "id should start with '50||mutecitation::', got: " + pub0.getId());
        assertEquals("publication", pub0.getResulttype().getClassid());
        assertEquals("publication", pub0.getResulttype().getClassname());
        assertTrue(pub0.getDataInfo().getInvisible());
        assertTrue(pub0.getDataInfo().getInferred());
        assertEquals("0.7", pub0.getDataInfo().getTrust());
        assertEquals("iis::mutecitation_export", pub0.getDataInfo().getInferenceprovenance());
        assertEquals("iis", pub0.getDataInfo().getProvenanceaction().getClassid());

        // instance type
        assertEquals(1, pub0.getInstance().size());
        assertEquals("0000", pub0.getInstance().get(0).getInstancetype().getClassid());
        assertEquals("UNKNOWN", pub0.getInstance().get(0).getInstancetype().getClassname());
        assertEquals("dnet:publication_resource", pub0.getInstance().get(0).getInstancetype().getSchemeid());
        assertEquals("dnet:publication_resource", pub0.getInstance().get(0).getInstancetype().getSchemename());

        // title
        assertEquals(1, pub0.getTitle().size());
        assertEquals("Introduction to AI", pub0.getTitle().get(0).getValue());
        assertEquals("main title", pub0.getTitle().get(0).getQualifier().getClassid());

        // authors with rank
        assertEquals(2, pub0.getAuthor().size());
        assertEquals("John Smith", pub0.getAuthor().get(0).getFullname());
        assertEquals(Integer.valueOf(1), pub0.getAuthor().get(0).getRank());
        assertEquals("Jane Doe", pub0.getAuthor().get(1).getFullname());
        assertEquals(Integer.valueOf(2), pub0.getAuthor().get(1).getRank());

        // year → YYYY-01-01
        assertEquals(1, pub0.getRelevantdate().size());
        assertEquals("2020-01-01", pub0.getRelevantdate().get(0).getValue());
        assertEquals("created", pub0.getRelevantdate().get(0).getQualifier().getClassid());

        // date of acceptance (year)
        assertNotNull(pub0.getDateofacceptance());
        assertEquals("2020-01-01", pub0.getDateofacceptance().getValue());

        // --- Verify second entity (pub1, ref 2: "Machine Learning") ---
        AtomicAction<Publication> entity1 = findEntityByTitle(capturedEntityActions, "Machine Learning");
        assertNotNull(entity1, "expected entity for 'Machine Learning'");
        Publication pub1 = entity1.getPayload();
        assertEquals(1, pub1.getAuthor().size());
        assertEquals("Bob Wilson", pub1.getAuthor().get(0).getFullname());
        assertEquals(Integer.valueOf(1), pub1.getAuthor().get(0).getRank());
        assertEquals("2019-01-01", pub1.getRelevantdate().get(0).getValue());
        assertEquals("2019-01-01", pub1.getDateofacceptance().getValue());
        assertEquals(1, pub1.getInstance().size());
        assertEquals("0000", pub1.getInstance().get(0).getInstancetype().getClassid());

        // --- Verify third entity (pub4, ref 1: "Deep Learning") with authors and YYYY-MM-DD year ---
        AtomicAction<Publication> entity2 = findEntityByTitle(capturedEntityActions, "Deep Learning");
        assertNotNull(entity2, "expected entity for 'Deep Learning'");
        Publication pub2 = entity2.getPayload();
        assertEquals(3, pub2.getAuthor().size());
        assertEquals("Alice Smith", pub2.getAuthor().get(0).getFullname());
        assertEquals(Integer.valueOf(1), pub2.getAuthor().get(0).getRank());
        assertEquals("Bob Johnson", pub2.getAuthor().get(1).getFullname());
        assertEquals(Integer.valueOf(2), pub2.getAuthor().get(1).getRank());
        assertEquals("Carol Williams", pub2.getAuthor().get(2).getFullname());
        assertEquals(Integer.valueOf(3), pub2.getAuthor().get(2).getRank());
        // YYYY-MM-DD preserved as-is
        assertEquals("2020-06-15", pub2.getRelevantdate().get(0).getValue());
        assertEquals("2020-06-15", pub2.getDateofacceptance().getValue());

        // --- Verify fourth entity (pub5: "Quantum Computing") ---
        AtomicAction<Publication> entity3 = findEntityByTitle(capturedEntityActions, "Quantum Computing");
        assertNotNull(entity3, "expected entity for 'Quantum Computing'");
        Publication pub3 = entity3.getPayload();
        assertEquals(1, pub3.getAuthor().size());
        assertEquals("Eve Adams", pub3.getAuthor().get(0).getFullname());
        assertEquals(Integer.valueOf(1), pub3.getAuthor().get(0).getRank());
        assertEquals("2023-01-01", pub3.getRelevantdate().get(0).getValue());
        assertEquals("2023-01-01", pub3.getDateofacceptance().getValue());

        // --- Verify that "Network Analysis" (invalid year) was exported (eligible) but without relevantdate ---
        AtomicAction<Publication> entityNetworkAnalysis = findEntityByTitle(capturedEntityActions, "Network Analysis");
        assertNotNull(entityNetworkAnalysis, "expected entity for 'Network Analysis'");
        assertNull(entityNetworkAnalysis.getPayload().getRelevantdate(),
                "Network Analysis should have no relevantdate (invalid year)");
        assertNull(entityNetworkAnalysis.getPayload().getDateofacceptance(),
                "Network Analysis should have no dateofacceptance (invalid year)");

        // --- Verify ineligible references were NOT exported ---
        assertNull(findEntityByTitle(capturedEntityActions, "No Authors Ref"),
                "reference without authors should not be exported");
        assertNull(findEntityByTitle(capturedEntityActions, ""),
                "reference with empty title should not be exported");

        // then - verify relations
        List<AtomicAction<Relation>> capturedRelationActions = IteratorUtils
                .toList(SequenceFileTextValueReader.fromFile(outputRelationPath),
                        text -> AtomicActionDeserializationUtils.deserializeAction(text.toString()));
        assertEquals(5, capturedRelationActions.size());

        for (AtomicAction<Relation> action : capturedRelationActions) {
            assertEquals(Relation.class, action.getClazz());
            assertNotNull(action.getPayload());
        }

        // Verify a specific relation: source=pub1, target matches the "Introduction to AI" entity id
        AtomicAction<Publication> introToAiEntity = findEntityByTitle(capturedEntityActions, "Introduction to AI");
        String introToAiId = introToAiEntity.getPayload().getId();
        boolean foundIntroToAiRelation = false;
        boolean foundQuantumRelation = false;
        for (AtomicAction<Relation> relAction : capturedRelationActions) {
            Relation rel = relAction.getPayload();
            assertEquals("resultResult", rel.getRelType());
            assertEquals("relationship", rel.getSubRelType());
            assertEquals("Cites", rel.getRelClass());
            assertTrue(rel.getDataInfo().getInferred());
            assertEquals("0.7", rel.getDataInfo().getTrust());
            assertEquals("iis::mutecitation_export", rel.getDataInfo().getInferenceprovenance());
            assertEquals("iis", rel.getDataInfo().getProvenanceaction().getClassid());

            if ("pub1".equals(rel.getSource()) && introToAiId.equals(rel.getTarget())) {
                foundIntroToAiRelation = true;
            }
            if ("pub5".equals(rel.getSource())) {
                foundQuantumRelation = true;
            }
        }
        assertTrue(foundIntroToAiRelation,
                "expected relation from pub1 to 'Introduction to AI' entity");
        assertTrue(foundQuantumRelation,
                "expected relation from pub5 to 'Quantum Computing' entity");
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private static AtomicAction<Publication> findEntityByTitle(
            List<AtomicAction<Publication>> actions, String title) {
        List<AtomicAction<Publication>> matches = actions.stream()
                .filter(a -> a.getPayload().getTitle() != null
                        && !a.getPayload().getTitle().isEmpty()
                        && title.equals(a.getPayload().getTitle().get(0).getValue()))
                .collect(Collectors.toList());
        return matches.isEmpty() ? null : matches.get(0);
    }

    private SparkJob buildJob() {
        return SparkJobBuilder.create()
                .setAppName("Spark Crossref Exporter")
                .setMainClass(CrossrefExporterJob.class)
                .addArg("-inputPath", inputPath)
                .addArg("-outputEntityPath", outputEntityPath)
                .addArg("-outputRelationPath", outputRelationPath)
                .addArg("-outputReportPath", reportPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
}
