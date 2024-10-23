package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class CitationRelationExporterUtils {

    private static final String INFERENCE_PROVENANCE = InfoSpaceConstants.SEMANTIC_CLASS_IIS +
            InfoSpaceConstants.INFERENCE_PROVENANCE_SEPARATOR + AlgorithmName.document_referencedDocuments;
    private static final String REPORT_ENTRY_KEY_REFERENCES = "processing.citationMatching.relation.references";
    private static final String REPORT_ENTRY_KEY_CITES_DOCS = "processing.citationMatching.relation.cites.docs";
    private static final String REPORT_ENTRY_KEY_ISCITEDBY_DOCS = "processing.citationMatching.relation.iscitedby.docs";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private CitationRelationExporterUtils() {
    }

    public static Dataset<Relation> processCitations(Dataset<Row> citations, UserDefinedFunction isValidConfidenceLevel,
            final String collectedFromKey) {
        return documentIdAndCitationEntry(citations)
                .select(
                        col("documentId"),
                        col("citationEntry.destinationDocumentId"),
                        col("citationEntry.confidenceLevel")
                )
                .na()
                .drop()
                .where(isValidConfidenceLevel.apply(col("citationEntry.confidenceLevel")))
                .groupBy(col("documentId"), col("destinationDocumentId"))
                .agg(max(col("confidenceLevel")).as("confidenceLevel"))
                .as(Encoders.bean(DocumentRelation.class))
                .flatMap(toRelationFlatMapFn(collectedFromKey), Encoders.kryo(Relation.class));
    }

    private static Dataset<Row> documentIdAndCitationEntry(Dataset<Row> citations) {
        return citations
                .select(col("documentId"),
                        explode(col("citations")).as("citationEntry"));
    }

    private static FlatMapFunction<DocumentRelation, Relation> toRelationFlatMapFn(final String collectedFromKey) {
        return (FlatMapFunction<DocumentRelation, Relation>) documentRelation -> {
            Relation forwardRelation = buildRelation(documentRelation.documentId,
                    documentRelation.destinationDocumentId,
                    OafConstants.REL_CLASS_CITES,
                    documentRelation.confidenceLevel,
                    collectedFromKey);
            Relation backwardRelation = buildRelation(documentRelation.destinationDocumentId,
                    documentRelation.documentId,
                    OafConstants.REL_CLASS_ISCITEDBY,
                    documentRelation.confidenceLevel,
                    collectedFromKey);
            return Stream.of(forwardRelation, backwardRelation).iterator();
        };
    }

    private static Relation buildRelation(String source, String target, String relClass, Float confidenceLevel, String collectedFromKey) {
        return BuilderModuleHelper.createRelation(source, target, OafConstants.REL_TYPE_RESULT_RESULT,
                OafConstants.SUBREL_TYPE_CITATION, relClass,
                BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, INFERENCE_PROVENANCE),
                collectedFromKey);
    }

    public static class DocumentRelation {
        private String documentId;
        private String destinationDocumentId;
        private Float confidenceLevel;

        public DocumentRelation() {

        }

        public String getDocumentId() {
            return documentId;
        }

        public void setDocumentId(String documentId) {
            this.documentId = documentId;
        }

        public String getDestinationDocumentId() {
            return destinationDocumentId;
        }

        public void setDestinationDocumentId(String destinationDocumentId) {
            this.destinationDocumentId = destinationDocumentId;
        }

        public Float getConfidenceLevel() {
            return confidenceLevel;
        }

        public void setConfidenceLevel(Float confidenceLevel) {
            this.confidenceLevel = confidenceLevel;
        }
    }

    public static Dataset<Text> relationsToSerializedActions(Dataset<Relation> relations) {
        return relations
                .map(toSerializedActionMapFn(), Encoders.kryo(Text.class));
    }

    private static MapFunction<Relation, Text> toSerializedActionMapFn() {
        return (MapFunction<Relation, Text>) payload ->
                new Text(AtomicActionSerializationUtils.serializeAction(buildAtomicAction(payload), OBJECT_MAPPER));
    }

    private static AtomicAction<Relation> buildAtomicAction(Relation payload) {
        return new AtomicAction<>(Relation.class, payload);
    }

    public static Dataset<ReportEntry> relationsToReportEntries(SparkSession spark, Dataset<Relation> relations) {
        long totalRelationCount = totalRelationCount(relations);
        long uniqueCitesRelationCount = uniqueCitesRelationCount(relations);
        long uniqueIsCitedByRelationCount = uniqueIsCitedByRelationCount(relations);

        return spark.createDataset(Arrays.asList(
                ReportEntryFactory.createCounterReportEntry(REPORT_ENTRY_KEY_REFERENCES, totalRelationCount),
                ReportEntryFactory.createCounterReportEntry(REPORT_ENTRY_KEY_CITES_DOCS, uniqueCitesRelationCount),
                ReportEntryFactory.createCounterReportEntry(REPORT_ENTRY_KEY_ISCITEDBY_DOCS, uniqueIsCitedByRelationCount)
        ), Encoders.kryo(ReportEntry.class));
    }

    private static long totalRelationCount(Dataset<Relation> relations) {
        return relations
                .filter((FilterFunction<Relation>) relation -> relation.getRelClass().equals(OafConstants.REL_CLASS_CITES))
                .count();
    }

    private static long uniqueCitesRelationCount(Dataset<Relation> relations) {
        return uniqueRelationCountByRelClass(relations, OafConstants.REL_CLASS_CITES);
    }

    private static long uniqueIsCitedByRelationCount(Dataset<Relation> relations) {
        return uniqueRelationCountByRelClass(relations, OafConstants.REL_CLASS_ISCITEDBY);
    }

    private static long uniqueRelationCountByRelClass(Dataset<Relation> relations, String relClass) {
        return relations
                .filter((FilterFunction<Relation>) relation -> relation.getRelClass().equals(relClass))
                .map((MapFunction<Relation, String>) Relation::getSource, Encoders.STRING())
                .distinct()
                .count();
    }
}
