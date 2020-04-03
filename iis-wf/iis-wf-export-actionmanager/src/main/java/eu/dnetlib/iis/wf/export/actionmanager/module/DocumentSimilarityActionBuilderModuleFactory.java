package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_DOCUMENTSSIMILARITY_THRESHOLD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.documentssimilarity.schemas.DocumentSimilarity;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * {@link DocumentSimilarity} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentSimilarityActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentSimilarity, Relation> {

    private static final String REL_TYPE = "resultResult";
    
    private static final String SUBREL_TYPE = "similarity";
    
    private static final String REL_CLASS_IS_AMONG_TOP_N = "isAmongTopNSimilarDocuments";
    
    private static final String REL_CLASS_HAS_AMONG_TOP_N = "hasAmongTopNSimilarDocuments";

    
    private static final Logger log = Logger.getLogger(DocumentSimilarityActionBuilderModuleFactory.class);

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentSimilarityActionBuilderModuleFactory() {
        super(AlgorithmName.document_similarities_standard);
    }

    // ------------------------ LOGIC ---------------------------------
    
    @Override
    public ActionBuilderModule<DocumentSimilarity, Relation> instantiate(Configuration config) {
        Float similarityThreshold = null;
        String thresholdStr = WorkflowRuntimeParameters.getParamValue(EXPORT_DOCUMENTSSIMILARITY_THRESHOLD, config);
        if (thresholdStr != null) {
            similarityThreshold = Float.valueOf(thresholdStr);
            log.info("setting documents similarity exporter threshold to: " + similarityThreshold);
        }
        return new DocumentSimilarityActionBuilderModule(provideTrustLevelThreshold(config), similarityThreshold);
    }
    
    // ------------------------ INNER CLASS --------------------------

    class DocumentSimilarityActionBuilderModule extends AbstractBuilderModule<DocumentSimilarity, Relation> {

        
        private final Float similarityThreshold;

        // ------------------------ CONSTRUCTORS --------------------------

        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param similarityThreshold similarity threshold, skipped when null
         */
        public DocumentSimilarityActionBuilderModule(Float trustLevelThreshold, Float similarityThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
            this.similarityThreshold = similarityThreshold;
        }

        // ------------------------ LOGIC --------------------------

        @Override
        public List<AtomicAction<Relation>> build(DocumentSimilarity object) {
            // checking similarity threshold if set
            if (similarityThreshold != null && object.getSimilarity() != null
                    && object.getSimilarity() < similarityThreshold) {
                return Collections.emptyList();
            } else {
                return Arrays.asList(createAction(object, false), createAction(object, true));    
            }
        }

        // ------------------------ PRIVATE --------------------------

        /**
         * Creates similarity related actions.
         * 
         * @param object source object
         * @param backwardMode flag indicating relation should be created in backward mode
         * @return similarity related actions
         */
        private AtomicAction<Relation> createAction(DocumentSimilarity object, boolean backwardMode) {
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);
            action.setPayload(buildRelation(object.getDocumentId().toString(), object.getOtherDocumentId().toString(),
                    object.getSimilarity(), backwardMode));
            return action;
        }

        /**
         * Builds {@link Relation} object with similarity details.
         */
        private Relation buildRelation(String sourceId, String targetDocId, float score, boolean invert) {
            Relation relation = new Relation();
            relation.setSource(invert?targetDocId:sourceId);
            relation.setTarget(invert?sourceId:targetDocId);
            relation.setRelType(REL_TYPE);
            relation.setSubRelType(SUBREL_TYPE);
            relation.setRelClass(invert ? REL_CLASS_IS_AMONG_TOP_N : REL_CLASS_HAS_AMONG_TOP_N);
            relation.setDataInfo(buildInferenceForTrustLevel(StaticConfigurationProvider.ACTION_TRUST_0_9));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            // FIXME extend Relation model to fin similarity score in
            if (true) throw new RuntimeException("unable to export score");
            // relation.setSimilarity(score);
            return relation;
        }

    }

}
