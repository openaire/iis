package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;

/**
 * {@link DocumentToProject} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToProjectActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToProject, Relation> {

    private static final String REL_TYPE = "resultProject";
    
    private static final String SUBREL_TYPE = "outcome";
    
    public static final String REL_CLASS_ISPRODUCEDBY = "isProducedBy";

    public static final String REL_CLASS_PRODUCES = "produces";

    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToProjectActionBuilderModuleFactory() {
        super(AlgorithmName.document_referencedProjects);
    }

    // ------------------------ LOGIC ----------------------------------
    
    @Override
    public ActionBuilderModule<DocumentToProject, Relation> instantiate(Configuration config) {
        return new DocumentToProjectActionBuilderModule(provideTrustLevelThreshold(config));
    }
    
    // ------------------------ INNER CLASS ----------------------------
    
    class DocumentToProjectActionBuilderModule extends AbstractBuilderModule<DocumentToProject, Relation> {


        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public DocumentToProjectActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
        }
        
        // ------------------------ LOGIC ----------------------------------

        @Override
        public List<AtomicAction<Relation>> build(DocumentToProject object) throws TrustLevelThresholdExceededException {
            return Arrays.asList(createAction(object, false), createAction(object, true));
        }

        // ------------------------ PRIVATE ----------------------------------
        
        /**
         * Creates similarity related actions.
         * 
         * @param object source object
         * @param backwardMode flag indicating relation should be created in backward mode
         * @throws TrustLevelThresholdExceededException 
         */
        private AtomicAction<Relation> createAction(DocumentToProject object, boolean backwardMode) throws TrustLevelThresholdExceededException {
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);
            action.setPayload(buildRelation(object, backwardMode));
            return action;
        }
        
        private Relation buildRelation(DocumentToProject object, boolean backwardMode) throws TrustLevelThresholdExceededException {
            Relation relation = new Relation();
            relation.setSource(backwardMode ? object.getProjectId().toString():  object.getDocumentId().toString());
            relation.setTarget(backwardMode ? object.getDocumentId().toString(): object.getProjectId().toString());
            relation.setRelType(REL_TYPE);
            relation.setSubRelType(SUBREL_TYPE);
            relation.setRelClass(backwardMode ? REL_CLASS_PRODUCES : REL_CLASS_ISPRODUCEDBY);
            relation.setDataInfo(buildInference(object.getConfidenceLevel(), getInferenceProvenance()));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            return relation;
        }
        
    }
}
