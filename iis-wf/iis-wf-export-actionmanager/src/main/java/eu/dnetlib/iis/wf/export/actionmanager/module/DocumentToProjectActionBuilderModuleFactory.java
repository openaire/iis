package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;

/**
 * {@link DocumentToProject} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToProjectActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToProject, Relation> {


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
            return Arrays.asList(
                    createAction(object.getDocumentId().toString(), object.getProjectId().toString(),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_ISPRODUCEDBY),
                    createAction(object.getProjectId().toString(), object.getDocumentId().toString(),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_PRODUCES));
        }

        // ------------------------ PRIVATE ----------------------------------
        
        /**
         * Creates similarity related actions.
         */
        private AtomicAction<Relation> createAction(String source, String target, float confidenceLevel, String relClass) throws TrustLevelThresholdExceededException {
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);
            
            Relation relation = new Relation();
            relation.setSource(source);
            relation.setTarget(target);
            relation.setRelType(OafConstants.REL_TYPE_RESULT_PROJECT);
            relation.setSubRelType(OafConstants.SUBREL_TYPE_OUTCOME);
            relation.setRelClass(relClass);
            relation.setDataInfo(buildInference(confidenceLevel, getInferenceProvenance()));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            
            action.setPayload(relation);
            return action;
        }
        
    }
}
