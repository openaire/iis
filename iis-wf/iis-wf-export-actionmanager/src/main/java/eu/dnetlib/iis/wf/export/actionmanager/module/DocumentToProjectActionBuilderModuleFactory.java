package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_RELATION_COLLECTEDFROM_KEY;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
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
        return new DocumentToProjectActionBuilderModule(provideTrustLevelThreshold(config),
                WorkflowRuntimeParameters.getParamValue(EXPORT_RELATION_COLLECTEDFROM_KEY, config));
    }
    
    // ------------------------ INNER CLASS ----------------------------
    
    class DocumentToProjectActionBuilderModule extends AbstractRelationBuilderModule<DocumentToProject> {


        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param collectedFromKey collectedFrom key to be set for relation
         */
        public DocumentToProjectActionBuilderModule(Float trustLevelThreshold, String collectedFromKey) {
            super(trustLevelThreshold, buildInferenceProvenance(), collectedFromKey);
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
            return createAtomicActionWithRelation(source, target, OafConstants.REL_TYPE_RESULT_PROJECT,
                    OafConstants.SUBREL_TYPE_OUTCOME, relClass, confidenceLevel);
        }
        
    }
}
