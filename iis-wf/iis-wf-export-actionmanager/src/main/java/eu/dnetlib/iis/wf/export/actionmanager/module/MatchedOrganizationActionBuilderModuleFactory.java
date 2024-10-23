package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_RELATION_COLLECTEDFROM_KEY;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganizationWithProvenance;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;

/**
 * {@link MatchedOrganizationWithProvenance} action builder factory module.
 * 
 * @author mhorst
 *
 */
public class MatchedOrganizationActionBuilderModuleFactory extends AbstractActionBuilderFactory<MatchedOrganizationWithProvenance, Relation> {


    // ------------------------ CONSTRUCTORS --------------------------
    
    public MatchedOrganizationActionBuilderModuleFactory() {
        super(AlgorithmName.document_affiliations);
    }

    // ------------------------ LOGIC ---------------------------------
    
    @Override
    public ActionBuilderModule<MatchedOrganizationWithProvenance, Relation> instantiate(Configuration config) {
        return new MatchedOrganizationActionBuilderModule(provideTrustLevelThreshold(config),
                WorkflowRuntimeParameters.getParamValue(EXPORT_RELATION_COLLECTEDFROM_KEY, config));
    }
    
    // ------------------------ INNER CLASS ---------------------------------
    
    /**
     * {@link MatchedOrganization} action builder module.
     *
     */
    class MatchedOrganizationActionBuilderModule extends AbstractRelationBuilderModule<MatchedOrganizationWithProvenance> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param collectedFromKey collectedFrom key to be set for relation
         */
        public MatchedOrganizationActionBuilderModule(Float trustLevelThreshold, String collectedFromKey) {
            super(trustLevelThreshold, buildInferenceProvenance(), collectedFromKey);
        }

        // ------------------------ LOGIC ---------------------------------
        
        @Override
        public List<AtomicAction<Relation>> build(MatchedOrganizationWithProvenance object) throws TrustLevelThresholdExceededException {
            return Arrays.asList(
                    createAction(object.getDocumentId().toString(), object.getOrganizationId().toString(),
                            object.getMatchStrength(), OafConstants.REL_CLASS_HAS_AUTHOR_INSTITUTION_OF),
                    createAction(object.getOrganizationId().toString(), object.getDocumentId().toString(),
                            object.getMatchStrength(), OafConstants.REL_CLASS_IS_AUTHOR_INSTITUTION_OF));
        }
        
        /**
         * Creates similarity related actions.
         */
        private AtomicAction<Relation> createAction(String source, String target, float confidenceLevel, String relClass) throws TrustLevelThresholdExceededException {
            return createAtomicActionWithRelation(source, target, OafConstants.REL_TYPE_RESULT_ORGANIZATION,
                    OafConstants.SUBREL_TYPE_AFFILIATION, relClass, confidenceLevel);
        }
        
    }
}
