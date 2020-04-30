package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
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
        return new MatchedOrganizationActionBuilderModule(provideTrustLevelThreshold(config));
    }
    
    // ------------------------ INNER CLASS ---------------------------------
    
    /**
     * {@link MatchedOrganization} action builder module.
     *
     */
    class MatchedOrganizationActionBuilderModule extends AbstractBuilderModule<MatchedOrganizationWithProvenance, Relation> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         */
        public MatchedOrganizationActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
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
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);

            Relation relation = new Relation();
            relation.setSource(source);
            relation.setTarget(target);
            relation.setRelType(OafConstants.REL_TYPE_RESULT_ORGANIZATION);
            relation.setSubRelType(OafConstants.SUBREL_TYPE_AFFILIATION);
            relation.setRelClass(relClass);
            relation.setDataInfo(buildInference(confidenceLevel));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            
            action.setPayload(relation);
            return action;
        }
        
    }
}
