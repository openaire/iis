package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganizationWithProvenance;

/**
 * {@link MatchedOrganizationWithProvenance} action builder factory module.
 * 
 * @author mhorst
 *
 */
public class MatchedOrganizationActionBuilderModuleFactory extends AbstractActionBuilderFactory<MatchedOrganizationWithProvenance, Relation> {

    private static final String REL_TYPE = "resultOrganization";
    
    private static final String SUBREL_TYPE = "affiliation";
    
    private static final String REL_CLASS_HAS_AUTHOR_INSTITUTION_OF = "hasAuthorInstitution";
    
    private static final String REL_CLASS_IS_AUTHOR_INSTITUTION_OF = "isAuthorInstitutionOf";


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
            return Arrays.asList(createAction(object, false), createAction(object, true));
        }
        
        /**
         * Creates similarity related actions.
         * 
         * @param object source object
         * @param backwardMode flag indicating relation should be created in backward mode
         * @throws TrustLevelThresholdExceededException 
         */
        private AtomicAction<Relation> createAction(MatchedOrganizationWithProvenance object, boolean backwardMode) throws TrustLevelThresholdExceededException {
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);
            action.setPayload(buildRelation(object, backwardMode));
            return action;
        }
        
        private Relation buildRelation(MatchedOrganizationWithProvenance object, boolean backwardMode) throws TrustLevelThresholdExceededException {
            Relation relation = new Relation();
            relation.setSource(backwardMode ? object.getOrganizationId().toString():  object.getDocumentId().toString());
            relation.setTarget(backwardMode ? object.getDocumentId().toString(): object.getOrganizationId().toString());
            relation.setRelType(REL_TYPE);
            relation.setSubRelType(SUBREL_TYPE);
            relation.setRelClass(backwardMode ? REL_CLASS_IS_AUTHOR_INSTITUTION_OF : REL_CLASS_HAS_AUTHOR_INSTITUTION_OF);
            relation.setDataInfo(buildInference(object.getMatchStrength()));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            return relation;
        }
        
    }
}
