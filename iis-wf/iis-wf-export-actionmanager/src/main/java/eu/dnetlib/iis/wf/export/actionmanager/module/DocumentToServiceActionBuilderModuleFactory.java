package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.service.schemas.DocumentToService;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;

/**
 * {@link DocumentToService} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToServiceActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToService, Relation> {

    
    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToServiceActionBuilderModuleFactory() {
        super(AlgorithmName.document_eoscServices);
    }
    
    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToService, Relation> instantiate(Configuration config) {
        return new DocumentToServiceActionBuilderModule(provideTrustLevelThreshold(config));
    }

    // ------------------------ INNER CLASS --------------------------
    
    class DocumentToServiceActionBuilderModule extends AbstractBuilderModule<DocumentToService, Relation> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         */
        public DocumentToServiceActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
        }

        // ------------------------ LOGIC --------------------------
        
        @Override
        public List<AtomicAction<Relation>> build(DocumentToService object) throws TrustLevelThresholdExceededException {
            return Arrays.asList(
                    createAction(object.getDocumentId().toString(), object.getServiceId().toString(),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_ISRELATEDTO));
        }

        // ------------------------ PRIVATE --------------------------
        
        /**
         * Creates result-datasource relationship actions.
         */
        private AtomicAction<Relation> createAction(String source, String target, float confidenceLevel,
                String relClass) throws TrustLevelThresholdExceededException {
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);

            Relation relation = new Relation();
            relation.setSource(source);
            relation.setTarget(target);
            relation.setRelType(OafConstants.REL_TYPE_RESULT_SERVICE);
            relation.setSubRelType(OafConstants.SUBREL_TYPE_RELATIONSHIP);
            relation.setRelClass(relClass);
            relation.setDataInfo(buildInference(confidenceLevel));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            
            action.setPayload(relation);
            
            return action;
        }
    }
}
