package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_RELATION_COLLECTEDFROM_VALUE;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
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
        return new DocumentToServiceActionBuilderModule(provideTrustLevelThreshold(config),
                WorkflowRuntimeParameters.getParamValue(EXPORT_RELATION_COLLECTEDFROM_VALUE, config));
    }

    // ------------------------ INNER CLASS --------------------------
    
    class DocumentToServiceActionBuilderModule extends AbstractRelationBuilderModule<DocumentToService> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param collectedFromValue collectedFrom value to be set for relation
         */
        public DocumentToServiceActionBuilderModule(Float trustLevelThreshold, String collectedFromValue) {
            super(trustLevelThreshold, buildInferenceProvenance(), collectedFromValue);
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
         * Creates result-service relationship actions.
         */
        private AtomicAction<Relation> createAction(String source, String target, float confidenceLevel,
                String relClass) throws TrustLevelThresholdExceededException {
            return createAtomicActionWithRelation(source, target, OafConstants.REL_TYPE_RESULT_SERVICE,
                    OafConstants.SUBREL_TYPE_RELATIONSHIP, relClass, confidenceLevel);
        }
    }
}
