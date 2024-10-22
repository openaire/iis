package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_RELATION_COLLECTEDFROM_VALUE;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;

/**
 * {@link DocumentToDataSet} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToDataSetActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToDataSet, Relation> {

    
    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToDataSetActionBuilderModuleFactory() {
        super(AlgorithmName.document_referencedDatasets);
    }
    
    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToDataSet, Relation> instantiate(Configuration config) {
        return new DocumentToDataSetActionBuilderModule(provideTrustLevelThreshold(config), 
                WorkflowRuntimeParameters.getParamValue(EXPORT_RELATION_COLLECTEDFROM_VALUE, config));
    }

    // ------------------------ INNER CLASS --------------------------
    
    class DocumentToDataSetActionBuilderModule extends AbstractRelationBuilderModule<DocumentToDataSet> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param collectedFromValue collectedFrom value to be set for relation
         */
        public DocumentToDataSetActionBuilderModule(Float trustLevelThreshold, String collectedFromValue) {
            super(trustLevelThreshold, buildInferenceProvenance(), collectedFromValue);
        }

        // ------------------------ LOGIC --------------------------
        
        @Override
        public List<AtomicAction<Relation>> build(DocumentToDataSet object) throws TrustLevelThresholdExceededException {
            return Arrays.asList(
                    createAction(object.getDocumentId().toString(), object.getDatasetId().toString(),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_REFERENCES),
                    createAction(object.getDatasetId().toString(), object.getDocumentId().toString(),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_IS_REFERENCED_BY));
        }

        // ------------------------ PRIVATE --------------------------
        
        /**
         * Creates similarity related actions.
         */
        private AtomicAction<Relation> createAction(String source, String target, float confidenceLevel,
                String relClass) throws TrustLevelThresholdExceededException {
            return createAtomicActionWithRelation(source, target, OafConstants.REL_TYPE_RESULT_RESULT,
                    OafConstants.SUBREL_TYPE_RELATIONSHIP, relClass, confidenceLevel);
        }
    }
}
