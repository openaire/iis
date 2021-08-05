package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
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
        return new DocumentToDataSetActionBuilderModule(provideTrustLevelThreshold(config));
    }

    // ------------------------ INNER CLASS --------------------------
    
    class DocumentToDataSetActionBuilderModule extends AbstractBuilderModule<DocumentToDataSet, Relation> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         */
        public DocumentToDataSetActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
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
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);

            Relation relation = new Relation();
            relation.setSource(source);
            relation.setTarget(target);
            relation.setRelType(OafConstants.REL_TYPE_RESULT_RESULT);
            relation.setSubRelType(OafConstants.SUBREL_TYPE_RELATIONSHIP);
            relation.setRelClass(relClass);
            relation.setDataInfo(buildInference(confidenceLevel));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            
            action.setPayload(relation);
            
            return action;
        }
    }
}
