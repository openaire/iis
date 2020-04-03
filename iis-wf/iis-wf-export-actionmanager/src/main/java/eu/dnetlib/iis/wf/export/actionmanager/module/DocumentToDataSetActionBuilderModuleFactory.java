package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;

/**
 * {@link DocumentToDataSet} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToDataSetActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToDataSet, Relation> {


    private static final String REL_TYPE = "resultResult";
    
    private static final String SUBREL_TYPE = "publicationDataset";
    
    private static final String REL_CLASS = "isRelatedTo";
    
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
            return Arrays.asList(createAction(object, false), createAction(object, true));
        }

        // ------------------------ PRIVATE --------------------------
        
        /**
         * Creates similarity related actions.
         * 
         * @param object source object
         * @param backwardMode flag indicating relation should be created in backward mode
         * @throws TrustLevelThresholdExceededException 
         */
        private AtomicAction<Relation> createAction(DocumentToDataSet object, boolean backwardMode) throws TrustLevelThresholdExceededException {
            AtomicAction<Relation> action = new AtomicAction<>();
            action.setClazz(Relation.class);
            action.setPayload(buildRelation(object, backwardMode));
            return action;
        }
        
        private Relation buildRelation(DocumentToDataSet object, boolean backwardMode) throws TrustLevelThresholdExceededException {
            Relation relation = new Relation();
            relation.setSource(backwardMode ? object.getDatasetId().toString():  object.getDocumentId().toString());
            relation.setTarget(backwardMode ? object.getDocumentId().toString(): object.getDatasetId().toString());
            relation.setRelType(REL_TYPE);
            relation.setSubRelType(SUBREL_TYPE);
            relation.setRelClass(REL_CLASS);
            relation.setDataInfo(buildInference(object.getConfidenceLevel()));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            return relation;
        }
    }
}
