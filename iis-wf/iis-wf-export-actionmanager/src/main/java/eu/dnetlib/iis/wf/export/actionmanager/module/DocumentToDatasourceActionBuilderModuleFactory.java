package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.datasource.schemas.DocumentToDatasource;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;

/**
 * {@link DocumentToDatasource} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToDatasourceActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToDatasource, Relation> {

    
    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToDatasourceActionBuilderModuleFactory() {
        super(AlgorithmName.document_referencedDatasources);
    }
    
    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToDatasource, Relation> instantiate(Configuration config) {
        return new DocumentToDatasourceActionBuilderModule(provideTrustLevelThreshold(config));
    }

    // ------------------------ INNER CLASS --------------------------
    
    class DocumentToDatasourceActionBuilderModule extends AbstractBuilderModule<DocumentToDatasource, Relation> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         */
        public DocumentToDatasourceActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
        }

        // ------------------------ LOGIC --------------------------
        
        @Override
        public List<AtomicAction<Relation>> build(DocumentToDatasource object) throws TrustLevelThresholdExceededException {
            return Arrays.asList(
                    createAction(object.getDocumentId().toString(), object.getDatasourceId().toString(),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_REFERENCES),
                    createAction(object.getDatasourceId().toString(), object.getDocumentId().toString(),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_IS_REFERENCED_BY));
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
            relation.setRelType(OafConstants.REL_TYPE_RESULT_DATASOURCE);
            relation.setSubRelType(OafConstants.SUBREL_TYPE_RELATIONSHIP);
            relation.setRelClass(relClass);
            relation.setDataInfo(buildInference(confidenceLevel));
            relation.setLastupdatetimestamp(System.currentTimeMillis());
            
            action.setPayload(relation);
            
            return action;
        }
    }
}
