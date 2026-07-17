package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_RELATION_COLLECTEDFROM_KEY;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.wf.export.actionmanager.IdentifierFactory;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;

/**
 * {@link DocumentToPatent} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToPatentActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToPatent, Relation> {

    
    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToPatentActionBuilderModuleFactory() {
        super(AlgorithmName.document_patent);
    }
    
    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToPatent, Relation> instantiate(Configuration config) {
        return new DocumentToPatentActionBuilderModule(provideTrustLevelThreshold(config), 
                WorkflowRuntimeParameters.getParamValue(EXPORT_RELATION_COLLECTEDFROM_KEY, config));
    }

    // ------------------------ INNER CLASS --------------------------
    
    class DocumentToPatentActionBuilderModule extends AbstractRelationBuilderModule<DocumentToPatent> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param collectedFromKey collectedFrom key to be set for relation
         */
        public DocumentToPatentActionBuilderModule(Float trustLevelThreshold, String collectedFromKey) {
            super(trustLevelThreshold, buildInferenceProvenance(), collectedFromKey);
        }

        // ------------------------ LOGIC --------------------------
        
        @Override
        public List<AtomicAction<Relation>> build(DocumentToPatent object) throws TrustLevelThresholdExceededException {
            return Arrays.asList(
                    createAction(object.getDocumentId().toString(), generatePatentId(object.getLensId().toString()),
                            object.getConfidenceLevel(), OafConstants.REL_CLASS_ISRELATEDTO));
        }

        // ------------------------ PRIVATE --------------------------
        
        /**
         * Creates document-to-patent relation actions.
         */
        private AtomicAction<Relation> createAction(String source, String target, float confidenceLevel,
                String relClass) throws TrustLevelThresholdExceededException {
            return createAtomicActionWithRelation(source, target, OafConstants.REL_TYPE_RESULT_RESULT,
                    OafConstants.SUBREL_TYPE_RELATIONSHIP, relClass, confidenceLevel);
        }

        private String generatePatentId(String lensId) {
            return InfoSpaceConstants.ROW_PREFIX_RESULT + InfoSpaceConstants.LENS_ENTITY_ID_PREFIX + 
                InfoSpaceConstants.ID_NAMESPACE_SEPARATOR +  IdentifierFactory.md5(lensId);
        }
    }
}
