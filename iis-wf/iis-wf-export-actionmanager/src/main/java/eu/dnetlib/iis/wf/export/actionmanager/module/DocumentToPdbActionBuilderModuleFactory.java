package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.ExternalReference;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * {@link DocumentToConceptIds} holding protein data bank identifiers action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToPdbActionBuilderModuleFactory extends AbstractBuilderFactory<DocumentToConceptIds> {

    public static final String EXPORT_PDB_URL_ROOT = "export.referenceextraction.pdb.url.root";

    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToPdbActionBuilderModuleFactory() {
        super(AlgorithmName.document_pdb);
    }

    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToConceptIds> instantiate(Configuration config, Agent agent,
            String actionSetId) {
        return new DocumentToPdbActionBuilderModule(provideTrustLevelThreshold(config), config.get(EXPORT_PDB_URL_ROOT),
                agent, actionSetId);
    }
    
    // ------------------------ INNER CLASS ---------------------------------
    
    class DocumentToPdbActionBuilderModule extends AbstractBuilderModule<DocumentToConceptIds> {

        private final String pdbUrlRoot;

        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param pdbUrlRoot protein databank root url
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public DocumentToPdbActionBuilderModule(Float trustLevelThreshold, String pdbUrlRoot, Agent agent,
                String actionSetId) {
            super(trustLevelThreshold, buildInferenceProvenance(), agent, actionSetId);
            this.pdbUrlRoot = pdbUrlRoot;
        }

        // ------------------------ LOGIC ---------------------------------
        
        @Override
        public List<AtomicAction> build(DocumentToConceptIds object) throws TrustLevelThresholdExceededException {
            Oaf oaf = buildOAFWithPdb(object);
            if (oaf != null) {
                return actionFactory.createUpdateActions(actionSetId, agent, object.getDocumentId().toString(),
                        Type.result, oaf.toByteArray());
            } else {
                return Collections.emptyList();
            }
        }

        /**
         * Builds {@link Oaf} object containing pdb external references.
         * @throws TrustLevelThresholdExceededException
         */
        private Oaf buildOAFWithPdb(DocumentToConceptIds source) throws TrustLevelThresholdExceededException {
            if (CollectionUtils.isNotEmpty(source.getConcepts())) {
                Result.Builder resultBuilder = Result.newBuilder();
                for (Concept concept : source.getConcepts()) {
                    resultBuilder.addExternalReference(buildExternalReference(concept));
                }
                OafEntity.Builder entityBuilder = OafEntity.newBuilder();
                if (source.getDocumentId() != null) {
                    entityBuilder.setId(source.getDocumentId().toString());
                }
                entityBuilder.setType(Type.result);
                entityBuilder.setResult(resultBuilder.build());
                return buildOaf(entityBuilder.build());
            }
            // fallback
            return null;
        }
        
        /**
         * Builds {@link ExternalReference} instance representing PDB concept.
         */
        private ExternalReference buildExternalReference(Concept concept) throws TrustLevelThresholdExceededException {
            ExternalReference.Builder externalRefBuilder = ExternalReference.newBuilder();
            externalRefBuilder.setSitename("Protein Data Bank");
            String pdbId = concept.getId().toString();
            if (pdbUrlRoot != null && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(pdbUrlRoot)) {
                externalRefBuilder.setUrl(pdbUrlRoot + pdbId);
            } else {
                throw new RuntimeException(EXPORT_PDB_URL_ROOT + " parameter is undefined!");
            }
            Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
            qualifierBuilder.setClassid("accessionNumber");
            qualifierBuilder.setClassname("accessionNumber");
            qualifierBuilder.setSchemeid("dnet:externalReference_typologies");
            qualifierBuilder.setSchemename("dnet:externalReference_typologies");
            externalRefBuilder.setQualifier(qualifierBuilder.build());
            externalRefBuilder.setRefidentifier(pdbId);
            externalRefBuilder.setDataInfo(
                    concept.getConfidenceLevel() != null ? buildInference(concept.getConfidenceLevel())
                            : buildInferenceForTrustLevel(StaticConfigurationProvider.ACTION_TRUST_0_9));
            return externalRefBuilder.build();
        }
    }
}
