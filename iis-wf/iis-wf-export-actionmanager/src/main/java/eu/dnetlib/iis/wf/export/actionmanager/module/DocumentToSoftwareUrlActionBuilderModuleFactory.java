package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.ExternalReference;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.export.schemas.DocumentToSoftwareUrls;
import eu.dnetlib.iis.export.schemas.SoftwareUrl;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * Converts {@link DocumentToSoftwareUrls} holding links to software into {@link AtomicAction} objects.
 * 
 * @author mhorst
 *
 */
public class DocumentToSoftwareUrlActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToSoftwareUrls> {

    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToSoftwareUrlActionBuilderModuleFactory() {
        super(AlgorithmName.document_software_url);
    }
    
    // ---------------------- LOGIC ----------------------------

    @Override
    public ActionBuilderModule<DocumentToSoftwareUrls> instantiate(Configuration config, Agent agent, String actionSetId) {
        return new DocumentToSoftwareUrlActionBuilderModule(provideTrustLevelThreshold(config), agent, actionSetId);
    }

    @Override
    public AlgorithmName getAlgorithName() {
        return algorithmName;
    }
    
    // ---------------------- INNER CLASSES ----------------------------

    class DocumentToSoftwareUrlActionBuilderModule extends AbstractBuilderModule<DocumentToSoftwareUrls> {

        // ---------------------- CONSTRUCTORS ----------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public DocumentToSoftwareUrlActionBuilderModule(Float trustLevelThreshold, Agent agent, String actionSetId) {
            super(trustLevelThreshold, buildInferenceProvenance(), agent, actionSetId);
        }
        
        // ---------------------- LOGIC ----------------------------

        @Override
        public List<AtomicAction> build(DocumentToSoftwareUrls object) throws TrustLevelThresholdExceededException {
            Preconditions.checkNotNull(object);
            
            Oaf oaf = buildOafWithSoftwareUrl(object);
            if (oaf != null) {
                return actionFactory.createUpdateActions(actionSetId, agent, object.getDocumentId().toString(),
                        Type.result, oaf.toByteArray());
            } else {
                return Collections.emptyList();
            }
        }

        // ---------------------- PRIVATE ----------------------------
        
        /**
         * Builds {@link Oaf} object containing external reference to url hosting software.
         * 
         * @param source document to software urls
         * @return {@link Oaf} object containing external references pointing to software urls
         * @throws TrustLevelThresholdExceededException
         */
        private Oaf buildOafWithSoftwareUrl(DocumentToSoftwareUrls source) throws TrustLevelThresholdExceededException {
            if (!source.getSoftwareUrls().isEmpty()) {
                Result.Builder resultBuilder = Result.newBuilder();
                for (SoftwareUrl sofwareUrl : source.getSoftwareUrls()) {
                    resultBuilder.addExternalReference(buildExternalReference(sofwareUrl));
                }
                OafEntity.Builder entityBuilder = OafEntity.newBuilder();
                entityBuilder.setId(source.getDocumentId().toString());
                entityBuilder.setType(Type.result);
                entityBuilder.setResult(resultBuilder.build());
                return buildOaf(entityBuilder.build());
    
            }
            return null;
        }
        
        private ExternalReference buildExternalReference(SoftwareUrl sofwareUrl) throws TrustLevelThresholdExceededException {
            ExternalReference.Builder externalRefBuilder = ExternalReference.newBuilder();
            externalRefBuilder.setUrl(sofwareUrl.getSoftwareUrl().toString());
            externalRefBuilder.setSitename(sofwareUrl.getRepositoryName().toString());
            Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
            qualifierBuilder.setClassid("software");
            qualifierBuilder.setClassname("software");
            qualifierBuilder.setSchemeid("dnet:externalReference_typologies");
            qualifierBuilder.setSchemename("dnet:externalReference_typologies");
            externalRefBuilder.setQualifier(qualifierBuilder.build());
            externalRefBuilder.setDataInfo(sofwareUrl.getConfidenceLevel() != null
                    ? buildInference(sofwareUrl.getConfidenceLevel()) : 
                        buildInferenceForTrustLevel(StaticConfigurationProvider.ACTION_TRUST_0_9));
            return externalRefBuilder.build();
        }
    }
}