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
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;

/**
 * {@link DocumentToSoftwareUrl} holding link to software action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToSoftwareUrlActionBuilderModuleFactory implements ActionBuilderFactory<DocumentToSoftwareUrl> {

    private final AlgorithmName algorithmName = AlgorithmName.document_software_url;
    
    // ---------------------- LOGIC ----------------------------

    @Override
    public ActionBuilderModule<DocumentToSoftwareUrl> instantiate(String predefinedTrust, Float trustLevelThreshold,
            Configuration config) {
        return new DocumentToSoftwareUrlActionBuilderModule(predefinedTrust, trustLevelThreshold);
    }

    @Override
    public AlgorithmName getAlgorithName() {
        return algorithmName;
    }
    
    // ---------------------- INNER CLASSES ----------------------------

    class DocumentToSoftwareUrlActionBuilderModule extends AbstractBuilderModule
            implements ActionBuilderModule<DocumentToSoftwareUrl> {

        // ---------------------- CONSTRUCTORS ----------------------------
        
        public DocumentToSoftwareUrlActionBuilderModule(String predefinedTrust, Float trustLevelThreshold) {
            super(predefinedTrust, trustLevelThreshold, algorithmName);
        }
        
        // ---------------------- LOGIC ----------------------------

        @Override
        public List<AtomicAction> build(DocumentToSoftwareUrl object, Agent agent, String actionSetId)
                throws TrustLevelThresholdExceededException {
            Preconditions.checkNotNull(object);
            Preconditions.checkNotNull(agent);
            Preconditions.checkNotNull(actionSetId);
            
            Oaf oaf = buildOAFWithPdb(object);
            if (oaf != null) {
                return actionFactory.createUpdateActions(actionSetId, agent, object.getDocumentId().toString(),
                        Type.result, oaf.toByteArray());
            } else {
                return Collections.emptyList();
            }
        }

        // ---------------------- PRIVATE ----------------------------
        
        /**
         * Builds OAF object containing external reference to url hosting software.
         * 
         * @param source
         * @return OAF object containing pdb external references
         * @throws TrustLevelThresholdExceededException
         */
        private Oaf buildOAFWithPdb(DocumentToSoftwareUrl source) throws TrustLevelThresholdExceededException {
            Result.Builder resultBuilder = Result.newBuilder();
            ExternalReference.Builder externalRefBuilder = ExternalReference.newBuilder();
            externalRefBuilder.setUrl(source.getSoftwareUrl().toString());
            Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
            // TODO set proper qualifer
            qualifierBuilder.setClassid("accessionNumber");
            qualifierBuilder.setClassname("accessionNumber");
            qualifierBuilder.setSchemeid("dnet:externalReference_typologies");
            qualifierBuilder.setSchemename("dnet:externalReference_typologies");
            externalRefBuilder.setQualifier(qualifierBuilder.build());
            // TODO set missing values
            // externalRefBuilder.setSitename(value);
            // externalRefBuilder.setRefidentifier(value);
            externalRefBuilder.setDataInfo(source.getConfidenceLevel() != null
                    ? buildInference(source.getConfidenceLevel()) : buildInference());
            resultBuilder.addExternalReference(externalRefBuilder.build());

            OafEntity.Builder entityBuilder = OafEntity.newBuilder();
            entityBuilder.setId(source.getDocumentId().toString());
            entityBuilder.setType(Type.result);
            entityBuilder.setResult(resultBuilder.build());
            return buildOaf(entityBuilder.build());
        }
    }

}
