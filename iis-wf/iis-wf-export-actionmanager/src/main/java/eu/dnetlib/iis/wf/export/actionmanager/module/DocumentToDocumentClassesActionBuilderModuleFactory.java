package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClass;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClasses;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;

/**
 * {@link DocumentToDocumentClasses} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToDocumentClassesActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToDocumentClasses> {

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentToDocumentClassesActionBuilderModuleFactory() {
        super(AlgorithmName.document_classes);
    }

    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToDocumentClasses> instantiate(Configuration config, Agent agent,
            String actionSetId) {
        return new DocumentToDocumentClassesActionBuilderModule(provideTrustLevelThreshold(config), agent, actionSetId);
    }
    
    // ------------------------ INNER CLASS ---------------------------

    class DocumentToDocumentClassesActionBuilderModule extends AbstractBuilderModule<DocumentToDocumentClasses> {

        // ------------------------ CONSTRUCTORS --------------------------

        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public DocumentToDocumentClassesActionBuilderModule(Float trustLevelThreshold, Agent agent,
                String actionSetId) {
            super(trustLevelThreshold, buildInferenceProvenance(), agent, actionSetId);
        }

        // ------------------------ LOGIC --------------------------
        
        @Override
        public List<AtomicAction> build(DocumentToDocumentClasses object) {
            Oaf oaf = buildOAFClasses(object);
            if (oaf != null) {
                return getActionFactory().createUpdateActions(getActionSetId(), getAgent(), 
                        object.getDocumentId().toString(), Type.result, oaf.toByteArray());
            } else {
                return Collections.emptyList();
            }
        }

        // ------------------------ PRIVATE --------------------------
        
        /**
         * Builds OAF object containing document classes.
         */
        private Oaf buildOAFClasses(DocumentToDocumentClasses source) {
            if (source.getClasses() != null) {
                List<? extends StructuredProperty> classificationSubjects = convertAvroToProtoBuff(
                        source.getClasses());
                if (CollectionUtils.isNotEmpty(classificationSubjects)) {
                    OafEntity.Builder entityBuilder = OafEntity.newBuilder();
                    Result.Builder resultBuilder = Result.newBuilder();
                    Metadata.Builder metaBuilder = Metadata.newBuilder();
                    metaBuilder.addAllSubject(classificationSubjects);
                    resultBuilder.setMetadata(metaBuilder.build());
                    entityBuilder.setResult(resultBuilder.build());
                    entityBuilder.setId(source.getDocumentId().toString());
                    entityBuilder.setType(Type.result);
                    return buildOaf(entityBuilder.build());
                }
            }
            // fallback
            return null;
        }

        private List<? extends StructuredProperty> convertAvroToProtoBuff(DocumentClasses source) {
            List<StructuredProperty> list = new ArrayList<StructuredProperty>();
            if (CollectionUtils.isNotEmpty(source.getArXivClasses())) {
                list.addAll(convertAvroToProtoBuff(source.getArXivClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_ARXIV));
            }
            if (CollectionUtils.isNotEmpty(source.getDDCClasses())) {
                list.addAll(convertAvroToProtoBuff(source.getDDCClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_DDC));
            }
            if (CollectionUtils.isNotEmpty(source.getWoSClasses())) {
                list.addAll(convertAvroToProtoBuff(source.getWoSClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_WOS));
            }
            if (CollectionUtils.isNotEmpty(source.getMeshEuroPMCClasses())) {
                list.addAll(convertAvroToProtoBuff(source.getMeshEuroPMCClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_MESHEUROPMC));
            }
            if (CollectionUtils.isNotEmpty(source.getACMClasses())) {
                list.addAll(convertAvroToProtoBuff(source.getACMClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_ACM));
            }
            return list;
        }

        private List<StructuredProperty> convertAvroToProtoBuff(List<DocumentClass> source, String taxonomyName) {
            List<StructuredProperty> results = new ArrayList<StructuredProperty>();
            for (DocumentClass current : source) {
                try {
                    StructuredProperty result = convertAvroToProtoBuff(current, taxonomyName);
                    if (result!=null) {
                        results.add(result);    
                    }
                } catch (TrustLevelThresholdExceededException e) {
                    // no need to log, we just do not attach result
                }
            }
            return results;
        }

        private StructuredProperty convertAvroToProtoBuff(DocumentClass source, String taxonomyName)
                        throws TrustLevelThresholdExceededException {
            if (source != null && CollectionUtils.isNotEmpty(source.getClassLabels())) {
                StructuredProperty.Builder builder = StructuredProperty.newBuilder();
                Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
                qualifierBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
                qualifierBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
                qualifierBuilder.setClassid(taxonomyName);
                qualifierBuilder.setClassname(taxonomyName);
                builder.setQualifier(qualifierBuilder.build());
                builder.setValue(
                        StringUtils.join(source.getClassLabels(), InfoSpaceConstants.CLASSIFICATION_HIERARCHY_SEPARATOR));
                float confidenceLevel = source.getConfidenceLevel();
                builder.setDataInfo(buildInference(confidenceLevel < 1 ? confidenceLevel : 1));
                return builder.build();
            } else {
                return null;
            }
        }
    }
}
