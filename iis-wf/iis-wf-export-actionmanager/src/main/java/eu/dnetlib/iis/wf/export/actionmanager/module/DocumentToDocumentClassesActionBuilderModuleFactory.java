package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Subject;
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
public class DocumentToDocumentClassesActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToDocumentClasses, Result> {

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentToDocumentClassesActionBuilderModuleFactory() {
        super(AlgorithmName.document_classes);
    }

    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToDocumentClasses, Result> instantiate(Configuration config) {
        return new DocumentToDocumentClassesActionBuilderModule(provideTrustLevelThreshold(config));
    }
    
    // ------------------------ INNER CLASS ---------------------------

    class DocumentToDocumentClassesActionBuilderModule extends AbstractEntityBuilderModule<DocumentToDocumentClasses, Result> {

        // ------------------------ CONSTRUCTORS --------------------------

        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         */
        public DocumentToDocumentClassesActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
        }

        // ------------------------ LOGIC --------------------------
        
        protected Class<Result> getResultClass() {
            return Result.class;
        }

        /**
         * Builds OAF object containing document classes.
         */
        @Override
        protected Result convert(DocumentToDocumentClasses source) {
            if (source.getClasses() != null) {
                List<Subject> classificationSubjects = convertClassesToOaf(
                        source.getClasses());
                if (CollectionUtils.isNotEmpty(classificationSubjects)) {
                    Result result = new Result();
                    result.setId(source.getDocumentId().toString());
                    result.setLastupdatetimestamp(System.currentTimeMillis());
                    result.setSubject(classificationSubjects);
                    return result;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        // ------------------------ PRIVATE --------------------------
        
        private List<Subject> convertClassesToOaf(DocumentClasses source) {
            List<Subject> list = new ArrayList<Subject>();
            if (CollectionUtils.isNotEmpty(source.getArXivClasses())) {
                list.addAll(convertClassesToOaf(source.getArXivClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_ARXIV));
            }
            if (CollectionUtils.isNotEmpty(source.getDDCClasses())) {
                list.addAll(convertClassesToOaf(source.getDDCClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_DDC));
            }
            if (CollectionUtils.isNotEmpty(source.getWoSClasses())) {
                list.addAll(convertClassesToOaf(source.getWoSClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_WOS));
            }
            if (CollectionUtils.isNotEmpty(source.getMeshEuroPMCClasses())) {
                list.addAll(convertClassesToOaf(source.getMeshEuroPMCClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_MESHEUROPMC));
            }
            if (CollectionUtils.isNotEmpty(source.getACMClasses())) {
                list.addAll(convertClassesToOaf(source.getACMClasses(),
                        InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_ACM));
            }
            return list;
        }

        private List<Subject> convertClassesToOaf(List<DocumentClass> source, String taxonomyName) {
            List<Subject> results = new ArrayList<Subject>();
            for (DocumentClass current : source) {
                try {
                	Subject result = convertClassToOaf(current, taxonomyName);
                    if (result!=null) {
                        results.add(result);    
                    }
                } catch (TrustLevelThresholdExceededException e) {
                    // no need to log, we just do not attach result
                }
            }
            return results;
        }

        private Subject convertClassToOaf(DocumentClass source, String taxonomyName)
                        throws TrustLevelThresholdExceededException {
            if (source != null && CollectionUtils.isNotEmpty(source.getClassLabels())) {
            	Subject structuredProperty = new Subject();
                Qualifier qualifier = new Qualifier();
                qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
                qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
                qualifier.setClassid(taxonomyName);
                qualifier.setClassname(taxonomyName);
                structuredProperty.setQualifier(qualifier);
                structuredProperty.setValue(
                        StringUtils.join(source.getClassLabels(), InfoSpaceConstants.CLASSIFICATION_HIERARCHY_SEPARATOR));
                float confidenceLevel = source.getConfidenceLevel();
                structuredProperty.setDataInfo(buildInference(confidenceLevel < 1 ? confidenceLevel : 1));
                return structuredProperty;
            } else {
                return null;
            }
        }
    }
}
