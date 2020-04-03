package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.oaf.ExternalReference;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;

/**
 * {@link DocumentToConceptIds} holding protein data bank identifiers action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToPdbActionBuilderModuleFactory extends AbstractActionBuilderFactory<DocumentToConceptIds, Result> {

    public static final String EXPORT_PDB_URL_ROOT = "export.referenceextraction.pdb.url.root";
    
    public static final String SITENAME = "Protein Data Bank";
    
    public static final String CLASS = "accessionNumber";
    
    public static final String SCHEME = "dnet:externalReference_typologies";
    

    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToPdbActionBuilderModuleFactory() {
        super(AlgorithmName.document_pdb);
    }

    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToConceptIds, Result> instantiate(Configuration config) {
        return new DocumentToPdbActionBuilderModule(provideTrustLevelThreshold(config), config.get(EXPORT_PDB_URL_ROOT));
    }
    
    // ------------------------ INNER CLASS ---------------------------------
    
    class DocumentToPdbActionBuilderModule extends AbstractEntityBuilderModule<DocumentToConceptIds, Result> {

        private final String pdbUrlRoot;

        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param pdbUrlRoot protein databank root url
         */
        public DocumentToPdbActionBuilderModule(Float trustLevelThreshold, String pdbUrlRoot) {
            super(trustLevelThreshold, buildInferenceProvenance());
            this.pdbUrlRoot = pdbUrlRoot;
        }

        // ------------------------ LOGIC ---------------------------------
        
        protected Class<Result> getResultClass() {
            return Result.class;
        }

        /**
         * Builds OAF object containing research initiative concepts.
         * @throws TrustLevelThresholdExceededException 
         */
        @Override
        protected Result convert(DocumentToConceptIds source) throws TrustLevelThresholdExceededException {
            if (CollectionUtils.isNotEmpty(source.getConcepts())) {
                Result result = new Result();
                result.setId(source.getDocumentId().toString());
                result.setLastupdatetimestamp(System.currentTimeMillis());
                
                List<ExternalReference> references = Lists.newArrayList();
                for (Concept concept : source.getConcepts()) {
                    references.add(buildExternalReference(concept));
                }
                result.setExternalReference(references);

                return result;
            } else {
                return null;
            }
        }
        
        /**
         * Builds {@link ExternalReference} instance representing PDB concept.
         */
        private ExternalReference buildExternalReference(Concept concept) throws TrustLevelThresholdExceededException {
            ExternalReference externalRef = new ExternalReference();
            externalRef.setSitename(SITENAME);
            String pdbId = concept.getId().toString();
            if (pdbUrlRoot != null && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(pdbUrlRoot)) {
                externalRef.setUrl(pdbUrlRoot + pdbId);
            } else {
                throw new RuntimeException(EXPORT_PDB_URL_ROOT + " parameter is undefined!");
            }
            Qualifier qualifier = new Qualifier();
            qualifier.setClassid(CLASS);
            qualifier.setClassname(CLASS);
            qualifier.setSchemeid(SCHEME);
            qualifier.setSchemename(SCHEME);
            externalRef.setQualifier(qualifier);
            externalRef.setRefidentifier(pdbId);
            externalRef.setDataInfo(buildInference(concept.getConfidenceLevel()));
            return externalRef;
        }
    }
}
