package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * {@link DocumentToResearchInitiatives} action builder module.
 * 
 * @author mhorst
 *
 */
public abstract class AbstractDocumentToConceptsActionBuilderModuleFactory
        extends AbstractActionBuilderFactory<DocumentToConceptIds, Result> {

    // ------------------------ CONSTRUCTORS --------------------------
    
    public AbstractDocumentToConceptsActionBuilderModuleFactory(AlgorithmName algorithmName) {
        super(algorithmName);
    }
    
    // ------------------------ LOGIC ---------------------------------

    @Override
    public ActionBuilderModule<DocumentToConceptIds, Result> instantiate(Configuration config) {
        return new DocumentToConceptsActionBuilderModule(provideTrustLevelThreshold(config));
    }
    
    // ------------------------ INNER CLASS ---------------------------
    
    class DocumentToConceptsActionBuilderModule extends AbstractEntityBuilderModule<DocumentToConceptIds, Result> {

        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         */
        public DocumentToConceptsActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
        }
        
        // ------------------------ LOGIC ----------------------------------

        protected Class<Result> getResultClass() {
            return Result.class;
        }

        /**
         * Builds OAF object containing research initiative concepts.
         */
        @Override
        protected Result convert(DocumentToConceptIds source) {
            if (CollectionUtils.isNotEmpty(source.getConcepts())) {
                Result result = new Result();
                result.setId(source.getDocumentId().toString());
                
                List<Context> contexts = new ArrayList<Context>(source.getConcepts().size());
                for (Concept concept : source.getConcepts()) {
                    Context context = new Context();
                    context.setId(concept.getId().toString());
                    context.setDataInfo(Collections.singletonList(buildInferenceForTrustLevel(StaticConfigurationProvider.ACTION_TRUST_0_9)));
                    contexts.add(context);
                }
                result.setContext(contexts);
                return result;
            } else {
                return null;    
            }
        }
    }
}
