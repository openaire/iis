package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Context;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * {@link DocumentToResearchInitiatives} action builder module.
 * @author mhorst
 *
 */
public abstract class AbstractDocumentToConceptsActionBuilderModuleFactory 
	extends AbstractBuilderFactory<DocumentToConceptIds> {

	/**
	 * Default constructor.
	 * @param algorithmName
	 */
	public AbstractDocumentToConceptsActionBuilderModuleFactory(
			AlgorithmName algorithmName) {
		super(algorithmName);
	}
	
	class DocumentToConceptsActionBuilderModule extends AbstractBuilderModule<DocumentToConceptIds> {

        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
		public DocumentToConceptsActionBuilderModule(Float trustLevelThreshold,
		        Agent agent, String actionSetId) {
			super(trustLevelThreshold, buildInferenceProvenance(), agent ,actionSetId);
		}
	
		@Override
		public List<AtomicAction> build(DocumentToConceptIds object) {
			Oaf oaf = buildOAFResearchInitiativeConcepts(object);
			if (oaf!=null) {
				return actionFactory.createUpdateActions(
						actionSetId,
						agent, object.getDocumentId().toString(), Type.result, 
						oaf.toByteArray());	
			} else {
				return Collections.emptyList();
			}
		}
		
		/**
		 * Builds OAF object containing research initiative concepts.
		 * @param source
		 * @return OAF object containing research initiative concepts
		 */
		protected Oaf buildOAFResearchInitiativeConcepts(DocumentToConceptIds source) {
			if (source.getConcepts()!=null && 
					source.getConcepts().size()>0) {
				Result.Metadata.Builder metaBuilder = Metadata.newBuilder();
				List<Context> contexts = new ArrayList<Context>(
						source.getConcepts().size());
				for (Concept concept : source.getConcepts()) {
					Context.Builder contextBuilder = Context.newBuilder();
					contextBuilder.setId(concept.getId().toString());
					contextBuilder.setDataInfo(
					        buildInferenceForTrustLevel(StaticConfigurationProvider.ACTION_TRUST_0_9));
					contexts.add(contextBuilder.build());
				}
				metaBuilder.addAllContext(contexts);
				Result.Builder resultBuilder = Result.newBuilder();
				resultBuilder.setMetadata(metaBuilder.build());
	
				OafEntity.Builder entityBuilder = OafEntity.newBuilder();
				if (source.getDocumentId()!=null) {
					entityBuilder.setId(source.getDocumentId().toString());	
				}
				entityBuilder.setType(Type.result);
				entityBuilder.setResult(resultBuilder.build());	
				return buildOaf(entityBuilder.build());
			}
//			fallback
			return null;
		}

	}

	@Override
	public ActionBuilderModule<DocumentToConceptIds> instantiate(Configuration config, Agent agent, String actionSetId) {
		return new DocumentToConceptsActionBuilderModule(
		        provideTrustLevelThreshold(config), agent, actionSetId);
	}

}
