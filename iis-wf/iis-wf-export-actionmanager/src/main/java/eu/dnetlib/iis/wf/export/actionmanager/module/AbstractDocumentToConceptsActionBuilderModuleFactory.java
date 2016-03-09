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

/**
 * {@link DocumentToResearchInitiatives} action builder module.
 * @author mhorst
 *
 */
public abstract class AbstractDocumentToConceptsActionBuilderModuleFactory 
	implements ActionBuilderFactory<DocumentToConceptIds> {

	protected final AlgorithmName algorithmName;
	
	/**
	 * Default constructor.
	 * @param algorithmName
	 */
	public AbstractDocumentToConceptsActionBuilderModuleFactory(
			AlgorithmName algorithmName) {
		this.algorithmName = algorithmName;
	}
	
	class DocumentToConceptsActionBuilderModule extends
	AbstractBuilderModule implements ActionBuilderModule<DocumentToConceptIds> {

		/**
		 * Default constructor.
		 * @param predefinedTrust
		 */
		public DocumentToConceptsActionBuilderModule(
				String predefinedTrust,  Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(DocumentToConceptIds object,
				Agent agent, String actionSetId) {
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
//					confidence level is omitted because it is always set to 1
					contextBuilder.setDataInfo(buildInference());
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
	public ActionBuilderModule<DocumentToConceptIds> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new DocumentToConceptsActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
