package eu.dnetlib.iis.workflows.export.actionmanager.module;

import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;

/**
 * {@link DocumentToResearchInitiatives} action builder module.
 * @author mhorst
 *
 */
public class DocumentToConceptIdsActionBuilderModuleFactory extends
		AbstractDocumentToConceptsActionBuilderModuleFactory implements
		ActionBuilderFactory<DocumentToConceptIds> {

	public DocumentToConceptIdsActionBuilderModuleFactory() {
		super(AlgorithmName.document_research_initiative);
	}
	
}