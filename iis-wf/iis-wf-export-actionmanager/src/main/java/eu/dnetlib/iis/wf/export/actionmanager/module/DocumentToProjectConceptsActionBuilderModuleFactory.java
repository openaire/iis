package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;

/**
 * {@link DocumentToResearchInitiatives} action builder module.
 * @author mhorst
 *
 */
public class DocumentToProjectConceptsActionBuilderModuleFactory extends
		AbstractDocumentToConceptsActionBuilderModuleFactory implements
		ActionBuilderFactory<DocumentToConceptIds> {

	public DocumentToProjectConceptsActionBuilderModuleFactory() {
		super(AlgorithmName.document_referencedProjects);
	}

}