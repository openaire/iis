package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;

/**
 * Document community action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToCommunityActionBuilderModuleFactory extends AbstractDocumentToConceptsActionBuilderModuleFactory
        implements ActionBuilderFactory<DocumentToConceptIds> {

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentToCommunityActionBuilderModuleFactory() {
        super(AlgorithmName.document_community);
    }

}