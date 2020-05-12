package eu.dnetlib.iis.wf.export.actionmanager.module;

/**
 * Document community action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToCommunityActionBuilderModuleFactory extends AbstractDocumentToConceptsActionBuilderModuleFactory {

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentToCommunityActionBuilderModuleFactory() {
        super(AlgorithmName.document_community);
    }

}