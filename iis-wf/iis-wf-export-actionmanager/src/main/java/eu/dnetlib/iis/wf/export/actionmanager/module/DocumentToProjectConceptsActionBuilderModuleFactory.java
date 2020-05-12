package eu.dnetlib.iis.wf.export.actionmanager.module;

/**
 * {@link DocumentToResearchInitiatives} action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToProjectConceptsActionBuilderModuleFactory extends AbstractDocumentToConceptsActionBuilderModuleFactory {

    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToProjectConceptsActionBuilderModuleFactory() {
        super(AlgorithmName.document_referencedProjects);
    }

}