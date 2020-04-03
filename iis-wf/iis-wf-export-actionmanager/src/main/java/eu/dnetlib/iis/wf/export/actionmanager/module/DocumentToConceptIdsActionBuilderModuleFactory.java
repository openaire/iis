package eu.dnetlib.iis.wf.export.actionmanager.module;

/**
 * {@link DocumentToResearchInitiatives} action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToConceptIdsActionBuilderModuleFactory extends AbstractDocumentToConceptsActionBuilderModuleFactory {

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentToConceptIdsActionBuilderModuleFactory() {
        super(AlgorithmName.document_research_initiative);
    }

}