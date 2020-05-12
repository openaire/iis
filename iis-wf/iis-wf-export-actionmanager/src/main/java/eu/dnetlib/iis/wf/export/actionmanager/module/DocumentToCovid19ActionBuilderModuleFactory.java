package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;

/**
 * {@link DocumentToConceptIds} action builder module coveying covid-19 references.
 * 
 * @author mhorst
 *
 */
public class DocumentToCovid19ActionBuilderModuleFactory extends AbstractDocumentToConceptsActionBuilderModuleFactory {

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentToCovid19ActionBuilderModuleFactory() {
        super(AlgorithmName.document_covid19);
    }

}