package eu.dnetlib.iis.wf.importer.dataset;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATACITE_MDSTORE_IDS_CSV;

import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.wf.importer.AbstractIdentifierDatastoreBuilder;

/**
 * Process module writing dataset MDStore identifiers from parameters as {@link Identifier} avro records.
 * 
 * This step is required for further MDStore records retrieval parallelization. 
 * 
 * @author mhorst
 *
 */
public class MDStoreIdentifierDatastoreBuilder extends AbstractIdentifierDatastoreBuilder {

    public MDStoreIdentifierDatastoreBuilder() {
        super(IMPORT_DATACITE_MDSTORE_IDS_CSV, null);
    }

}
