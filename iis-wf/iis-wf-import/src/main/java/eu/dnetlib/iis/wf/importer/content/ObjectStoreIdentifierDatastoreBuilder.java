package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV;

import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.wf.importer.AbstractIdentifierDatastoreBuilder;

/**
 * Process module writing ObjectStore identifiers from parameters as {@link Identifier} avro records.
 * 
 * This step is required for further ObjectStore records retrieval parallelization. 
 * 
 * @author mhorst
 *
 */
public class ObjectStoreIdentifierDatastoreBuilder extends AbstractIdentifierDatastoreBuilder {

    public ObjectStoreIdentifierDatastoreBuilder() {
        super(IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV, IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV);
    }

}
