package eu.dnetlib.iis.wf.importer.infospace.truncator.factory;

import eu.dnetlib.iis.wf.importer.infospace.truncator.DocumentMetadataAvroTruncator;

/**
 * {@link DocumentMetadataAvroTruncator} creation abstraction.
 */
public interface DocumentMetadataAvroTruncatorFactory {
    DocumentMetadataAvroTruncator create();
}
