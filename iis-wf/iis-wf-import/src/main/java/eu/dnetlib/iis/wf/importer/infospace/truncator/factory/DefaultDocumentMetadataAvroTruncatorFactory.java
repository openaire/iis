package eu.dnetlib.iis.wf.importer.infospace.truncator.factory;

import eu.dnetlib.iis.wf.importer.infospace.truncator.DocumentMetadataAvroTruncator;

/**
 * Factory for {@link DocumentMetadataAvroTruncator} providing a default implementation with production values of truncation parameters.
 */
public class DefaultDocumentMetadataAvroTruncatorFactory implements DocumentMetadataAvroTruncatorFactory {
    @Override
    public DocumentMetadataAvroTruncator create() {
        return new DocumentMetadataAvroTruncator();
    }
}
