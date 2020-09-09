package eu.dnetlib.iis.wf.importer.infospace.truncator.factory;

import eu.dnetlib.iis.wf.importer.infospace.truncator.DocumentMetadataAvroTruncator;

public class TestDocumentMetadataAvroTruncatorFactory implements DocumentMetadataAvroTruncatorFactory {
    @Override
    public DocumentMetadataAvroTruncator create() {
        return DocumentMetadataAvroTruncator.newBuilder()
                .setMaxAbstractLength(1000)
                .setMaxTitleLength(75)
                .setMaxAuthorsSize(25)
                .setMaxAuthorFullnameLength(25)
                .setMaxKeywordsSize(5)
                .setMaxKeywordLength(15)
                .build();
    }
}
