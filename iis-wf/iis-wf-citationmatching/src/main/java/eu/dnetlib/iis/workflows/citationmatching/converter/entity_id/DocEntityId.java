package eu.dnetlib.iis.workflows.citationmatching.converter.entity_id;

import java.security.InvalidParameterException;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class DocEntityId {
    private static final String PREFIX = "doc_";

    private final String documentId;
    public DocEntityId(String documentId) {
        this.documentId = documentId;
    }

    public String getDocumentId() {
        return documentId;
    }

    @Override
    public String toString() {
        return PREFIX + documentId;
    }

    public static DocEntityId parseFrom(String docEntityId) {
        return new DocEntityId(dropPrefix(docEntityId));
    }

    private static String dropPrefix(String docEntityId) {
        if (!docEntityId.startsWith(PREFIX)) {
            throw new InvalidParameterException("invalid document id: " + docEntityId);
        }
        return docEntityId.substring(PREFIX.length());
    }
}
