package eu.dnetlib.iis.citationmatching.converter.entity_id;

import java.security.InvalidParameterException;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class CitEntityId {
    private static final String PREFIX = "cit_";
    private static final String SEPARATOR = "_";

    private final String sourceDocumentId;
    private final int position;
    public CitEntityId(String sourceDocumentId, int position) {
        this.sourceDocumentId = sourceDocumentId;
        this.position = position;
    }

    public String getSourceDocumentId() {
        return sourceDocumentId;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return PREFIX + sourceDocumentId + SEPARATOR + position;
    }

    public static CitEntityId parseFrom(String citId) {
        String citIdWithoutPrefix = dropPrefix(citId);
        int idx = citIdWithoutPrefix.lastIndexOf(SEPARATOR);
        if (idx == - 1) {
            throw new InvalidParameterException(getErrorMessage(citId));
        }

        String documentId = citIdWithoutPrefix.substring(0, idx);
        if (documentId.isEmpty()) {
            throw new InvalidParameterException(getErrorMessage(citId));
        }

        int position;
        try {
            position = Integer.parseInt(citIdWithoutPrefix.substring(idx + 1));
        } catch (Exception e) {
            throw new InvalidParameterException(getErrorMessage(citId));
        }

        return new CitEntityId(documentId, position);
    }

    private static String dropPrefix(String entityId) {
        if (!entityId.startsWith(PREFIX)) {
            throw new InvalidParameterException(getErrorMessage(entityId));
        }
        return entityId.substring(PREFIX.length());
    }

    private static String getErrorMessage(String id) {
        return "invalid citation id: " + id;
    }
}
