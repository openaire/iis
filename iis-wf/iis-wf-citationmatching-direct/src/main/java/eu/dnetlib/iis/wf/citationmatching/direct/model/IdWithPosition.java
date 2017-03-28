package eu.dnetlib.iis.wf.citationmatching.direct.model;

/**
 * Helper class storing document id with reference position.
 * 
 * @author madryk
 *
 */
public class IdWithPosition {
    
    private final String documentId;
    
    private final int referencePosition;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public IdWithPosition(String documentId, int referencePosition) {
        this.documentId = documentId;
        this.referencePosition = referencePosition;
    }
    
    
    //------------------------ GETTERS --------------------------
    
    public String getDocumentId() {
        return documentId;
    }
    
    public int getReferencePosition() {
        return referencePosition;
    }
    
    
    //------------------------ equals & hashCode --------------------------
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IdWithPosition other = (IdWithPosition) obj;
        if (referencePosition != other.referencePosition)
            return false;
        if (documentId == null) {
            if (other.documentId != null)
                return false;
        } else if (!documentId.equals(other.documentId))
            return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + referencePosition;
        result = prime * result + ((documentId == null) ? 0 : documentId.hashCode());
        return result;
    }
    
    
    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return "IdWithPosition [documentId=" + documentId + ", referencePosition=" + referencePosition + "]";
    }
    
}
