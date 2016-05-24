package eu.dnetlib.iis.wf.affmatching.model;

import com.google.common.base.Objects;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;

/**
 * Simplified version of {@link AffMatchResult} used for quality testing purposes.
 * 
 * @author madryk
 */
public class SimpleAffMatchResult {

    private String documentId;
    
    private int affiliationPosition;
    
    private String organizationId;

    
    //------------------------ CONSTRUCTORS --------------------------
    
    public SimpleAffMatchResult(String documentId, int affiliationPosition, String organizationId) {
        this.documentId = documentId;
        this.affiliationPosition = affiliationPosition;
        this.organizationId = organizationId;
        
    }

    
    //------------------------ GETTERS --------------------------
    
    public String getDocumentId() {
        return documentId;
    }

    public int getAffiliationPosition() {
        return affiliationPosition;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    
    //------------------------ hashCode & equals --------------------------
    
    @Override
    public int hashCode() {
        return Objects.hashCode(documentId, affiliationPosition, organizationId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleAffMatchResult other = (SimpleAffMatchResult) obj;
        
        return Objects.equal(documentId, other.documentId) &&
                Objects.equal(affiliationPosition, other.affiliationPosition) &&
                Objects.equal(organizationId, other.organizationId);
    }
    
    
    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return "SimpleAffMatchResult [documentId=" + documentId + ", affiliationPosition=" + affiliationPosition
                + ", organizationId=" + organizationId + "]";
    }
    
    
}
