package eu.dnetlib.iis.wf.affmatching.model;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Affiliation data that the affiliation matching module operates on. 
 * 
 * @author Łukasz Dumiszewski
 *
 */
public class AffMatchAffiliation {
  
    
    private final String documentId;
    
    private final int position;
    
    private String organizationName;
    
    private String countryName;
    
    private String countryCode;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public AffMatchAffiliation(String documentId, int position) {
        Preconditions.checkArgument(StringUtils.isNotBlank(documentId));
        Preconditions.checkArgument(position > 0);
        this.documentId = documentId;
        this.position = position;
    }
    
    
    //------------------------ GETTERS --------------------------
    
    /**
     * Id of the document the affiliation comes from
     */
    public String getDocumentId() {
        return documentId;
    }
    
    /**
     * Position of this affiliation in the document 
     */
    public int getPosition() {
        return position;
    }
    
    /**
     * Name of the organization 
     */
    public String getOrganizationName() {
        return organizationName;
    }
    
    /**
     * Name of the organization's ({@link #getOrganizationName()}) country
     */
    public String getCountryName() {
        return countryName;
    }

    /**
     * ISO code of the organization's ({@link #getOrganizationName()}) country
     */
    public String getCountryCode() {
        return countryCode;
    }
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns {@link #getDocumentId()} + "###" + {@link #getPosition()} 
     */
    public String getId() {
        return getDocumentId() + "###" + getPosition();
    }

    
 
    //------------------------ SETTERS --------------------------
    
    public void setOrganizationName(String organizationName) {
        this.organizationName = organizationName;
    }
    
    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }
    
    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }


    //------------------------ HashCode & Equals --------------------------
    
    @Override
    public int hashCode() {
        return Objects.hash(documentId, position);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
         }
         
         if (getClass() != obj.getClass()) {
            return false;
         }
         
         final AffMatchAffiliation other = (AffMatchAffiliation) obj;
         
         return Objects.equals(documentId, other.documentId) &&
                Objects.equals(position, other.position);
    }
    
    
    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return "AffMatchAffiliation [documentId=" + documentId + ", position=" + position
                + ", organizationName=" + organizationName + ", countryName=" + countryName
                + ", countryCode=" + countryCode + "]";
    }
   

}
