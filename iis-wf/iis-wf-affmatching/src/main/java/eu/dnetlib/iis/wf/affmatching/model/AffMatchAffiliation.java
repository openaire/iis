package eu.dnetlib.iis.wf.affmatching.model;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

public class AffMatchAffiliation {
  
    
    private String documentId;
    
    private int position;
    
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

}
