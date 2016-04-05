package eu.dnetlib.iis.common.importer;

/**
 * A representation of an affiliation extracted by cermine
 * 
* @author Łukasz Dumiszewski
*/

public class CermineAffiliation {
    
    private String institution;
    private String countryName;
    private String countryCode;
    private String address;
    private String rawText;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    CermineAffiliation() {}
    
    
    //------------------------ GETTERS --------------------------
    
    public String getInstitution() {
        return institution;
    }
    
    public String getCountryName() {
        return countryName;
    }
    
    public String getCountryCode() {
        return countryCode;
    }
    
    public String getAddress() {
        return address;
    }
    
    /** 
     * Affiliation raw text. <br/>
     * Cermine enriches the affiliation it parses with tags. It does NOT remove any affiliation data even if it is not
     * able to generate any sensible tag. This method returns the affiliation text without tags (the tags are removed).
     * */
    public String getRawText() {
        return rawText;
    }
    
    
    
    //------------------------ SETTERS --------------------------
    
    void setInstitution(String institution) {
        this.institution = institution;
    }
    
    void setCountryName(String countryName) {
        this.countryName = countryName;
    }
    
    void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }
    
    void setAddress(String address) {
        this.address = address;
    }
    
    void setRawText(String rawText) {
        this.rawText = rawText;
    }
    
    
    
    
}
