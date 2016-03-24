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
    
    public String getRawText() {
        return rawText;
    }
    
    
    
    //------------------------ SETTERS --------------------------
    
    public void setInstitution(String institution) {
        this.institution = institution;
    }
    
    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }
    
    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }
    
    public void setAddress(String address) {
        this.address = address;
    }
    
    public void setRawText(String rawText) {
        this.rawText = rawText;
    }
    
    
    
    
}
