package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import org.apache.commons.lang3.StringUtils;

/**
 * This is an extended affiliation conveying structured affiliation details instead of a single raw text line.
 * It reflects the affiliations encoded in JATS records coming from Springer, in contrary to the ones coming from PubMed
 * where an affiliation is a simple raw string which has to be parsed in order to obtain affiliation metadata. 
 * 
 * @author mhorst
 */
public class JatsExtendedAffiliation {
    
    private StringBuilder institutionOrgDivision;
    
    private StringBuilder institutionOrgName;
    
    private StringBuilder addrLineStreet;
    
    private StringBuilder addrLinePostCode;
    
    private StringBuilder addrLineCity;
    
    private StringBuilder countryName;
    
    private String countryCode;
    
    
    //-----------------------CONSTRUCTOR-------------------------
    
    
    public JatsExtendedAffiliation() {
        initialize();
    }
    
    //--------------------------PUBLIC --------------------------
    
    /**
     * Checks the number of fields set.
     */
    public int getNumberOfFieldsSet() {
        int count = 0;
        if (institutionOrgDivision.length() > 0) count++;
        if (institutionOrgName.length() > 0) count++;
        if (addrLineStreet.length() > 0) count++;
        if (addrLinePostCode.length() > 0) count++;
        if (addrLineCity.length() > 0) count++;
        if (countryName.length() > 0) count++;
        if (StringUtils.isNotBlank(countryCode)) count++;
        return count;
    }
    
    /**
     * Clears the values of all the affiliation fields.
     */
    public void clear() {
        initialize();
    }
    
    /**
     * Generates raw text representation of the affiliation relying on the structured metadata.
     */
    public String generateRawText() {
        StringBuilder strBuilder = new StringBuilder();
        
        strBuilder = generateFullOrganizationName(strBuilder);
        strBuilder = generateFullAddress(strBuilder);
        appendWithCheckAndLeadingComma(strBuilder, countryName);
        
        return strBuilder.toString();
        
    }
    
    public String generateFullOrganizationName() {
        StringBuilder strBuilder = new StringBuilder();
        appendWithCheckAndLeadingComma(strBuilder, institutionOrgDivision);
        appendWithCheckAndLeadingComma(strBuilder, institutionOrgName);
        return strBuilder.toString();
    }
    
    public String generateFullAddress() {
        StringBuilder strBuilder = new StringBuilder();
        appendWithCheckAndLeadingComma(strBuilder, addrLineStreet);
        appendWithCheckAndLeadingComma(strBuilder, addrLinePostCode);
        if (StringUtils.isNotBlank(addrLinePostCode)) {
            appendWithCheckAndLeadingSpace(strBuilder, addrLineCity);
        } else {
            appendWithCheckAndLeadingComma(strBuilder, addrLineCity);    
        }
        
        return strBuilder.toString();
    }
    
    //--------------------------PRIVATE --------------------------
    
    private StringBuilder generateFullOrganizationName(StringBuilder strBuilder) {
        appendWithCheckAndLeadingComma(strBuilder, institutionOrgDivision);
        appendWithCheckAndLeadingComma(strBuilder, institutionOrgName);
        return strBuilder;
    }
    
    private StringBuilder generateFullAddress(StringBuilder strBuilder) {
        appendWithCheckAndLeadingComma(strBuilder, addrLineStreet);
        appendWithCheckAndLeadingComma(strBuilder, addrLinePostCode);
        if (StringUtils.isNotBlank(addrLinePostCode)) {
            appendWithCheckAndLeadingSpace(strBuilder, addrLineCity);
        } else {
            appendWithCheckAndLeadingComma(strBuilder, addrLineCity);    
        }
        
        return strBuilder;
    }
    
    private static void appendWithCheckAndLeadingComma(StringBuilder strBuilder, CharSequence phrase) {
        appendWithCheckAndLeadingSeparator(strBuilder, phrase, ", ");
    }
    
    private static void appendWithCheckAndLeadingSpace(StringBuilder strBuilder, CharSequence phrase) {
        appendWithCheckAndLeadingSeparator(strBuilder, phrase, " ");
    }
    
    private static void appendWithCheckAndLeadingSeparator(StringBuilder strBuilder, CharSequence phrase, String separator) {
        if (StringUtils.isNotBlank(phrase)) {
            if (strBuilder.length()>0) {
                strBuilder.append(separator);
            }
            strBuilder.append(phrase);
        }
    }

    /**
     * Clears the values of all the affiliation fields.
     */
    private void initialize() {
        institutionOrgDivision = new StringBuilder();
        institutionOrgName = new StringBuilder();
        addrLineStreet = new StringBuilder();
        addrLinePostCode = new StringBuilder();
        addrLineCity = new StringBuilder();
        countryName = new StringBuilder();
        countryCode = "";
    }
    
    //------------------------ GETTERS --------------------------
    
    public String getInstitutionOrgDivision() {
        return institutionOrgDivision.toString();
    }

    public String getInstitutionOrgName() {
        return institutionOrgName.toString();
    }

    public String getAddrLineStreet() {
        return addrLineStreet.toString();
    }

    public String getAddrLinePostCode() {
        return addrLinePostCode.toString();
    }

    public String getAddrLineCity() {
        return addrLineCity.toString();
    }

    public String getCountryName() {
        return countryName.toString();
    }

    public String getCountryCode() {
        return countryCode;
    }
    
    
    //------------------------ APPENDERS/SETTERS --------------------------
    
    public void appendToInstitutionOrgDivision(String institutionOrgDivision) {
        if (institutionOrgDivision != null) {
            this.institutionOrgDivision.append(institutionOrgDivision);    
        }
    }

    public void appendToInstitutionOrgName(String institutionOrgName) {
        if (institutionOrgName != null) {
            this.institutionOrgName.append(institutionOrgName);    
        }
    }

    public void appendToAddrLineStreet(String addrLineStreet) {
        if (addrLineStreet != null) {
            this.addrLineStreet.append(addrLineStreet);    
        }
    }

    public void appendToAddrLinePostCode(String addrLinePostCode) {
        if (addrLinePostCode != null) {
            this.addrLinePostCode.append(addrLinePostCode);    
        }
    }

    public void appendToAddrLineCity(String addrLineCity) {
        if (addrLineCity != null) {
            this.addrLineCity.append(addrLineCity);    
        }
    }

    public void appendToCountryName(String countryName) {
        if (countryName != null) {
            this.countryName.append(countryName);    
        }
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }
    
}
