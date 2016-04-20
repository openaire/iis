package eu.dnetlib.iis.wf.affmatching.normalize;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.string.LenientComparisonStringNormalizer;
import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * 
 * Normalizer of {@link AffMatchOrganization} properties
 * 
 * @author Łukasz Dumiszewski
*/

public class AffMatchOrganizationNormalizer implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    private StringNormalizer organizationNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer organizationShortNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryCodeNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer websiteUrlNormalizer = new WebsiteUrlNormalizer();

    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Creates a new {@link AffMatchOrganization} based on <code>affMatchOrganization</code>, with its properties normalized
     * by corresponding normalizers (see setters).
     */
    public AffMatchOrganization normalize(AffMatchOrganization affMatchOrganization) {
    
        Preconditions.checkNotNull(affMatchOrganization);
        
        AffMatchOrganization normalizedOrganization = new AffMatchOrganization(affMatchOrganization.getId());
        
        normalizedOrganization.setName(organizationNameNormalizer.normalize(affMatchOrganization.getName()));
    
        normalizedOrganization.setShortName(organizationShortNameNormalizer.normalize(affMatchOrganization.getShortName()));
        
        normalizedOrganization.setCountryName(countryNameNormalizer.normalize(affMatchOrganization.getCountryName()));
        
        normalizedOrganization.setCountryCode(countryCodeNormalizer.normalize(affMatchOrganization.getCountryCode()));
        
        normalizedOrganization.setWebsiteUrl(websiteUrlNormalizer.normalize(affMatchOrganization.getWebsiteUrl()));
    
        return normalizedOrganization;
    }


    
    //------------------------ SETTERS --------------------------

    public void setOrganizationNameNormalizer(StringNormalizer organizationNameNormalizer) {
        this.organizationNameNormalizer = organizationNameNormalizer;
    }

    public void setOrganizationShortNameNormalizer(StringNormalizer organizationShortNameNormalizer) {
        this.organizationShortNameNormalizer = organizationShortNameNormalizer;
    }

    public void setCountryNameNormalizer(StringNormalizer countryNameNormalizer) {
        this.countryNameNormalizer = countryNameNormalizer;
    }

    public void setCountryCodeNormalizer(StringNormalizer countryCodeNormalizer) {
        this.countryCodeNormalizer = countryCodeNormalizer;
    }

    public void setWebsiteUrlNormalizer(StringNormalizer websiteUrlNormalizer) {
        this.websiteUrlNormalizer = websiteUrlNormalizer;
    }
    
}
