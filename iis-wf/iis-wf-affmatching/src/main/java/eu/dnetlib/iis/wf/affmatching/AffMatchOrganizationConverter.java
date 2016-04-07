package eu.dnetlib.iis.wf.affmatching;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.string.LenientComparisonStringNormalizer;
import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Converter of {@link Organization} into {@link AffMatchOrganization}
 * 
* @author Łukasz Dumiszewski
*/

public class AffMatchOrganizationConverter {

    
    private StringNormalizer organizationNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer websiteUrlNormalizer = new WebsiteUrlNormalizer();
    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Converts {@link Organization} into {@link AffMatchOrganization}
     */
    public AffMatchOrganization convert(Organization organization) {
        
        Preconditions.checkNotNull(organization);
        
        AffMatchOrganization affMatchOrg = new AffMatchOrganization();
        
        affMatchOrg.setId(organization.getId());
        
        affMatchOrg.setName(organizationNameNormalizer.normalize(organization.getName().toString()));
        
        if (affMatchOrg.getName().equals("missing legal name")) {
            affMatchOrg.setName("");
        }
        
        affMatchOrg.setShortName(organization.getShortName());
        
        affMatchOrg.setCountryName(countryNameNormalizer.normalize(organization.getCountryName().toString()));
        
        affMatchOrg.setCountryCode(organization.getCountryCode());
        
        affMatchOrg.setWebsiteUrl(websiteUrlNormalizer.normalize(organization.getWebsiteUrl().toString()));
        
        return affMatchOrg;
    }
    
    
    //------------------------ SETTERS --------------------------

    public void setOrganizationNameNormalizer(StringNormalizer organizationNameNormalizer) {
        this.organizationNameNormalizer = organizationNameNormalizer;
    }

    public void setCountryNameNormalizer(StringNormalizer countryNameNormalizer) {
        this.countryNameNormalizer = countryNameNormalizer;
    }
    
    public void setWebsiteUrlNormalizer(StringNormalizer websiteUrlNormalizer) {
        this.websiteUrlNormalizer = websiteUrlNormalizer;
    }
    
}
