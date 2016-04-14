package eu.dnetlib.iis.wf.affmatching;

import static eu.dnetlib.iis.common.string.CharSequenceUtils.toStringWithNullToEmpty;

import org.apache.commons.lang3.StringUtils;

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

public class OrganizationConverter {

    
    private StringNormalizer organizationNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer organizationShortNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryCodeNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer websiteUrlNormalizer = new WebsiteUrlNormalizer();
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Converts {@link Organization} into {@link AffMatchOrganization}
     */
    public AffMatchOrganization convert(Organization organization) {
        
        Preconditions.checkNotNull(organization);
        Preconditions.checkArgument(StringUtils.isNotBlank(organization.getId()));
        
        
        AffMatchOrganization affMatchOrg = new AffMatchOrganization();
        
        affMatchOrg.setId(organization.getId());
        
        
        affMatchOrg.setName(organizationNameNormalizer.normalize(toStringWithNullToEmpty(organization.getName())));
        
        if (affMatchOrg.getName().equals("missing legal name")) {
            affMatchOrg.setName("");
        }
        
        affMatchOrg.setShortName(organizationShortNameNormalizer.normalize(toStringWithNullToEmpty(organization.getShortName())));
        
        affMatchOrg.setCountryName(countryNameNormalizer.normalize(toStringWithNullToEmpty(organization.getCountryName())));
        
        affMatchOrg.setCountryCode(countryCodeNormalizer.normalize(toStringWithNullToEmpty(organization.getCountryCode())));
        
        affMatchOrg.setWebsiteUrl(websiteUrlNormalizer.normalize(toStringWithNullToEmpty(organization.getWebsiteUrl())));
        
        return affMatchOrg;
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
