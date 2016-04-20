package eu.dnetlib.iis.wf.affmatching.read;

import static eu.dnetlib.iis.common.string.CharSequenceUtils.toStringWithNullToEmpty;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Converter of {@link Organization} into {@link AffMatchOrganization}
 * 
* @author Łukasz Dumiszewski
*/

public class OrganizationConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Converts {@link Organization} into {@link AffMatchOrganization}
     */
    public AffMatchOrganization convert(Organization organization) {
        
        Preconditions.checkNotNull(organization);
        Preconditions.checkArgument(StringUtils.isNotBlank(organization.getId()));
        
        
        AffMatchOrganization affMatchOrg = new AffMatchOrganization(organization.getId().toString());
        
        String orgName = toStringWithNullToEmpty(organization.getName());
        if (orgName.trim().equalsIgnoreCase("missing legal name")) {
            orgName = "";
        }
        
        affMatchOrg.setName(orgName);
        
        affMatchOrg.setShortName(toStringWithNullToEmpty(organization.getShortName()));
        
        affMatchOrg.setCountryName(toStringWithNullToEmpty(organization.getCountryName()));
        
        affMatchOrg.setCountryCode(toStringWithNullToEmpty(organization.getCountryCode()));
        
        affMatchOrg.setWebsiteUrl(toStringWithNullToEmpty(organization.getWebsiteUrl()));
        
        return affMatchOrg;
    }
    
    
}
