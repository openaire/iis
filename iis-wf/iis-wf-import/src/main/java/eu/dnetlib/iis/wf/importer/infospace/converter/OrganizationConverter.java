package eu.dnetlib.iis.wf.importer.infospace.converter;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.Organization;

/**
 * Converter of {@link eu.dnetlib.dhp.schema.oaf.Organization} object into {@link Organization}
 * 
 * 
 * @author Łukasz Dumiszewski
*/

public class OrganizationConverter implements OafEntityToAvroConverter<eu.dnetlib.dhp.schema.oaf.Organization, Organization> {

    private static final long serialVersionUID = -2774070399465323718L;
    
    private static Logger log = Logger.getLogger(OrganizationConverter.class);
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link eu.dnetlib.dhp.schema.oaf.Organization} object into {@link Organization}
     */
    @Override
    public Organization convert(eu.dnetlib.dhp.schema.oaf.Organization srcOrganization) {

        Preconditions.checkNotNull(srcOrganization);
        
        if (!isDataCorrect(srcOrganization)) {
        
            return null;
        
        }
        
        Organization.Builder orgBuilder = Organization.newBuilder();
        
        orgBuilder.setId(srcOrganization.getId());
        
        orgBuilder.setName(srcOrganization.getLegalname().getValue());
        
        orgBuilder.setShortName(srcOrganization.getLegalshortname() != null ? srcOrganization.getLegalshortname().getValue() : null);    
     
        orgBuilder.setCountryName(srcOrganization.getCountry() != null ? srcOrganization.getCountry().getClassname() : null);

        orgBuilder.setCountryCode(srcOrganization.getCountry() != null ? srcOrganization.getCountry().getClassid() : null);
     
        orgBuilder.setWebsiteUrl(srcOrganization.getWebsiteurl() != null ? srcOrganization.getWebsiteurl().getValue(): null);    
        
        return orgBuilder.build();
        
    }
    

    //------------------------ PRIVATE --------------------------
    
    private boolean isDataCorrect(eu.dnetlib.dhp.schema.oaf.Organization srcOrganization) {
        
        if (srcOrganization.getLegalname() == null || StringUtils.isBlank(srcOrganization.getLegalname().getValue())) {
            
            log.error("skipping: empty organization name");
            
            return false;
            
        }
        
        return true;
    }

}
