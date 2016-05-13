package eu.dnetlib.iis.wf.importer.infospace.converter;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OrganizationProtos.Organization.Metadata;
import eu.dnetlib.iis.importer.schemas.Organization;

/**
 * Converter of {@link OafEntity} object containing {@link eu.dnetlib.data.proto.OrganizationProtos.Organization}
 * into {@link Organization}
 * 
 * 
 * @author Łukasz Dumiszewski
*/

public class OrganizationConverter implements OafEntityToAvroConverter<Organization> {

    
    private static Logger log = Logger.getLogger(OrganizationConverter.class);
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link OafEntity} object containing {@link eu.dnetlib.data.proto.OrganizationProtos.Organization}
     * into {@link Organization}
     */
    @Override
    public Organization convert(OafEntity oafEntity) {

        Preconditions.checkNotNull(oafEntity);
        
        if (!isDataCorrect(oafEntity)) {
        
            return null;
        
        }
        
        eu.dnetlib.data.proto.OrganizationProtos.Organization srcOrganization = oafEntity.getOrganization();
        
        
        Organization.Builder orgBuilder = Organization.newBuilder();
        
        orgBuilder.setId(oafEntity.getId());
        
        Metadata srcOrgMetadata = srcOrganization.getMetadata();
        
        orgBuilder.setName(srcOrgMetadata.getLegalname().getValue());
        
        orgBuilder.setShortName(srcOrgMetadata.getLegalshortname().getValue());
     
        orgBuilder.setCountryName(srcOrgMetadata.getCountry().getClassname());
        
        orgBuilder.setCountryCode(srcOrgMetadata.getCountry().getClassid());
        
        orgBuilder.setWebsiteUrl(srcOrgMetadata.getWebsiteurl().getValue());
     
        
        
        return orgBuilder.build();
        
    }
    

    //------------------------ PRIVATE --------------------------
    
    private boolean isDataCorrect(OafEntity oafEntity) {
        

        eu.dnetlib.data.proto.OrganizationProtos.Organization srcOrganization = oafEntity.getOrganization();
        
        if (srcOrganization.getMetadata().getLegalname() == null || StringUtils.isBlank(srcOrganization.getMetadata().getLegalname().getValue())) {
            
            log.error("skipping: empty organization name");
            
            return false;
            
        }

        
        return true;
    }

}
