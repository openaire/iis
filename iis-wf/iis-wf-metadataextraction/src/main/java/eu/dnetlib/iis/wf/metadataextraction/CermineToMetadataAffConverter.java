package eu.dnetlib.iis.wf.metadataextraction;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.importer.CermineAffiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
/**
 * Converter of {@link CermineAffiliation} to {@link Affiliation}
 * 
* @author Łukasz Dumiszewski
*/

class CermineToMetadataAffConverter {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link CermineAffiliation} to {@link Affiliation}
     */
    Affiliation convert(CermineAffiliation cAff) {
        
        Preconditions.checkNotNull(cAff);
        
        return Affiliation.newBuilder()
                            .setRawText(cAff.getRawText())
                            .setOrganization(cAff.getInstitution())
                            .setAddress(cAff.getAddress())
                            .setCountryName(cAff.getCountryName())
                            .setCountryCode(cAff.getCountryCode())
                            .build();
    }
    
}
