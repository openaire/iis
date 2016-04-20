package eu.dnetlib.iis.wf.affmatching.normalize;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.string.LenientComparisonStringNormalizer;
import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;

/**
 * 
 * Normalizer of {@link AffMatchAffiliation} properties
 * 
 * @author Łukasz Dumiszewski
*/

public class AffMatchAffiliationNormalizer implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    private StringNormalizer organizationNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryCodeNormalizer = new LenientComparisonStringNormalizer();
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Creates a new {@link AffMatchAffiliation} based on <code>affMatchAffiliation</code>, with its properties normalized
     * by corresponding normalizers (see setters).
     */
    public AffMatchAffiliation normalize(AffMatchAffiliation affMatchAffiliation) {
    
        Preconditions.checkNotNull(affMatchAffiliation);
        
        AffMatchAffiliation normalizedAffiliation = new AffMatchAffiliation(affMatchAffiliation.getDocumentId(), affMatchAffiliation.getPosition());
        
        normalizedAffiliation.setOrganizationName(organizationNameNormalizer.normalize(affMatchAffiliation.getOrganizationName()));
        
        normalizedAffiliation.setCountryName(countryNameNormalizer.normalize(affMatchAffiliation.getCountryName()));
        
        normalizedAffiliation.setCountryCode(countryCodeNormalizer.normalize(affMatchAffiliation.getCountryCode()));
    
        return normalizedAffiliation;
    }


    //------------------------ SETTERS --------------------------

    public void setOrganizationNameNormalizer(StringNormalizer organizationNameNormalizer) {
        this.organizationNameNormalizer = organizationNameNormalizer;
    }

    public void setCountryNameNormalizer(StringNormalizer countryNameNormalizer) {
        this.countryNameNormalizer = countryNameNormalizer;
    }

    public void setCountryCodeNormalizer(StringNormalizer countryCodeNormalizer) {
        this.countryCodeNormalizer = countryCodeNormalizer;
    }
    
    
}
