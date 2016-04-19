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
     * Normalizes properties of <code>affMatchAffiliation</code> with corresponding normalizers (see
     * setters)<br/><br/>
     * This method <b>changes the passed object and returns it</b>. It does NOT create a new one.
     */
    public AffMatchAffiliation normalize(AffMatchAffiliation affMatchAffiliation) {
    
        Preconditions.checkNotNull(affMatchAffiliation);
        
        affMatchAffiliation.setOrganizationName(organizationNameNormalizer.normalize(affMatchAffiliation.getOrganizationName()));
        
        affMatchAffiliation.setCountryName(countryNameNormalizer.normalize(affMatchAffiliation.getCountryName()));
        
        affMatchAffiliation.setCountryCode(countryCodeNormalizer.normalize(affMatchAffiliation.getCountryCode()));
    
        return affMatchAffiliation;
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
