package eu.dnetlib.iis.wf.affmatching.bucket;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;

/**
 * An implementation of {@link BucketHasher} that hashes {@link AffMatchAffiliation}. The generated hash is based on the name
 * of the affiliation organization.
 * 
 * @author Łukasz Dumiszewski
*/
public class AffiliationOrgNameFirstLettersBucketHasher implements BucketHasher<AffMatchAffiliation> {

    
    private static final long serialVersionUID = 1L;

    private StringPartFirstLettersHasher stringPartFirstLettersHasher = new StringPartFirstLettersHasher();
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the passed affiliation. The hash is generated from {@link AffMatchAffiliation#getOrganizationName()}.<br/>
     * The method uses {@link StringPartFirstLettersHasher#hash(String, int, int)} internally.
     */
    @Override
    public String hash(AffMatchAffiliation affiliation) {
        
        Preconditions.checkNotNull(affiliation);
        Preconditions.checkArgument(StringUtils.isNotBlank(affiliation.getOrganizationName()));
        
        return stringPartFirstLettersHasher.hash(affiliation.getOrganizationName());
    }


    //------------------------ SETTERS --------------------------

    /**
     * Hasher that will be used in {@link #hash(AffMatchAffiliation)} to generate hash from the name of the organization. 
     */
    public void setStringPartFirstLettersHasher(StringPartFirstLettersHasher stringPartFirstLettersHasher) {
        this.stringPartFirstLettersHasher = stringPartFirstLettersHasher;
    }

        
    

}
