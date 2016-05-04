package eu.dnetlib.iis.wf.affmatching.bucket;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * An implementation of {@link BucketHasher} that hashes {@link AffMatchOrganization}. The generated hash is based on the name
 * of the organization.
 * 
 * @author Łukasz Dumiszewski
*/

public class OrganizationNameFirstLettersBucketHasher implements BucketHasher<AffMatchOrganization> {
    
    
    private static final long serialVersionUID = 1L;

    private StringPartFirstLettersHasher stringPartFirstLettersHasher = new StringPartFirstLettersHasher();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the passed organization. The hash is generated from {@link AffMatchOrganization#getName()}.<br/>
     * The method uses {@link StringPartFirstLettersHasher} internally.
     */
    @Override
    public String hash(AffMatchOrganization organization) {
        
        Preconditions.checkNotNull(organization);
        Preconditions.checkArgument(StringUtils.isNotBlank(organization.getName()));
        
        return stringPartFirstLettersHasher.hash(organization.getName());
    }


    //------------------------ SETTERS --------------------------
    
    /**
     * Hasher that will be used in {@link #hash(AffMatchOrganization)} to generate hash from the name of the organization. 
     */
    public void setStringPartFirstLettersHasher(StringPartFirstLettersHasher stringPartFirstLettersHasher) {
        this.stringPartFirstLettersHasher = stringPartFirstLettersHasher;
    }

    

    
}
