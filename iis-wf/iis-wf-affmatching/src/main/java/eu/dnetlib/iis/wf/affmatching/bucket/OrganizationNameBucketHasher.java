package eu.dnetlib.iis.wf.affmatching.bucket;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * An implementation of {@link BucketHasher} that hashes {@link AffMatchOrganization}. The generated hash is based on the names
 * of the organization.
 * 
 * @author Łukasz Dumiszewski
*/

public class OrganizationNameBucketHasher implements BucketHasher<AffMatchOrganization> {
    
    
    private static final long serialVersionUID = 1L;

    private BucketHasher<String> stringHasher = new StringPartFirstLettersHasher();
    
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction = new GetOrgNameFunction(); 
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the passed organization. The hash is generated from the first name of the organization names returned
     * by the function {@link #setGetOrgNamesFunction(Function)}.<br/>
     * The method uses {@link BucketHasher#hash(String)} internally.<br/>
     * Returns null if the function {@link #setGetOrgNamesFunction(Function)} returns empty list of organization names.
     */
    @Override
    public String hash(AffMatchOrganization organization) {
        
        Preconditions.checkNotNull(organization);
        
        List<String> orgNames = getOrgNamesFunction.apply(organization);
        
        if (CollectionUtils.isEmpty(orgNames)) {
            return null;
        }
        
        return stringHasher.hash(orgNames.get(0));
    }


    //------------------------ SETTERS --------------------------
    
    /**
     * Hasher that will be used in {@link #hash(AffMatchOrganization)} to generate hash from the name of the organization. 
     */
    public void setStringHasher(BucketHasher<String> stringHasher) {
        this.stringHasher = stringHasher;
    }


    /**
     * Function returning organization names that will be used in {@link #hash(AffMatchOrganization)}. Defaults to {@link GetOrgNameFunction} 
     */
    public void setGetOrgNamesFunction(Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
        this.getOrgNamesFunction = getOrgNamesFunction;
    }

    

    
}
