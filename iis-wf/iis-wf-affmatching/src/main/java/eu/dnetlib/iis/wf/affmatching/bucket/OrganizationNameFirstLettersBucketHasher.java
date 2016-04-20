package eu.dnetlib.iis.wf.affmatching.bucket;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class OrganizationNameFirstLettersBucketHasher implements BucketHasher<AffMatchOrganization> {
    
    
    private static final long serialVersionUID = 1L;

    private StringPartFirstLettersHasher stringPartFirstLettersHasher = new StringPartFirstLettersHasher();
    
    private int numberOfLettersPerPart = 2;
    
    private int numberOfParts = 2;
    
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public String hash(AffMatchOrganization organization) {
        
        Preconditions.checkNotNull(organization);
        Preconditions.checkArgument(StringUtils.isNotBlank(organization.getName()));
        
        return stringPartFirstLettersHasher.hash(organization.getName(), numberOfParts, numberOfLettersPerPart);
    }

    
    //------------------------ SETTERS --------------------------
    

    public void setNumberOfLettersPerPart(int numberOfLettersPerPart) {
        this.numberOfLettersPerPart = numberOfLettersPerPart;
    }


    public void setNumberOfParts(int numberOfParts) {
        this.numberOfParts = numberOfParts;
    }
    
}
