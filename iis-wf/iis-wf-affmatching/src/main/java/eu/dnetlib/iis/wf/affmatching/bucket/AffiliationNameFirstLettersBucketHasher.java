package eu.dnetlib.iis.wf.affmatching.bucket;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;

/**
* @author Łukasz Dumiszewski
*/

public class AffiliationNameFirstLettersBucketHasher implements BucketHasher<AffMatchAffiliation> {

    
    private static final long serialVersionUID = 1L;

    private StringPartFirstLettersHasher stringPartFirstLettersHasher = new StringPartFirstLettersHasher();
    
    private int numberOfLettersPerPart = 2;
    
    private int numberOfParts = 2;
    
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public String hash(AffMatchAffiliation affiliation) {
        
        Preconditions.checkNotNull(affiliation);
        Preconditions.checkArgument(StringUtils.isNotBlank(affiliation.getOrganizationName()));
        
        return stringPartFirstLettersHasher.hash(affiliation.getOrganizationName(), numberOfParts, numberOfLettersPerPart);
    }

    
    //------------------------ SETTERS --------------------------
    

    public void setNumberOfLettersPerPart(int numberOfLettersPerPart) {
        this.numberOfLettersPerPart = numberOfLettersPerPart;
    }


    public void setNumberOfParts(int numberOfParts) {
        this.numberOfParts = numberOfParts;
    }
    
    
    

}
