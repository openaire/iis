package eu.dnetlib.iis.wf.affmatching.model;

import com.google.common.base.Preconditions;

/**
 * Affiliation-organization matching result.
 * 
 * @author Łukasz Dumiszewski
*/

public class AffMatchResult {
    
    
    private AffMatchAffiliation affiliation;
    
    private AffMatchOrganization organization;
    
    private float matchStrength;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public AffMatchResult(AffMatchAffiliation affiliation, AffMatchOrganization organization, float matchStrength) {
        
        Preconditions.checkNotNull(affiliation);
        
        Preconditions.checkNotNull(organization);
        
        Preconditions.checkArgument(matchStrength >= 0);
        
        
        this.affiliation = affiliation;
        
        this.organization = organization;
        
        this.matchStrength = matchStrength;
    }

    
    //------------------------ GETTERS --------------------------
    
    public AffMatchAffiliation getAffiliation() {
        return affiliation;
    }


    public AffMatchOrganization getOrganization() {
        return organization;
    }

    /**
     * Tells how much the {@link #getAffiliation()} and {@link #getOrganization()} match.
     * 0 - they do not much at all. 
     */
    public float getMatchStrength() {
        return matchStrength;
    }
    
    
    
}
