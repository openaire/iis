package eu.dnetlib.iis.wf.affmatching;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchAffiliationNormalizer;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchOrganizationNormalizer;
import eu.dnetlib.iis.wf.affmatching.read.AffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.read.OrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.AffMatchResultWriter;
import eu.dnetlib.iis.wf.affmatching.write.IisAffMatchResultWriter;

/**
 * Configurable affiliation matching service.<br/><br/>
 * It reads, normalizes, matches and writes the matched affiliation-organization pairs.
 *  
 * 
 * 
 * @author Łukasz Dumiszewski
*/

public class AffMatchingService implements Serializable {
    

    private static final long serialVersionUID = 1L;

    
    private OrganizationReader organizationReader = new IisOrganizationReader();
    
    private AffiliationReader affiliationReader = new IisAffiliationReader();
    
    
    private AffMatchAffiliationNormalizer affMatchAffiliationNormalizer = new AffMatchAffiliationNormalizer();
    
    private AffMatchOrganizationNormalizer affMatchOrganizationNormalizer = new AffMatchOrganizationNormalizer();
    
    
    private AffOrgMatcher affOrgMatcher = new AffOrgMatcher();
    
    
    private AffMatchResultWriter affMatchResultWriter = new IisAffMatchResultWriter();
    
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Matches the affiliations from <code>inputAffPath</code> with organizations from <code>inputOrgPath</code>.
     * Saves the result in <code>outputPath</code>.
     */
    public void matchAffiliations(JavaSparkContext sc, String inputAffPath, String inputOrgPath, String outputPath) {
        
        
        JavaRDD<AffMatchAffiliation> affiliations = affiliationReader.readAffiliations(sc, inputAffPath).filter(aff -> (StringUtils.isNotBlank(aff.getOrganizationName())));
        
        JavaRDD<AffMatchOrganization> organizations = organizationReader.readOrganizations(sc, inputOrgPath).filter(org -> (StringUtils.isNotBlank(org.getName())));
        
        
        Preconditions.checkNotNull(affiliations);
        
        Preconditions.checkNotNull(organizations);
        
        
        JavaRDD<AffMatchAffiliation> normalizedAffiliations = affiliations.map(aff -> affMatchAffiliationNormalizer.normalize(aff));
        
        JavaRDD<AffMatchOrganization> normalizedOrganizations = organizations.map(org -> affMatchOrganizationNormalizer.normalize(org));
        
        
        JavaRDD<AffMatchResult> matchedAffOrgs = affOrgMatcher.match(normalizedAffiliations, normalizedOrganizations);        
        
        
        affMatchResultWriter.write(matchedAffOrgs, outputPath);
        
    }
    
    
    
    //------------------------ SETTERS --------------------------
    
    public void setOrganizationReader(OrganizationReader organizationReader) {
        this.organizationReader = organizationReader;
    }
    
    public void setAffiliationReader(AffiliationReader affiliationReader) {
        this.affiliationReader = affiliationReader;
    }

    public void setAffMatchAffiliationNormalizer(AffMatchAffiliationNormalizer affMatchAffiliationNormalizer) {
        this.affMatchAffiliationNormalizer = affMatchAffiliationNormalizer;
    }

    public void setAffMatchOrganizationNormalizer(AffMatchOrganizationNormalizer affMatchOrganizationNormalizer) {
        this.affMatchOrganizationNormalizer = affMatchOrganizationNormalizer;
    }

    public void setAffMatcher(AffOrgMatcher affMatcher) {
        this.affOrgMatcher = affMatcher;
    }

    
   
    
}
