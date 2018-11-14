package eu.dnetlib.iis.wf.affmatching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.match.AffMatchResultChooser;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchAffiliationNormalizer;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchOrganizationNormalizer;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.AffMatchOrganizationAltNameFiller;
import eu.dnetlib.iis.wf.affmatching.read.AffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.OrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.AffMatchResultWriter;
import scala.Tuple2;

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
    
    
    private OrganizationReader organizationReader;
    
    private AffiliationReader affiliationReader;
    
    
    private AffMatchAffiliationNormalizer affMatchAffiliationNormalizer = new AffMatchAffiliationNormalizer();
    
    private AffMatchOrganizationNormalizer affMatchOrganizationNormalizer = new AffMatchOrganizationNormalizer();
    
    private AffMatchOrganizationAltNameFiller affMatchOrganizationAltNameFiller = new AffMatchOrganizationAltNameFiller();
    
    
    private List<AffOrgMatcher> affOrgMatchers;
    
    private AffMatchResultChooser affMatchResultChooser = new AffMatchResultChooser();
    
    
    private AffMatchResultWriter affMatchResultWriter;
    
    private int numberOfEmittedFiles;
    
    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Matches the affiliations from <code>inputAffPath</code> with organizations from <code>inputOrgPath</code>.
     * Saves the result in <code>outputPath</code>.
     */
    public void matchAffiliations(JavaSparkContext sc, String inputAffPath, String inputOrgPath, String outputPath, String outputReportPath) {

        checkArguments(sc, inputAffPath, inputOrgPath, outputPath);
        
        checkState();
        
        
        JavaRDD<AffMatchAffiliation> affiliations = affiliationReader.readAffiliations(sc, inputAffPath);
        
        JavaRDD<AffMatchOrganization> organizations = organizationReader.readOrganizations(sc, inputOrgPath);
        
        
        JavaRDD<AffMatchAffiliation> normalizedAffiliations = affiliations.map(aff -> affMatchAffiliationNormalizer.normalize(aff)).filter(aff -> (StringUtils.isNotBlank(aff.getOrganizationName())));
        
        JavaRDD<AffMatchOrganization> normalizedOrganizations = organizations.map(org -> affMatchOrganizationNormalizer.normalize(org)).filter(org -> (StringUtils.isNotBlank(org.getName())));
        
        
        JavaRDD<AffMatchOrganization> enrichedOrganizations = normalizedOrganizations.map(x -> affMatchOrganizationAltNameFiller.fillAlternativeNames(x));
        
        JavaRDD<AffMatchResult> allMatchedAffOrgs = doMatch(sc, normalizedAffiliations, enrichedOrganizations);
        
        
        affMatchResultWriter.write(sc, allMatchedAffOrgs, outputPath, outputReportPath, numberOfEmittedFiles);
        
    }



    
    
    //------------------------ PRIVATE --------------------------

    
    private void checkState() {
        
        Preconditions.checkNotNull(organizationReader, "organizationReader has not been set");
        
        Preconditions.checkNotNull(affiliationReader, "affiliationReader has not been set");
        
        Preconditions.checkNotNull(affMatchResultWriter, "affMatchResultWriter has not been set");
        
        Preconditions.checkNotNull(affMatchOrganizationAltNameFiller, "affMatchOrganizationAltNameFiller has not been set");
        
        Preconditions.checkState(CollectionUtils.isNotEmpty(affOrgMatchers), "no AffOrgMatcher has been set");
    }


    

    private void checkArguments(JavaSparkContext sc, String inputAffPath, String inputOrgPath, String outputPath) {
        
        Preconditions.checkNotNull(sc);
        
        Preconditions.checkArgument(StringUtils.isNotBlank(inputAffPath));
        
        Preconditions.checkArgument(StringUtils.isNotBlank(inputOrgPath));
        
        Preconditions.checkArgument(StringUtils.isNotBlank(outputPath));
    }



    private JavaRDD<AffMatchResult> doMatch(JavaSparkContext sc, JavaRDD<AffMatchAffiliation> normalizedAffiliations, JavaRDD<AffMatchOrganization> normalizedOrganizations) {
        
        JavaPairRDD<Tuple2<String, String>, AffMatchResult> allMatchedAffOrgsWithKey = sc.parallelizePairs(new ArrayList<>());
        
        
        for (AffOrgMatcher affOrgMatcher : affOrgMatchers) {
            
            JavaRDD<AffMatchResult> matchedAffOrgs = affOrgMatcher.match(normalizedAffiliations, normalizedOrganizations);
            
            JavaPairRDD<Tuple2<String, String>, AffMatchResult> matchedAffOrgsWithKey = matchedAffOrgs
                    .keyBy(x -> new Tuple2<>(x.getAffiliation().getId(), x.getOrganization().getId()));
            
            allMatchedAffOrgsWithKey = allMatchedAffOrgsWithKey.union(matchedAffOrgsWithKey);
            
            
        }
        
        JavaRDD<AffMatchResult> allMatchedAffOrgs = allMatchedAffOrgsWithKey
                .reduceByKey((r1, r2) -> affMatchResultChooser.chooseBetter(r1, r2))
                .values();
        
        
        return allMatchedAffOrgs;
    }
    
    
    
    //------------------------ SETTERS --------------------------
    
    public void setOrganizationReader(OrganizationReader organizationReader) {
        this.organizationReader = organizationReader;
    }
    
    public void setAffiliationReader(AffiliationReader affiliationReader) {
        this.affiliationReader = affiliationReader;
    }

    public void setAffMatchOrganizationAltNameFiller(AffMatchOrganizationAltNameFiller affMatchOrganizationAltNameFiller) {
        this.affMatchOrganizationAltNameFiller = affMatchOrganizationAltNameFiller;
    }

    public void setAffOrgMatchers(List<AffOrgMatcher> affOrgMatchers) {
        this.affOrgMatchers = affOrgMatchers;
    }

    public void setAffMatchResultWriter(AffMatchResultWriter affMatchResultWriter) {
        this.affMatchResultWriter = affMatchResultWriter;
    }

    public void setNumberOfEmittedFiles(int numberOfEmittedFiles) {
        this.numberOfEmittedFiles = numberOfEmittedFiles;
    }


}
