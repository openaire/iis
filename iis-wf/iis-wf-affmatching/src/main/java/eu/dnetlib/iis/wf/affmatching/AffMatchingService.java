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

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchAffiliationNormalizer;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchOrganizationNormalizer;
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
    
    private DocumentOrganizationReader documentOrganizationReader;
    
    
    private AffMatchAffiliationNormalizer affMatchAffiliationNormalizer = new AffMatchAffiliationNormalizer();
    
    private AffMatchOrganizationNormalizer affMatchOrganizationNormalizer = new AffMatchOrganizationNormalizer();
    
    
    private List<AffOrgMatcher> affOrgMatchers;
    
    
    private AffMatchResultWriter affMatchResultWriter;
    
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Matches the affiliations from <code>inputAffPath</code> with organizations from <code>inputOrgPath</code>.
     * Saves the result in <code>outputPath</code>.
     */
    public void matchAffiliations(JavaSparkContext sc, String inputAffPath, String inputOrgPath, String inputDocProjPath, String inputProjOrgPath, String outputPath) {

        checkArguments(sc, inputAffPath, inputOrgPath, inputDocProjPath, inputProjOrgPath, outputPath);
        
        checkState();
        
        
        JavaRDD<AffMatchAffiliation> affiliations = affiliationReader.readAffiliations(sc, inputAffPath).filter(aff -> (StringUtils.isNotBlank(aff.getOrganizationName())));
        
        JavaRDD<AffMatchOrganization> organizations = organizationReader.readOrganizations(sc, inputOrgPath).filter(org -> (StringUtils.isNotBlank(org.getName())));
        
        JavaRDD<AffMatchDocumentOrganization> documentOrganizations = documentOrganizationReader.readDocumentOrganization(sc, inputDocProjPath, inputProjOrgPath);
        
        
        JavaRDD<AffMatchAffiliation> normalizedAffiliations = affiliations.map(aff -> affMatchAffiliationNormalizer.normalize(aff));
        
        JavaRDD<AffMatchOrganization> normalizedOrganizations = organizations.map(org -> affMatchOrganizationNormalizer.normalize(org));
        

        JavaRDD<AffMatchResult> allMatchedAffOrgs = doMatch(sc, normalizedAffiliations, normalizedOrganizations, documentOrganizations);
        
        
        affMatchResultWriter.write(allMatchedAffOrgs, outputPath);
        
    }



    
    
    //------------------------ PRIVATE --------------------------

    
    private void checkState() {
        
        Preconditions.checkNotNull(organizationReader, "organizationReader has not been set");
        
        Preconditions.checkNotNull(affiliationReader, "affiliationReader has not been set");
        
        Preconditions.checkNotNull(documentOrganizationReader, "documentOrganizationReader has not been set");
        
        Preconditions.checkNotNull(affMatchResultWriter, "affMatchResultWriter has not been set");
        
        Preconditions.checkState(CollectionUtils.isNotEmpty(affOrgMatchers), "no AffOrgMatcher has been set");
    }


    

    private void checkArguments(JavaSparkContext sc, String inputAffPath, String inputOrgPath, String inputDocProjPath, String inputProjOrgPath, String outputPath) {
        
        Preconditions.checkNotNull(sc);
        
        Preconditions.checkArgument(StringUtils.isNotBlank(inputAffPath));
        
        Preconditions.checkArgument(StringUtils.isNotBlank(inputOrgPath));
        
        Preconditions.checkArgument(StringUtils.isNotBlank(inputDocProjPath));
        
        Preconditions.checkArgument(StringUtils.isNotBlank(inputProjOrgPath));
        
        Preconditions.checkArgument(StringUtils.isNotBlank(outputPath));
    }



    private JavaRDD<AffMatchResult> doMatch(JavaSparkContext sc, JavaRDD<AffMatchAffiliation> normalizedAffiliations, JavaRDD<AffMatchOrganization> normalizedOrganizations,
            JavaRDD<AffMatchDocumentOrganization> documentOrganizations) {
        
        JavaPairRDD<String, AffMatchAffiliation> idAffiliations = normalizedAffiliations.keyBy(aff -> aff.getId());
        
        JavaRDD<AffMatchResult> allMatchedAffOrgs = sc.parallelize(new ArrayList<>());
        
        
        for (AffOrgMatcher affOrgMatcher : affOrgMatchers) {
            
            JavaRDD<AffMatchResult> matchedAffOrgs = affOrgMatcher.match(idAffiliations.values(), normalizedOrganizations, documentOrganizations);
            
            allMatchedAffOrgs = allMatchedAffOrgs.union(matchedAffOrgs);
            
            
            JavaPairRDD<String, String> matchedAffIds = matchedAffOrgs.mapToPair(affMatchOrg -> new Tuple2<>(affMatchOrg.getAffiliation().getId(), ""));
            
            idAffiliations = idAffiliations.subtractByKey(matchedAffIds);
        }
        
        return allMatchedAffOrgs;
    }
    
    
    
    //------------------------ SETTERS --------------------------
    
    public void setOrganizationReader(OrganizationReader organizationReader) {
        this.organizationReader = organizationReader;
    }
    
    public void setAffiliationReader(AffiliationReader affiliationReader) {
        this.affiliationReader = affiliationReader;
    }

    public void setDocumentOrganizationReader(DocumentOrganizationReader documentOrganizationReader) {
        this.documentOrganizationReader = documentOrganizationReader;
    }

    public void setAffOrgMatchers(List<AffOrgMatcher> affOrgMatchers) {
        this.affOrgMatchers = affOrgMatchers;
    }

    public void setAffMatchResultWriter(AffMatchResultWriter affMatchResultWriter) {
        this.affMatchResultWriter = affMatchResultWriter;
    }




}
