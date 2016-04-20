package eu.dnetlib.iis.wf.affmatching;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.bucket.AffiliationNameFirstLettersBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.BucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationNameFirstLettersBucketHasher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchAffiliationNormalizer;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchOrganizationNormalizer;
import eu.dnetlib.iis.wf.affmatching.read.AffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.read.OrganizationReader;
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

    
    private OrganizationReader organizationReader = new IisOrganizationReader();
    
    private AffiliationReader affiliationReader = new IisAffiliationReader();
    
    
    private AffMatchAffiliationNormalizer affMatchAffiliationNormalizer = new AffMatchAffiliationNormalizer();
    
    private AffMatchOrganizationNormalizer affMatchOrganizationNormalizer = new AffMatchOrganizationNormalizer();
    
    
    private BucketHasher<AffMatchAffiliation> affiliationBucketHasher = new AffiliationNameFirstLettersBucketHasher();
    
    private BucketHasher<AffMatchOrganization> organizationBucketHasher = new OrganizationNameFirstLettersBucketHasher();
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Matches the affiliations from <code>inputAffPath</code> with organizations from <code>inputOrgPath</code>.
     * Saves the result in <code>outputPath</code>.
     */
    public void matchAffiliations(JavaSparkContext sc, String inputAffPath, String inputOrgPath, String outputPath) {
        
        
        JavaRDD<AffMatchAffiliation> affiliations = affiliationReader.readAffiliations(sc, inputAffPath).filter(aff -> (StringUtils.isNotBlank(aff.getOrganizationName())));
        
        JavaRDD<AffMatchOrganization> organizations = organizationReader.readOrganizations(sc, inputOrgPath).filter(org -> (StringUtils.isNotBlank(org.getName())));
        
        
        
        JavaRDD<AffMatchAffiliation> normalizedAffiliations = affiliations.map(aff -> affMatchAffiliationNormalizer.normalize(aff));
        
        JavaRDD<AffMatchOrganization> normalizedOrganizations = organizations.map(org -> affMatchOrganizationNormalizer.normalize(org));
        
        
        
        JavaPairRDD<String, AffMatchAffiliation> hashAffiliations = normalizedAffiliations.mapToPair(aff -> new Tuple2<String, AffMatchAffiliation>(affiliationBucketHasher.hash(aff), aff)); 
        
        JavaPairRDD<String, AffMatchOrganization> hashOrganizations = normalizedOrganizations.mapToPair(org -> new Tuple2<String, AffMatchOrganization>(organizationBucketHasher.hash(org), org)); 

        
        
        JavaPairRDD<String, Tuple2<AffMatchAffiliation, AffMatchOrganization>> hashAffOrg = hashAffiliations.join(hashOrganizations);
        
        
        //SparkAvroSaver.saveJavaRDD(organizations, AffMatchOrganization.SCHEMA$, params.outputAvroPath);

        //SparkAvroSaver.saveJavaRDD(affiliations, AffMatchAffiliation.SCHEMA$, params.outputAvroAffPath);

        
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

    public void setAffiliationBucketHasher(BucketHasher<AffMatchAffiliation> affiliationBucketHasher) {
        this.affiliationBucketHasher = affiliationBucketHasher;
    }

    public void setOrganizationBucketHasher(BucketHasher<AffMatchOrganization> organizationBucketHasher) {
        this.organizationBucketHasher = organizationBucketHasher;
    }
    
}
