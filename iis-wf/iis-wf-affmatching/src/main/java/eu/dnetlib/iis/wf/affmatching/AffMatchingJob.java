package eu.dnetlib.iis.wf.affmatching;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Job matching affiliations with organizations.
 * 
 * @param inputAvroOrgPath path to directory with avro files containing organizations 
 * 
 * @author Łukasz Dumiszewski
 */

public class AffMatchingJob {
    
    private static OrganizationConverter organizationConverter = new OrganizationConverter();
    private static AffiliationConverter affiliationConverter = new AffiliationConverter();
    
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AffMatchingJobParameters params = new AffMatchingJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
          
            
            JavaRDD<Organization> sourceOrganizations = SparkAvroLoader.loadJavaRDD(sc, params.inputAvroOrgPath, Organization.class);
            JavaRDD<AffMatchOrganization> organizations = sourceOrganizations.map(srcOrg -> organizationConverter.convert(srcOrg));
            
            JavaRDD<ExtractedDocumentMetadata> sourceAffiliations = SparkAvroLoader.loadJavaRDD(sc, params.inputAvroAffPath, ExtractedDocumentMetadata.class);
            JavaRDD<AffMatchAffiliation> affiliations = sourceAffiliations.flatMap(srcAff -> affiliationConverter.convert(srcAff)).filter(aff -> (aff.getOrganizationName().length() > 0));  
                    
            // TODO: ACTUAL MATCHING
            
            SparkAvroSaver.saveJavaRDD(organizations, AffMatchOrganization.SCHEMA$, params.outputAvroPath);
            SparkAvroSaver.saveJavaRDD(affiliations, AffMatchAffiliation.SCHEMA$, params.outputAvroAffPath);
            
        }
    }
        
    
    //------------------------ PRIVATE --------------------------
    
    
    @Parameters(separators = "=")
    private static class AffMatchingJobParameters {
        
        @Parameter(names = "-inputAvroOrgPath", required = true, description="path to directory with avro files containing organizations")
        private String inputAvroOrgPath;
        
        @Parameter(names = "-inputAvroAffPath", required = true, description="path to directory with avro files containing affiliations")
        private String inputAvroAffPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputAvroAffPath", required = true) // Added temporarily. When the job class code is finished, the results of the job will written as matched pairs of affId and orgId to outputAvroPath.  
        private String outputAvroAffPath;
        
    }
}
