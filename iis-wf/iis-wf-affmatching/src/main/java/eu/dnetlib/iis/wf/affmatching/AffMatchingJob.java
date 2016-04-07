package eu.dnetlib.iis.wf.affmatching;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.importer.schemas.Organization;
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
    
    private static AffMatchOrganizationConverter converter = new AffMatchOrganizationConverter();
    
    
    
    
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
            
            JavaRDD<AffMatchOrganization> organizations = sourceOrganizations.map(srcOrg -> converter.convert(srcOrg));
            
            
            // TODO: READ DOCUMENT AFFILIATIONS
            
            // TODO: ACTUAL MATCHING
            
            
            SparkAvroSaver.saveJavaRDD(organizations, AffMatchOrganization.SCHEMA$, params.outputAvroPath);
        
        }
    }
        
    
    //------------------------ PRIVATE --------------------------
    
    
    @Parameters(separators = "=")
    private static class AffMatchingJobParameters {
        
        @Parameter(names = "-inputAvroOrgPath", required = true, description="path to directory with avro files containing organizations")
        private String inputAvroOrgPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        
    }
}
