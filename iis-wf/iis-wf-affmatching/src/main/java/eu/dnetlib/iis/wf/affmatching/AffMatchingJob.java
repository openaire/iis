package eu.dnetlib.iis.wf.affmatching;

import static com.google.common.collect.ImmutableList.of;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createFirstWordsHashBucketMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createDocOrgRelationMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createMainSectionHashBucketMatcher;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.IisAffMatchResultWriter;

/**
 * Job matching affiliations with organizations.
 * 
 * @param inputAvroOrgPath path to directory with avro files containing organizations 
 * 
 * @author Łukasz Dumiszewski
 */

public class AffMatchingJob {
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AffMatchingJobParameters params = new AffMatchingJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
          
            AffMatchingService affMatchingService = createAffMatchingService(sc, params);
            
            affMatchingService.matchAffiliations(sc, params.inputAvroAffPath, params.inputAvroOrgPath, params.outputAvroPath);
            
        }
    }
        
    
    //------------------------ PRIVATE --------------------------
    
    
    @Parameters(separators = "=")
    private static class AffMatchingJobParameters {
        
        @Parameter(names = "-inputAvroOrgPath", required = true, description="path to directory with avro files containing organizations")
        private String inputAvroOrgPath;
        
        @Parameter(names = "-inputAvroAffPath", required = true, description="path to directory with avro files containing affiliations")
        private String inputAvroAffPath;
        
        @Parameter(names = "-inputAvroDocProjPath", required = true, description="path to directory with avro files containing document to project relations")
        private String inputAvroDocProjPath;
        
        @Parameter(names = "-inputDocProjConfidenceThreshold", required = false, description="minimal confidence level for document to project relations (no limit by default)")
        private Float inputDocProjConfidenceThreshold = null;
        
        @Parameter(names = "-inputAvroProjOrgPath", required = true, description="path to directory with avro files containing project to organization relations")
        private String inputAvroProjOrgPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
    }
    
    
    
    private static AffMatchingService createAffMatchingService(JavaSparkContext sparkContext, AffMatchingJobParameters params) {
        
        AffMatchingService affMatchingService = new AffMatchingService();
        
        
        // readers
        
        affMatchingService.setAffiliationReader(new IisAffiliationReader());
        affMatchingService.setOrganizationReader(new IisOrganizationReader());
        
        
        // writer
        
        affMatchingService.setAffMatchResultWriter(new IisAffMatchResultWriter());
        
        
        // matchers
        
        AffOrgMatcher docOrgRelationMatcher = 
                createDocOrgRelationMatcher(sparkContext, params.inputAvroDocProjPath, params.inputAvroProjOrgPath, params.inputDocProjConfidenceThreshold);
        
        AffOrgMatcher mainSectionHashBucketMatcher = createMainSectionHashBucketMatcher();
        
        AffOrgMatcher firstWordsHashBucketMatcher = createFirstWordsHashBucketMatcher();
        
        
        
        
        affMatchingService.setAffOrgMatchers(of(docOrgRelationMatcher, mainSectionHashBucketMatcher, firstWordsHashBucketMatcher));
        
        return affMatchingService;
    }
    
    
}
