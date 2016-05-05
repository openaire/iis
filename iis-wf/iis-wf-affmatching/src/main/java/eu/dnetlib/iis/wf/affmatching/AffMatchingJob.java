package eu.dnetlib.iis.wf.affmatching;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import datafu.com.google.common.collect.Lists;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchComputer;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameCountryStrictMatchVoter;
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
    
    private static AffMatchingService affMatchingService = createAffMatchingService();


    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AffMatchingJobParameters params = new AffMatchingJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
          
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
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
    }
    
    
    
    private static AffMatchingService createAffMatchingService() {
        
        AffMatchingService affMatchingService = new AffMatchingService();
        
        
        // readers
        
        affMatchingService.setAffiliationReader(new IisAffiliationReader());
        affMatchingService.setOrganizationReader(new IisOrganizationReader());
        
        
        // writer
        
        affMatchingService.setAffMatchResultWriter(new IisAffMatchResultWriter());
        
        
        // affOrgHashBucketMatcher
        
        AffOrgJoiner affOrgHashBucketJoiner = new AffOrgHashBucketJoiner();
        
        AffOrgMatchComputer affOrgHashMatchComputer = new AffOrgMatchComputer();
        affOrgHashMatchComputer.setAffOrgMatchVoters(Lists.newArrayList(new NameCountryStrictMatchVoter()));
        
        AffOrgMatcher affOrgHashBucketMatcher = new AffOrgMatcher();
        affOrgHashBucketMatcher.setAffOrgJoiner(affOrgHashBucketJoiner);
        affOrgHashBucketMatcher.setAffOrgMatchComputer(affOrgHashMatchComputer);
        
        
        
        
        affMatchingService.setAffOrgMatchers(Lists.newArrayList(affOrgHashBucketMatcher));
        
        return affMatchingService;
    }
    
    
}
