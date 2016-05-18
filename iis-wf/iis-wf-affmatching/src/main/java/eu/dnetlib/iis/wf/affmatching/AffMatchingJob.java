package eu.dnetlib.iis.wf.affmatching;

import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedShortNameStrictCountryLooseMatchVoter;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffiliationMainSectionBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.DocOrgRelationAffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationMainSectionBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationCombiner;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationFetcher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisDocumentProjectReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisProjectOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchComputer;
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
        
        AffMatchingService affMatchingService = createAffMatchingService(params);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
          
            affMatchingService.matchAffiliations(sc, params.inputAvroAffPath, params.inputAvroOrgPath, 
                    params.inputAvroDocProjPath, params.inputAvroProjOrgPath, params.outputAvroPath);
            
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
    
    
    
    private static AffMatchingService createAffMatchingService(AffMatchingJobParameters params) {
        
        AffMatchingService affMatchingService = new AffMatchingService();
        
        
        // readers
        
        affMatchingService.setAffiliationReader(new IisAffiliationReader());
        affMatchingService.setOrganizationReader(new IisOrganizationReader());
        
        DocumentOrganizationFetcher documentOrganizationFetcher = new DocumentOrganizationFetcher();
        documentOrganizationFetcher.setDocumentProjectReader(new IisDocumentProjectReader());
        documentOrganizationFetcher.setProjectOrganizationReader(new IisProjectOrganizationReader());
        documentOrganizationFetcher.setDocumentOrganizationCombiner(new DocumentOrganizationCombiner());
        documentOrganizationFetcher.setDocProjConfidenceLevelThreshold(params.inputDocProjConfidenceThreshold);
        
        affMatchingService.setDocumentOrganizationFetcher(documentOrganizationFetcher);
        
        
        // writer
        
        affMatchingService.setAffMatchResultWriter(new IisAffMatchResultWriter());
        
        
        // docOrgRelationAffOrgMatcher
        
        DocOrgRelationAffOrgJoiner docOrgRelationAffOrgJoiner = new DocOrgRelationAffOrgJoiner();
        
        AffOrgMatchComputer docOrgRelationAffOrgMatchComputer = new AffOrgMatchComputer();
        docOrgRelationAffOrgMatchComputer.setAffOrgMatchVoters(ImmutableList.of(
                createNameCountryStrictMatchVoter(),
                createNameStrictCountryLooseMatchVoter(),
                createSectionedNameStrictCountryLooseMatchVoter(),
                createSectionedNameLevenshteinCountryLooseMatchVoter(),
                createSectionedShortNameStrictCountryLooseMatchVoter()));
        
        AffOrgMatcher docOrgRelationAffOrgMatcher = new AffOrgMatcher();
        docOrgRelationAffOrgMatcher.setAffOrgJoiner(docOrgRelationAffOrgJoiner);
        docOrgRelationAffOrgMatcher.setAffOrgMatchComputer(docOrgRelationAffOrgMatchComputer);
        
        
        // affOrgMainSectionHashBucketMatcher
        
        AffOrgHashBucketJoiner mainSectionHashBucketJoiner = new AffOrgHashBucketJoiner();
        
        mainSectionHashBucketJoiner.setAffiliationBucketHasher(new AffiliationMainSectionBucketHasher());
        mainSectionHashBucketJoiner.setOrganizationBucketHasher(new OrganizationMainSectionBucketHasher());
        
        AffOrgMatchComputer mainSectionHashMatchComputer = new AffOrgMatchComputer();
        
        mainSectionHashMatchComputer.setAffOrgMatchVoters(ImmutableList.of(
                createNameCountryStrictMatchVoter(),
                createNameStrictCountryLooseMatchVoter(),
                createSectionedNameStrictCountryLooseMatchVoter(),
                createSectionedNameLevenshteinCountryLooseMatchVoter(),
                createSectionedShortNameStrictCountryLooseMatchVoter()));
        
        AffOrgMatcher mainSectionHashBucketMatcher = new AffOrgMatcher();
        mainSectionHashBucketMatcher.setAffOrgJoiner(mainSectionHashBucketJoiner);
        mainSectionHashBucketMatcher.setAffOrgMatchComputer(mainSectionHashMatchComputer);
        
        
        // affOrgFirstWordsHashBucketMatcher
        
        AffOrgJoiner firstWordsHashBucketJoiner = new AffOrgHashBucketJoiner();
        
        AffOrgMatchComputer firstWordsHashMatchComputer = new AffOrgMatchComputer();
        
        firstWordsHashMatchComputer.setAffOrgMatchVoters(ImmutableList.of(
                createNameCountryStrictMatchVoter(),
                createNameStrictCountryLooseMatchVoter(),
                createSectionedNameStrictCountryLooseMatchVoter(),
                createSectionedNameLevenshteinCountryLooseMatchVoter(),
                createSectionedShortNameStrictCountryLooseMatchVoter()));
        
        AffOrgMatcher firstWordsHashBucketMatcher = new AffOrgMatcher();
        firstWordsHashBucketMatcher.setAffOrgJoiner(firstWordsHashBucketJoiner);
        firstWordsHashBucketMatcher.setAffOrgMatchComputer(firstWordsHashMatchComputer);
        
        
        
        
        affMatchingService.setAffOrgMatchers(ImmutableList
                .of(docOrgRelationAffOrgMatcher, mainSectionHashBucketMatcher, firstWordsHashBucketMatcher));
        
        return affMatchingService;
    }
    
    
}
