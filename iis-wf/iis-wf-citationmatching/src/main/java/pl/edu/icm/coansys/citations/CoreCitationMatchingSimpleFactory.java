package pl.edu.icm.coansys.citations;

import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import pl.edu.icm.coansys.citations.hashers.CitationNameYearHashGenerator;
import pl.edu.icm.coansys.citations.hashers.CitationNameYearPagesHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearNumNumHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearPagesHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearStrictHashGenerator;
import pl.edu.icm.coansys.citations.hashers.HashGenerator;


/**
 * Factory of {@link CoreCitationMatchingService} service
 * 
 * @author madryk
 */
public class CoreCitationMatchingSimpleFactory {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Creates and returns {@link CoreCitationMatchingService}.
     */
    public CoreCitationMatchingService createCoreCitationMatchingService(JavaSparkContext sc, long maxHashBucketSize) {
        
        CoreCitationMatchingService coreCitationMatchingService = new CoreCitationMatchingService();
        coreCitationMatchingService.setSparkContext(sc);
        coreCitationMatchingService.setMaxHashBucketSize(maxHashBucketSize);
        coreCitationMatchingService.setMatchableEntityHashers(createMatchableEntityHashers());
        
        return coreCitationMatchingService;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private List<Pair<MatchableEntityHasher, MatchableEntityHasher>> createMatchableEntityHashers() {
        List<Pair<MatchableEntityHasher, MatchableEntityHasher>> matchableEntityHashers = Lists.newArrayList();
        
        for (Pair<HashGenerator, HashGenerator> citAndDocHashGenerators : createHashGenerators()) {
            
            MatchableEntityHasher citationHasher = createMatchableEntityHasher(citAndDocHashGenerators.getLeft());
            MatchableEntityHasher documentHasher = createMatchableEntityHasher(citAndDocHashGenerators.getRight());
            
            matchableEntityHashers.add(new ImmutablePair<>(citationHasher, documentHasher));
        }
        
        
        return matchableEntityHashers;
    }
    
    private List<Pair<HashGenerator, HashGenerator>> createHashGenerators() {
        List<Pair<HashGenerator, HashGenerator>> hashGenerators = Lists.newArrayList();
        
        hashGenerators.add(new ImmutablePair<>(new CitationNameYearPagesHashGenerator(), new DocumentNameYearPagesHashGenerator()));
        hashGenerators.add(new ImmutablePair<>(new CitationNameYearPagesHashGenerator(), new DocumentNameYearNumNumHashGenerator()));
        hashGenerators.add(new ImmutablePair<>(new CitationNameYearHashGenerator(), new DocumentNameYearStrictHashGenerator()));
        hashGenerators.add(new ImmutablePair<>(new CitationNameYearHashGenerator(), new DocumentNameYearHashGenerator()));
        
        return hashGenerators;
    }
    
    
    private MatchableEntityHasher createMatchableEntityHasher(HashGenerator hashGenerator) {
        MatchableEntityHasher matchableEntityHasher = new MatchableEntityHasher();
        matchableEntityHasher.setHashGenerator(hashGenerator);
        return matchableEntityHasher;
    }
    
}
