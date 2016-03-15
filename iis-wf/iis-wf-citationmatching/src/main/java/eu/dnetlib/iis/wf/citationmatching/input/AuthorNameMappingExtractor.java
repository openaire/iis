package eu.dnetlib.iis.wf.citationmatching.input;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.Person;
import scala.Tuple2;

/**
 * Extractor of authorId to authorName mapping
 * 
 * @author madryk
 */
public class AuthorNameMappingExtractor {

    private static final String AUTHOR_NOT_FOUND_NAME_FALLBACK = "_UNDEFINED_";
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Extracts authorId to authorName mapping rdd grouped by documents.
     * If author couldn't be found in persons rdd, then algorithm will use {@literal _UNDEFINED_}
     * for authorName.
     * 
     * @param documents - pair rdd with documents (keys should contain documentIds; values should
     *      contain {@link DocumentMetadata} with authorIds in {@link BasicMetadata#getAuthors()})
     * @param persons - rdd with persons
     * @return rdd where key is documentId and value is a map with authorId to authorName mapping
     *      for that document. If document doesn't contain any author, then it won't be included
     *      in returned rdd
     */
    public JavaPairRDD<String, Map<String, String>> extractAuthorNameMapping(JavaPairRDD<String, DocumentMetadata> documents, JavaRDD<Person> persons) {
        
        JavaPairRDD<String, String> authorIdToDocumentIdMapping = extractAuthorIdToDocumentIdMapping(documents);
        
        
        JavaPairRDD<String, String> personIdToNameMapping = extractPersonIdToNameMapping(persons);
        
        
        JavaPairRDD<String, Map<String, String>> documentAuthors = 
                matchPersonNamesWithDocumentAuthors(authorIdToDocumentIdMapping, personIdToNameMapping);
        
        return documentAuthors;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private JavaPairRDD<String, String> extractAuthorIdToDocumentIdMapping(JavaPairRDD<String, DocumentMetadata> documents) {
        
        JavaPairRDD<String, String> authorIdDocumentIdMapping = documents
                .flatMapToPair(sourceMeta -> {
                    List<Tuple2<String, String>> l = Lists.newArrayList();
                    
                    for (CharSequence authorId : sourceMeta._2.getBasicMetadata().getAuthors()) {
                        l.add(new Tuple2<>(authorId.toString(), sourceMeta._2.getId().toString()));
                    }
                    return l;
                });
        
        return authorIdDocumentIdMapping;
    }
    
    
    private JavaPairRDD<String, String> extractPersonIdToNameMapping(JavaRDD<Person> persons) {
        return persons
                .keyBy(x -> x.getId().toString())
                .mapValues(p -> p.getFullname().toString());
    }
    
    
    private JavaPairRDD<String, Map<String, String>> matchPersonNamesWithDocumentAuthors(JavaPairRDD<String, String> authorIdToDocumentIdMapping, JavaPairRDD<String, String> personIdToNameMapping) {
        
        JavaPairRDD<String, Map<String, String>> documentAuthors = authorIdToDocumentIdMapping
                .leftOuterJoin(personIdToNameMapping)
                .mapToPair(x -> {
                    
                    String authorId = x._1;
                    String documentId = x._2._1;
                    String authorFullName = x._2._2.or(AUTHOR_NOT_FOUND_NAME_FALLBACK);
                    
                    return new Tuple2<>(documentId, new Tuple2<>(authorId, authorFullName));
                })
                .groupByKey()
                .mapValues(authorIdNameIterable -> {
                    Map<String, String> authorIdNameMapping = Maps.newHashMap();
                    
                    authorIdNameIterable.forEach(authorIdName -> authorIdNameMapping.put(authorIdName._1, authorIdName._2));
                    
                    return authorIdNameMapping;
                });
        
        return documentAuthors;
        
    }
}
