package eu.dnetlib.iis.wf.citationmatching.input;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;

/**
 * Attacher of author names into documents rdd
 * 
 * @author madryk
 */
public class AuthorNameAttacher {

    //------------------------ LOGIC --------------------------
    
    /**
     * Attaches author names to documents rdd
     * 
     * @param documents - pair rdd with documents (keys should contain documentIds; values should
     *      contain {@link DocumentMetadata} with authorIds in {@link BasicMetadata#getAuthors()})
     * @param documentAuthors - authorId to authorName mapping rdd grouped by documents
     * @return documents rdd with authorNames instead of authorIds in {@link BasicMetadata#getAuthors()}
     */
    public JavaPairRDD<String, DocumentMetadata> attachAuthorNames(JavaPairRDD<String, DocumentMetadata> documents, JavaPairRDD<String, Map<String, String>> documentAuthors) {
        
        JavaPairRDD<String, DocumentMetadata> outputMeta = documents
                .leftOuterJoin(documentAuthors)
                .mapValues(x -> {
                    
                    List<CharSequence> authorIds = x._1.getBasicMetadata().getAuthors();
                    List<CharSequence> authors = Lists.newArrayList();
                    
                    for (CharSequence authorId : authorIds) {
                        authors.add(x._2.get().get(authorId.toString()));
                    }
                    
                    DocumentMetadata newMeta = DocumentMetadata.newBuilder(x._1).build();
                    newMeta.getBasicMetadata().setAuthors(authors);
                    
                    return newMeta;
                });
        
        return outputMeta;
    }
}
