package eu.dnetlib.iis.workflows.citationmatching.direct;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import scala.Tuple2;


public class IdentifierMappingExtractor implements Serializable {

    private static final long serialVersionUID = 1L;

    
    public JavaPairRDD<String, String> extractIdMapping(JavaRDD<DocumentMetadata> output, String idType, Function<Iterable<DocumentMetadata>, DocumentMetadata> pickSingle) {
        
        JavaPairRDD<String, String> doiToId = output
                .filter(x -> x.getExternalIdentifiers() != null && x.getExternalIdentifiers().containsKey(idType))
                .mapToPair(x -> new Tuple2<String, DocumentMetadata>((String)x.getExternalIdentifiers().get(idType), x))
                .groupByKey()
                .mapValues(pickSingle)
                .filter(x -> x._2 != null)
                .mapValues(x -> (String)x.getId());
        
        return doiToId;
        
    }
}
