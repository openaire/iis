package eu.dnetlib.iis.workflows.citationmatching.direct;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata;
import scala.Tuple2;

public class ExternalIdReferencePicker implements Serializable {

    private static final long serialVersionUID = 1L;

    
    public JavaPairRDD<String, Citation> pickReferences(JavaRDD<DocumentMetadata> output, String idType) {
        
        JavaPairRDD<String, Citation> pmidCitation = output
                .flatMapToPair(new PairFlatMapFunction<DocumentMetadata, String, Citation>() {

                    @Override
                    public Iterable<Tuple2<String, Citation>> call(DocumentMetadata t) throws Exception {
                        List<Tuple2<String, Citation>> workingCitations = Lists.newArrayList();
                        
                        for (ReferenceMetadata refMetadata : t.getReferences()) {
                            Citation wc = new Citation();
                            wc.setSourceDocumentId(t.getId());
                            wc.setPosition(refMetadata.getPosition());
                            
                            if (refMetadata.getExternalIds() == null || !refMetadata.getExternalIds().containsKey(idType)) {
                                continue;
                            }
                            
                            workingCitations.add(new Tuple2<String, Citation>((String)refMetadata.getExternalIds().get(idType), wc));
                        }
                        
                        return workingCitations;
                    }

                    
                });
        
        return pmidCitation;
    }
}
