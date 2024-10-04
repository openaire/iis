package eu.dnetlib.iis.wf.citationmatching;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.wf.citationmatching.converter.ReferenceMetadataToMatchableConverter;
import pl.edu.icm.coansys.citations.InputCitationConverter;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * Converter of {@link ReferenceMetadata} rdd to {@link MatchableEntity} rdd
 * 
 * @author madryk
 */
public class ReferenceMetadataInputConverter implements InputCitationConverter<String, ReferenceMetadata>, Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger log = Logger.getLogger(ReferenceMetadataInputConverter.class);

    private final static int MAX_CITATION_LENGTH = 10000;


    private ReferenceMetadataToMatchableConverter converter = new ReferenceMetadataToMatchableConverter();


    //------------------------ LOGIC --------------------------

    /**
     * Converts rdd with citations of type {@link ReferenceMetadata}
     * to rdd with citations of type {@link MatchableEntity}.
     * Method assumes that keys of input rdd will contain citation id.
     * Keys in returned rdd will be unchanged.
     */
    @Override
    public JavaPairRDD<String, MatchableEntity> convertCitations(JavaPairRDD<String, ReferenceMetadata> inputCitations) {

        JavaPairRDD<String, MatchableEntity> citationEntities = inputCitations
                .flatMapToPair(inputCitation -> {
                    List<Tuple2<String, MatchableEntity>> list = Lists.newArrayList();
                    
                    MatchableEntity entity = converter.convertToMatchableEntity(inputCitation._1, inputCitation._2);
                    
                    if (entity.rawText().get().length() > MAX_CITATION_LENGTH) {
                        log.error("RawText of citation " + inputCitation._1 + " exceeds length limit (" + MAX_CITATION_LENGTH + ").");
                        return list.iterator();
                    }
                    
                    list.add(new Tuple2<String, MatchableEntity>(inputCitation._1, entity));
                    
                    return list.iterator();
                });

        return citationEntities;
    }


    //------------------------ SETTERS --------------------------

    public void setConverter(ReferenceMetadataToMatchableConverter converter) {
        this.converter = converter;
    }

}
