package eu.dnetlib.iis.wf.citationmatching;

import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.wf.citationmatching.converter.MatchedCitationToCitationConverter;
import pl.edu.icm.coansys.citations.OutputConverter;
import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * Converter of {@link MatchableEntity} and {@link IdWithSimilarity} pair rdd
 * to {@link Citation} rdd
 * 
 * @author madryk
 */
public class CitationOutputConverter implements OutputConverter<Citation, NullWritable>, Serializable {

    private static final long serialVersionUID = 1L;

    private MatchedCitationToCitationConverter converter = new MatchedCitationToCitationConverter();


    //------------------------ LOGIC --------------------------

    /**
     * Converts rdd with matched citations to rdd with {@link Citation}s.
     */
    @Override
    public JavaPairRDD<Citation, NullWritable> convertMatchedCitations(
            JavaPairRDD<MatchableEntity, IdWithSimilarity> matchedCitations) {

        JavaPairRDD<Citation, NullWritable> convertedMatchedCitations = matchedCitations
                .mapToPair(x -> new Tuple2<>(converter.convertToCitation(x._1, x._2), NullWritable.get()));

        return convertedMatchedCitations;
    }


    //------------------------ SETTERS --------------------------

    public void setConverter(MatchedCitationToCitationConverter converter) {
        this.converter = converter;
    }

}
