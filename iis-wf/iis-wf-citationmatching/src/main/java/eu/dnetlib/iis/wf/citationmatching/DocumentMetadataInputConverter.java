package eu.dnetlib.iis.wf.citationmatching;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.citationmatching.converter.DocumentMetadataToMatchableConverter;
import pl.edu.icm.coansys.citations.InputDocumentConverter;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * Converter of {@link DocumentMetadata} rdd to {@link MatchableEntity} rdd
 * 
 * @author madryk
 */
public class DocumentMetadataInputConverter implements InputDocumentConverter<String, DocumentMetadata>, Serializable {

    private static final long serialVersionUID = 1L;

    private DocumentMetadataToMatchableConverter converter = new DocumentMetadataToMatchableConverter();


    //------------------------ LOGIC --------------------------

    /**
     * Converts rdd with documents of type {@link DocumentMetadata}
     * to rdd with documents of type {@link MatchableEntity}.
     * Method assumes that keys of input rdd will contain document id.
     * Keys in returned rdd will be unchanged.
     */
    @Override
    public JavaPairRDD<String, MatchableEntity> convertDocuments(JavaPairRDD<String, DocumentMetadata> inputDocuments) {

        JavaPairRDD<String, MatchableEntity> documentEntities = inputDocuments
                .mapToPair(document -> new Tuple2<>(document._1, converter.convertToMatchableEntity(document._1, document._2)));

        return documentEntities;
    }


    //------------------------ SETTERS --------------------------

    public void setConverter(DocumentMetadataToMatchableConverter converter) {
        this.converter = converter;
    }

}
