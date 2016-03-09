package eu.dnetlib.iis.wf.citationmatching.converter;

import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractAuthors;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractJournal;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractPages;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractTitle;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractYear;

import java.io.Serializable;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Converter of {@link DocumentMetadata} object to {@link MatchableEntity} object
 * 
 * @author madryk
 */
public class DocumentMetadataToMatchableConverter implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------

    /**
     * Converts {@link DocumentMetadata} to {@link MatchableEntity}.
     * As {@link MatchableEntity#id()} it uses documentId parameter.
     */
    public MatchableEntity convertToMatchableEntity(String documentId, DocumentMetadata docMetadata) {

        BasicMetadata metadata = docMetadata.getBasicMetadata();

        MatchableEntity entity = MatchableEntity.fromParameters(documentId, 
                extractAuthors(metadata), extractJournal(metadata), extractTitle(metadata), 
                extractPages(metadata), extractYear(metadata), null);

        return entity;
    }
}
