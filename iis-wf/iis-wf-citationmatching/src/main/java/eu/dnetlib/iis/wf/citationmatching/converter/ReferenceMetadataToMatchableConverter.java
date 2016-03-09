package eu.dnetlib.iis.wf.citationmatching.converter;

import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractAuthors;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractJournal;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractPages;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractTitle;
import static eu.dnetlib.iis.wf.citationmatching.converter.BasicMetadataDataExtractionUtil.extractYear;

import java.io.Serializable;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Converter of {@link ReferenceMetadata} object to {@link MatchableEntity} object
 * 
 * @author madryk
 */
public class ReferenceMetadataToMatchableConverter implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------

    /**
     * Converts {@link ReferenceMetadata} to {@link MatchableEntity}.
     * As {@link MatchableEntity#id()} it uses citationId parameter.
     */
    public MatchableEntity convertToMatchableEntity(String citationId, ReferenceMetadata refMetadata) {

        BasicMetadata metadata = refMetadata.getBasicMetadata();

        String authors = extractAuthors(metadata);
        String journal = extractJournal(metadata);
        String pages = extractPages(metadata);
        String title = extractTitle(metadata);
        String year = extractYear(metadata);

        String rawText = (refMetadata.getRawText() != null)
                ? refMetadata.getRawText().toString()
                : (authors + ": " + title + ". " + journal + " (" + year + ") " + pages);

        MatchableEntity entity = MatchableEntity.fromParameters(citationId, 
                authors, journal, title, pages, year, rawText);


        return entity;
    }
}
