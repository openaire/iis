package eu.dnetlib.iis.wf.citationmatching.converter;

import java.util.stream.Collectors;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;

/**
 * Util class for extracting data from {@link BasicMetadata}
 * 
 * @author madryk
 */
class BasicMetadataDataExtractionUtil {


    //------------------------ CONSTRUCTORS --------------------------

    private BasicMetadataDataExtractionUtil() { }


    //------------------------ LOGIC --------------------------

    public static String extractAuthors(BasicMetadata metadata) {
        return metadata.getAuthors().stream()
                .map(x -> x.toString())
                .collect(Collectors.joining(", "));
    }

    public static String extractJournal(BasicMetadata metadata) {
        return convertToString(metadata.getJournal());
    }

    public static String extractPages(BasicMetadata metadata) {
        return convertToString(metadata.getPages());
    }

    public static String extractTitle(BasicMetadata metadata) {
        return convertToString(metadata.getTitle());
    }

    public static String extractYear(BasicMetadata metadata) {
        return convertToString(metadata.getYear());
    }


    //------------------------ PRIVATE --------------------------

    private static String convertToString(CharSequence charSequence) {
        return (charSequence != null) ? charSequence.toString() : null;
    }
}
