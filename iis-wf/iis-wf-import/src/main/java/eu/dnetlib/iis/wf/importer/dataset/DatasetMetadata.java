package eu.dnetlib.iis.wf.importer.dataset;

import java.util.List;
import java.util.Map;

/**
 * Record grouping raw dataset metadata.
 * 
 * @author mhorst
 *
 */
public class DatasetMetadata {

    String headerId;
    String idType;
    String idValue;
    List<CharSequence> creatorNames;
    List<CharSequence> titles;
    List<CharSequence> formats;
    Map<CharSequence, CharSequence> alternateIdentifiers;
    String description;
    String publisher;
    String publicationYear;
    String resourceTypeClass;
    String resourceTypeValue;
    String currentAlternateIdentifierType;
    
}
