package eu.dnetlib.iis.wf.referenceextraction.patent.parser;

import java.io.Serializable;

import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;

/**
 * Patent metadata parser.
 * 
 * @author mhorst
 *
 */
public interface PatentMetadataParser extends Serializable {

    /**
     * Parses XML file and produces {@link Patent.Builder} object by supplementing
     * the object provided as parameter.
     */
    Patent.Builder parse(CharSequence source, Patent.Builder patentBuilder) throws PatentMetadataParserException;
}
