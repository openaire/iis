package eu.dnetlib.iis.wf.metadataextraction.parser;

import org.apache.commons.lang3.StringUtils;

import pl.edu.icm.cermine.bibref.CRFBibReferenceParser;
import pl.edu.icm.cermine.bibref.model.BibEntry;
import pl.edu.icm.cermine.bibref.model.BibEntryFieldType;
import pl.edu.icm.cermine.exception.AnalysisException;

/**
 * {@link ReferenceTextParser} implementation relying on the CERMINE CRF-based
 * {@link CRFBibReferenceParser}.
 *
 * @author mhorst
 */
public class CermineReferenceTextParser implements ReferenceTextParser {

    private transient CRFBibReferenceParser referenceParser;

    @Override
    public ParsedReference parse(String text) throws AnalysisException {
        if (StringUtils.isBlank(text)) {
            return null;
        }
        BibEntry bibEntry = getReferenceParser().parseBibReference(text);
        if (bibEntry == null) {
            return null;
        }
        ParsedReference parsed = new ParsedReference();
        parsed.setTitle(bibEntry.getFirstFieldValue(BibEntryFieldType.TITLE));
        parsed.setAuthors(bibEntry.getAllFieldValues(BibEntryFieldType.AUTHOR));
        parsed.setPages(bibEntry.getFirstFieldValue(BibEntryFieldType.PAGES));
        parsed.setJournal(bibEntry.getFirstFieldValue(BibEntryFieldType.JOURNAL));
        parsed.setVolume(bibEntry.getFirstFieldValue(BibEntryFieldType.VOLUME));
        parsed.setYear(bibEntry.getFirstFieldValue(BibEntryFieldType.YEAR));
        parsed.setEdition(bibEntry.getFirstFieldValue(BibEntryFieldType.EDITION));
        parsed.setPublisher(bibEntry.getFirstFieldValue(BibEntryFieldType.PUBLISHER));
        parsed.setLocation(bibEntry.getFirstFieldValue(BibEntryFieldType.LOCATION));
        parsed.setSeries(bibEntry.getFirstFieldValue(BibEntryFieldType.SERIES));
        parsed.setIssue(bibEntry.getFirstFieldValue(BibEntryFieldType.NUMBER));
        parsed.setUrl(bibEntry.getFirstFieldValue(BibEntryFieldType.URL));
        parsed.setDoi(bibEntry.getFirstFieldValue(BibEntryFieldType.DOI));
        parsed.setIsbn(bibEntry.getFirstFieldValue(BibEntryFieldType.ISBN));
        parsed.setIssn(bibEntry.getFirstFieldValue(BibEntryFieldType.ISSN));
        return parsed;
    }

    /**
     * Returns the reference parser, initializing it lazily on first use.
     */
    private CRFBibReferenceParser getReferenceParser() {
        if (referenceParser == null) {
            try {
                referenceParser = CRFBibReferenceParser.getInstance();
            } catch (AnalysisException e) {
                throw new RuntimeException("Unable to initialize CRF BibReference parser", e);
            }
        }
        return referenceParser;
    }
}
