package eu.dnetlib.iis.wf.metadataextraction.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.metadataextraction.schemas.Range;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;

/**
 * Fills {@link ReferenceBasicMetadata} fields parsed from a raw reference text,
 * but only for the fields which have not already been set from an explicit
 * JSON record field mapping.
 *
 * @author mhorst
 */
public final class ParsedReferenceFiller {

    private ParsedReferenceFiller() {
    }

    /**
     * Applies fields parsed from the raw unstructured text,
     * but only for fields that have NOT already been set.
     */
    public static void applyParsedFields(ReferenceBasicMetadata.Builder basicBuilder, ParsedReference parsed) {

        // title - only if not already set
        if (basicBuilder.getTitle() == null) {
            String parsedTitle = parsed.getTitle();
            if (StringUtils.isNotBlank(parsedTitle)) {
                basicBuilder.setTitle(parsedTitle);
            }
        }

        // authors - never explicitly set from JSON mapping
        List<CharSequence> authors = new ArrayList<>();
        List<String> parsedAuthors = parsed.getAuthors();
        if (parsedAuthors != null) {
            for (String author : parsedAuthors) {
                if (StringUtils.isNotBlank(author)) {
                    authors.add(author);
                }
            }
        }
        if (!authors.isEmpty()) {
            basicBuilder.setAuthors(authors);
        }

        // pages - only if not already set
        if (basicBuilder.getPages() == null) {
            String parsedPages = parsed.getPages();
            if (StringUtils.isNotBlank(parsedPages)) {
                Range pagesRange = parsePagesRange(parsedPages);
                if (pagesRange != null) {
                    basicBuilder.setPages(pagesRange);
                }
            }
        }

        // journal - only if not already set
        if (basicBuilder.getJournal() == null) {
            String parsedJournal = parsed.getJournal();
            if (StringUtils.isNotBlank(parsedJournal)) {
                basicBuilder.setJournal(parsedJournal);
            }
        }

        // source - never explicitly set from JSON mapping
        String parsedSource = parsed.getJournal();
        if (StringUtils.isBlank(parsedSource)) {
            parsedSource = parsed.getTitle();
        }
        if (StringUtils.isNotBlank(parsedSource) && basicBuilder.getSource() == null) {
            basicBuilder.setSource(parsedSource);
        }

        // volume - only if not already set
        if (basicBuilder.getVolume() == null) {
            String parsedVolume = parsed.getVolume();
            if (StringUtils.isNotBlank(parsedVolume)) {
                basicBuilder.setVolume(parsedVolume);
            }
        }

        // year - only if not already set
        if (basicBuilder.getYear() == null) {
            String parsedYear = parsed.getYear();
            if (StringUtils.isNotBlank(parsedYear)) {
                basicBuilder.setYear(parsedYear);
            }
        }

        // edition - only if not already set
        if (basicBuilder.getEdition() == null) {
            String parsedEdition = parsed.getEdition();
            if (StringUtils.isNotBlank(parsedEdition)) {
                basicBuilder.setEdition(parsedEdition);
            }
        }

        // publisher - never explicitly set from JSON mapping
        String parsedPublisher = parsed.getPublisher();
        if (StringUtils.isNotBlank(parsedPublisher)) {
            basicBuilder.setPublisher(parsedPublisher);
        }

        // location - never explicitly set from JSON mapping
        String parsedLocation = parsed.getLocation();
        if (StringUtils.isNotBlank(parsedLocation)) {
            basicBuilder.setLocation(parsedLocation);
        }

        // series - only if not already set
        if (basicBuilder.getSeries() == null) {
            String parsedSeries = parsed.getSeries();
            if (StringUtils.isNotBlank(parsedSeries)) {
                basicBuilder.setSeries(parsedSeries);
            }
        }

        // issue - only if not already set
        if (basicBuilder.getIssue() == null) {
            String parsedIssue = parsed.getIssue();
            if (StringUtils.isNotBlank(parsedIssue)) {
                basicBuilder.setIssue(parsedIssue);
            }
        }

        // url - never explicitly set from JSON mapping
        String parsedUrl = parsed.getUrl();
        if (StringUtils.isNotBlank(parsedUrl)) {
            basicBuilder.setUrl(parsedUrl);
        }

        // externalIds - only fill in identifiers not already mapped from JSON
        Map<CharSequence, CharSequence> existingExtIds = basicBuilder.getExternalIds();
        if (existingExtIds == null) {
            existingExtIds = new HashMap<>();
        }

        String parsedDoi = parsed.getDoi();
        if (StringUtils.isNotBlank(parsedDoi) && !existingExtIds.containsKey("doi")) {
            existingExtIds.put("doi", parsedDoi);
        }

        String parsedIsbn = parsed.getIsbn();
        if (StringUtils.isNotBlank(parsedIsbn) && !existingExtIds.containsKey("ISBN")) {
            existingExtIds.put("ISBN", parsedIsbn);
        }

        String parsedIssn = parsed.getIssn();
        if (StringUtils.isNotBlank(parsedIssn) && !existingExtIds.containsKey("ISSN")) {
            existingExtIds.put("ISSN", parsedIssn);
        }

        if (!existingExtIds.isEmpty()) {
            basicBuilder.setExternalIds(existingExtIds);
        }
    }

    /**
     * Parses a page range string (e.g. "149-187" or "149") into a {@link Range} object.
     */
    private static Range parsePagesRange(String pagesStr) {
        if (StringUtils.isBlank(pagesStr)) {
            return null;
        }
        String trimmed = pagesStr.trim();
        String[] parts = trimmed.split("[-–—]+");
        if (parts.length == 1) {
            String single = parts[0].trim();
            if (StringUtils.isNotBlank(single)) {
                return Range.newBuilder().setStart(single).build();
            }
        } else if (parts.length >= 2) {
            String start = parts[0].trim();
            String end = parts[parts.length - 1].trim();
            if (StringUtils.isNotBlank(start)) {
                Range.Builder rangeBuilder = Range.newBuilder().setStart(start);
                if (StringUtils.isNotBlank(end)) {
                    rangeBuilder.setEnd(end);
                }
                return rangeBuilder.build();
            }
        }
        return null;
    }
}
