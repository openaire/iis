package eu.dnetlib.iis.wf.metadataextraction.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.wf.metadataextraction.grobid.GrobidClient;
import eu.dnetlib.iis.wf.metadataextraction.grobid.GrobidTeiBiblStructParser;

/**
 * {@link ReferenceTextParser} implementation relying on an external Grobid
 * REST server ({@code /api/processCitation}).
 *
 * @author mhorst
 */
public class GrobidReferenceTextParser implements ReferenceTextParser {

    private final String grobidServerUrl;

    private final int connectionTimeout;

    private final int readTimeout;

    private transient GrobidClient grobidClient;

    /**
     * @param grobidServerUrl Grobid server location
     * @param connectionTimeout connection timeout in ms
     * @param readTimeout read timeout in ms
     */
    public GrobidReferenceTextParser(String grobidServerUrl, int connectionTimeout, int readTimeout) {
        this.grobidServerUrl = grobidServerUrl;
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
    }

    @Override
    public ParsedReference parse(String text) throws Exception {
        if (StringUtils.isBlank(text)) {
            return null;
        }
        String teiXml = getGrobidClient().processCitation(text);
        if (StringUtils.isBlank(teiXml)) {
            return null;
        }
        return GrobidTeiBiblStructParser.parse(teiXml);
    }

    @Override
    public List<ParsedReference> parse(List<String> texts) throws Exception {
        if (texts == null || texts.isEmpty()) {
            return Collections.emptyList();
        }

        // Keep the original alignment: blank texts yield null entries.
        List<String> toParse = new ArrayList<>(texts.size());
        for (String text : texts) {
            toParse.add(StringUtils.isBlank(text) ? null : text);
        }
        List<String> nonBlank = new ArrayList<>(toParse.size());
        for (String text : toParse) {
            if (text != null) {
                nonBlank.add(text);
            }
        }

        List<ParsedReference> result = new ArrayList<>(Collections.nCopies(toParse.size(), (ParsedReference) null));

        if (!nonBlank.isEmpty()) {
            String teiXml = getGrobidClient().processCitationList(nonBlank);
            if (StringUtils.isNotBlank(teiXml)) {
                List<ParsedReference> parsedList = GrobidTeiBiblStructParser.parseList(teiXml);
                int parsedIdx = 0;
                for (int i = 0; i < toParse.size(); i++) {
                    if (toParse.get(i) == null) {
                        continue;
                    }
                    if (parsedIdx < parsedList.size()) {
                        result.set(i, parsedList.get(parsedIdx));
                    }
                    parsedIdx++;
                }
            }
        }
        return result;
    }

    /**
     * Returns the Grobid client, initializing it lazily on first use.
     */
    private GrobidClient getGrobidClient() {
        if (grobidClient == null) {
            // throttleSleepTime is the base for the exponential retry backoff
            grobidClient = new GrobidClient(grobidServerUrl, connectionTimeout, readTimeout, 2000L, 10);
        }
        return grobidClient;
    }
}
