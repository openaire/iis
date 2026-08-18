package eu.dnetlib.iis.wf.metadataextraction.parser;

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

    /**
     * Returns the Grobid client, initializing it lazily on first use.
     */
    private GrobidClient getGrobidClient() {
        if (grobidClient == null) {
            grobidClient = new GrobidClient(grobidServerUrl, connectionTimeout, readTimeout, 10000L, 10);
        }
        return grobidClient;
    }
}
