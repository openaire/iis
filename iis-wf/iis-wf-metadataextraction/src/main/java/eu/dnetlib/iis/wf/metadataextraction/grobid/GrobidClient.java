package eu.dnetlib.iis.wf.metadataextraction.grobid;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.iis.wf.importer.HttpClientUtils;

/**
 * HTTP client communicating with Grobid server.
 * 
 * @author mhorst
 */
public class GrobidClient implements Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(GrobidClient.class);

    /**
     * Grobid server URL.
     */
    private final String grobidUrl;
    
    /**
     * HTTP client to be closed once the GrobidClient is closed. 
     */
    private final CloseableHttpClient httpClient;
    
    /**
     * Default constructor accepting Grobid server location as parameter.
     * @param grobidUrl grobid server location
     * @param connectionTimeout
     * @param readTimeout
     */
    public GrobidClient(String grobidUrl, int connectionTimeout, int readTimeout) {
        this.grobidUrl = grobidUrl;
        this.httpClient = HttpClientUtils.buildHttpClient(connectionTimeout, readTimeout);
    }
    
    // ------------------------------------- LOGIC ----------------------------------------------
    
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: GrobidClient <url> <input_file>");
            System.exit(1);
        }

        String grobidUrl = args[0];
        String inputFile = args[1];
        
        logger.info("Processing PDF from file: {}", inputFile);
        logger.info("Using Grobid server at {}", grobidUrl);
        
        try (GrobidClient client = new GrobidClient(grobidUrl, 60000, 60000)) {
            long startTime = System.currentTimeMillis();
            String result = client.processPdfFile(inputFile);
            System.out.println(result);
            System.out.println("result generated in " + (System.currentTimeMillis()-startTime) + "ms");
        }
    }
    
    /**
     * Parses a PDF document by relying on an external Grobid service.
     * 
     * @param pdfPath Path to the PDF file
     * @return The TEI XML result as a string
     * @throws IOException If an error occurs during processing
     */
    public String processPdfFile(String pdfPath) throws IOException {
        File pdfFile = new File(pdfPath);
        
        if (!pdfFile.exists() || !pdfFile.isFile()) {
            throw new IOException("PDF file not found or is not a regular file: " + pdfPath);
        }
        return processPdfInputStream(new BufferedInputStream(new FileInputStream(pdfFile)));
    }
    
    /**
     * Parses a PDF input stream by relying on an external Grobid service.
     * @param pdfInputStream PDF input stream
     * @return The TEI XML result as a string
     * @throws IOException If an error occurs during processing
     */
    public String processPdfInputStream(InputStream pdfInputStream) throws IOException {
        HttpPost httpPost = new HttpPost(grobidUrl + "/api/processFulltextDocument");
        
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("input", pdfInputStream, ContentType.APPLICATION_OCTET_STREAM, null);
        
        // Add form parameters for raw citations and affiliations
        builder.addTextBody("includeRawCitations", "1", ContentType.TEXT_PLAIN);
        builder.addTextBody("includeRawAffiliations", "1", ContentType.TEXT_PLAIN);
        
        HttpEntity multipart = builder.build();
        httpPost.setEntity(multipart);

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            int statusCode = response.getStatusLine().getStatusCode();
            
            if (statusCode != 200) {
                String error = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                throw new IOException("Grobid request failed with status code " + statusCode + ": " + error);
            }
            
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null) {
                return EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
            } else {
                throw new IOException("No response entity received from Grobid");
            }
        }
        
    }
    
    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }    

}
