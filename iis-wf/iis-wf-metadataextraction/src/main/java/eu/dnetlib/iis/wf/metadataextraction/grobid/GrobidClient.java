package eu.dnetlib.iis.wf.metadataextraction.grobid;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class GrobidClient {

    private final String grobidUrl;
    
    public GrobidClient(String grobidUrl) {
        this.grobidUrl = grobidUrl;
    }
    
    /**
     * Process a PDF document with Grobid's fulltext service
     * 
     * @param pdfPath Path to the PDF file
     * @return The TEI XML result as a string
     * @throws IOException If an error occurs during processing
     */
    public String processFulltextDocument(String pdfPath) throws IOException {
        String processingUrl = grobidUrl + "/api/processFulltextDocument";
        
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            File pdfFile = new File(pdfPath);
            
            if (!pdfFile.exists() || !pdfFile.isFile()) {
                throw new IOException("PDF file not found or is not a regular file: " + pdfPath);
            }
            
            HttpPost httpPost = new HttpPost(processingUrl);
            
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            // builder.addBinaryBody("input", pdfFile, ContentType.APPLICATION_PDF, pdfFile.getName());
            builder.addBinaryBody("input", pdfFile, ContentType.APPLICATION_OCTET_STREAM, pdfFile.getName());
            
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
    }
}
