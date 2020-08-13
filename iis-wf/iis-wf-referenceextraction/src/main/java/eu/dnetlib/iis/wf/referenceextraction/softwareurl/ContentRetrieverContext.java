package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.Serializable;

/**
 * Content retriveal context.
 * 
 * @author mhorst
 *
 */
public class ContentRetrieverContext implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 6360286121671381083L;

    private ContentRetriever contentRetriever;
    
    private int connectionTimeout;
    
    private int readTimeout;
    
    private int maxPageContentLength;
    
    private int numberOfEmittedFiles;
    
    private int numberOfPartitionsForCrawling;

    public ContentRetrieverContext() {}
    
    public ContentRetrieverContext(String contentRetrieverClassName, int connectionTimeout, int readTimeout,
            int maxPageContentLength, int numberOfEmittedFiles, int numberOfPartitionsForCrawling)
            throws Exception {
        @SuppressWarnings("unchecked")
        Class<ContentRetriever> clazz = (Class<ContentRetriever>) Class.forName(contentRetrieverClassName);
        this.contentRetriever = clazz.getConstructor().newInstance();
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
        this.maxPageContentLength = maxPageContentLength;
        this.numberOfEmittedFiles = numberOfEmittedFiles;
        this.numberOfPartitionsForCrawling = numberOfPartitionsForCrawling;
    }


    public ContentRetriever getContentRetriever() {
        return contentRetriever;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public int getMaxPageContentLength() {
        return maxPageContentLength;
    }

    public int getNumberOfEmittedFiles() {
        return numberOfEmittedFiles;
    }
    
    public int getNumberOfPartitionsForCrawling() {
        return numberOfPartitionsForCrawling;
    }

    public void setContentRetriever(ContentRetriever contentRetriever) {
        this.contentRetriever = contentRetriever;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public void setMaxPageContentLength(int maxPageContentLength) {
        this.maxPageContentLength = maxPageContentLength;
    }

    public void setNumberOfEmittedFiles(int numberOfEmittedFiles) {
        this.numberOfEmittedFiles = numberOfEmittedFiles;
    }
    
    public void setNumberOfPartitionsForCrawling(int numberOfPartitionsForCrawling) {
        this.numberOfPartitionsForCrawling = numberOfPartitionsForCrawling;
    }
    
}
