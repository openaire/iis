package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

import com.machinepublishers.jbrowserdriver.JBrowserDriver;
import com.machinepublishers.jbrowserdriver.Settings;
import com.machinepublishers.jbrowserdriver.Settings.Builder;
import com.machinepublishers.jbrowserdriver.Timezone;
import com.machinepublishers.jbrowserdriver.UserAgent;

/**
 * HTTP based, javascript aware content retriever.
 * @author mhorst
 *
 */
public class JavaScriptAwareContentRetriever implements ContentRetriever {

    
    private static final long serialVersionUID = 1149015004253691194L;
    
    private static final Logger log = Logger.getLogger(JavaScriptAwareContentRetriever.class);

    private transient ThreadLocal<JBrowserDriver> threadLocalDriver;
    
    // ------------------------- CONSTRUCTORS ---------------------------------
    
    public JavaScriptAwareContentRetriever() {
        this.threadLocalDriver = buildDriverAsThreadLocal();
    }
    
    // ------------------------- PUBLIC ---------------------------------
    
    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url, int connectionTimeout, int readTimeout,
            int maxPageContentLength) {
        long startTime = System.currentTimeMillis();
        String currentUrl = url.toString();
        
        try {
//            JBrowserDriver driver = new JBrowserDriver();
            JBrowserDriver driver = threadLocalDriver.get();
            
            log.info("starting content retrieval for url: " + currentUrl);
            driver.get(currentUrl);
            // TODO remove this comment
            log.info("got content after: " + (System.currentTimeMillis()-startTime));
            //FIXME how to restrict response size with JBrowserDriver?
            try {
                return new ContentRetrieverResponse(driver.getPageSource());
            } finally {
                log.info("finished content retrieval for url: " + currentUrl + " in " +
                        (System.currentTimeMillis()-startTime) + " ms");
            }
        } catch (Exception e) {
            log.error("content retrieval failed for url: " + currentUrl, e);
            return new ContentRetrieverResponse(e);
        }
    }
    
    // ------------------------------- PRIVATE ----------------------------
    
    /**
     * Custom deserialization method to be used within Spark to instantiate content retriever. 
     */
    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
        // perform the default de-serialization first
        inputStream.defaultReadObject();
        
        this.threadLocalDriver = buildDriverAsThreadLocal();
    }
    
    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        // perform the default serialization for all non-transient, non-static fields
        outputStream.defaultWriteObject();
    }

    private ThreadLocal<JBrowserDriver> buildDriverAsThreadLocal() {
        return new ThreadLocal<JBrowserDriver>() {
            @Override
            protected JBrowserDriver initialValue() {
                try {
                    Builder builder = Settings.builder();
                    builder.headless(true);
                    builder.javascript(true);
                    builder.quickRender(true);
                    builder.timezone(Timezone.EUROPE_WARSAW);
                    builder.userAgent(UserAgent.CHROME);
                    // more advanced logging
                    builder.javaOptions("-Dquantum.verbose=true", "-Dprism.verbose=true", "-verbose", "-verbose:class", "-Dprism.useFontConfig=false");
                    return new JBrowserDriver(builder.build());
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
