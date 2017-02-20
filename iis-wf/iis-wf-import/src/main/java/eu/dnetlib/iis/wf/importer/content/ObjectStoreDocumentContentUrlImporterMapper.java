package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import eu.dnetlib.data.objectstore.rmi.ObjectStoreFile;
import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.wf.importer.facade.ObjectStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;

/**
 * {@link Identifier} based ObjectStore records importer producing {@link DocumentContentUrl} output.
 * 
 * @author mhorst
 *
 */
public class ObjectStoreDocumentContentUrlImporterMapper extends Mapper<AvroKey<Identifier>, NullWritable, AvroKey<DocumentContentUrl>, NullWritable> {

    private final Logger log = Logger.getLogger(ObjectStoreDocumentContentUrlImporterMapper.class);

    /**
     * Progress log interval.
     */
    private static final int PROGRESS_LOG_INTERVAL = 100000;
    
    /**
     * MDStore service facade.
     */
    private ObjectStoreFacade objectStoreFacade;
    
    
    //------------------------ LOGIC --------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            objectStoreFacade = ServiceFacadeUtils.instantiate(context.getConfiguration());
        } catch (ServiceFacadeException e) {
            throw new RuntimeException("unable to instantiate MDStore service facade", e);
        }
    }

    @Override
    protected void map(AvroKey<Identifier> key, NullWritable value, Context context) throws IOException, InterruptedException {
        try {
            String objectStoreId = key.datum().getId().toString();
            
            long startTime = System.currentTimeMillis();
            long intervalTime = startTime;
            int recordIndex=0;

            log.info("starting importing process from object store: " + objectStoreId);
            
            for (String record : objectStoreFacade.deliverObjects(objectStoreId, 0l, System.currentTimeMillis())) {
                context.write(new AvroKey<DocumentContentUrl>(buildRecord(record)), NullWritable.get());
                if (recordIndex>0 && recordIndex%PROGRESS_LOG_INTERVAL==0) {
                    log.info("content retrieval progress: " + recordIndex + ", time taken to process " +
                            PROGRESS_LOG_INTERVAL + " elements: " + ((System.currentTimeMillis() - intervalTime)/1000) + " secs");
                    intervalTime = System.currentTimeMillis();
                }
                recordIndex++;
            }
            
            log.info("URL importing process from object store: " + objectStoreId + " has finished");
        } catch (ServiceFacadeException e) {
            throw new IOException("Unable to deliver objects using ObjectStore facade", e);
        }
        

    }
    
    // ------------------------ PRIVATE --------------------------
    
    /**
     * Builds {@link DocumentContentUrl} record out of ObjectStore metadata record encoded as JSON.
     * 
     * @param metaJsonRecord metadata record obtained from ObjectStore
     */
    private DocumentContentUrl buildRecord(String metaJsonRecord) {
        ObjectStoreFile objStoreFile = ObjectStoreFile.createObject(metaJsonRecord);
        String resultId = ObjectStoreContentProviderUtils.extractResultIdFromObjectId(objStoreFile.getObjectID());
        DocumentContentUrl.Builder documentContentUrlBuilder = DocumentContentUrl.newBuilder();
        documentContentUrlBuilder.setId(resultId);
        documentContentUrlBuilder.setUrl(objStoreFile.getURI());
        documentContentUrlBuilder.setMimeType(objStoreFile.getMimeType());
        documentContentUrlBuilder.setContentChecksum(objStoreFile.getMd5Sum());
        documentContentUrlBuilder.setContentSizeKB(objStoreFile.getFileSizeKB());
        return documentContentUrlBuilder.build();
    }
    
}
