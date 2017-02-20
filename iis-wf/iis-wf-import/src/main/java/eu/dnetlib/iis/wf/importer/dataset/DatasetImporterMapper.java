package eu.dnetlib.iis.wf.importer.dataset;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DatasetToMDStore;
import eu.dnetlib.iis.wf.importer.RecordReceiver;
import eu.dnetlib.iis.wf.importer.facade.MDStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;


/**
 * Mapper importing dataset records from MDStore identified with {@link Identifier} provided at input.
 * 
 * @author mhorst
 *
 */
public class DatasetImporterMapper extends Mapper<AvroKey<Identifier>, NullWritable, NullWritable, NullWritable> {

    
    private final Logger log = Logger.getLogger(this.getClass());
    
    /**
     * Logging interval.
     */
    private final int progressLogInterval = 100000;
    
    /**
     * Multiple outputs.
     */
    private MultipleOutputs mos = null;

    /**
     * Dataset output.
     */
    private String namedOutputDataset;

    /**
     * Dataset to MDStore relation output.
     */
    private String namedOutputDatasetToMDStore;
    
    /**
     * Sax parser to be used for datset metadata extraction.
     */
    private SAXParser saxParser;
    
    /**
     * MDStore service facade.
     */
    private MDStoreFacade mdStoreFacade;
    
    //------------------------ LOGIC --------------------------
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        namedOutputDataset = context.getConfiguration().get("output.dataset");
        if (namedOutputDataset == null || namedOutputDataset.isEmpty()) {
            throw new RuntimeException("no named output provided for dataset");
        }
        namedOutputDatasetToMDStore = context.getConfiguration().get("output.dataset_to_mdstore");
        if (namedOutputDatasetToMDStore == null || namedOutputDatasetToMDStore.isEmpty()) {
            throw new RuntimeException("no named output provided for dataset to mdstore relations");
        }
        mos = new MultipleOutputs(context);
        
        SAXParserFactory parserFactory = SAXParserFactory.newInstance();
        parserFactory.setNamespaceAware(true);
        try {
            saxParser = parserFactory.newSAXParser();
            mdStoreFacade = ServiceFacadeUtils.instantiate(context.getConfiguration());
        } catch (ParserConfigurationException | SAXException e) {
            throw new RuntimeException(e);
        } catch (ServiceFacadeException e) {
            throw new RuntimeException("unable to instantiate MDStore service facade", e);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
    
    @Override
    public void map(AvroKey<Identifier> key, NullWritable ignore, Context context)
            throws IOException, InterruptedException {
        try {
            String mdStoreId = key.datum().getId().toString();
            long startTime = System.currentTimeMillis();
            int currentCount = 0;
            
            for (String record : mdStoreFacade.deliverMDRecords(mdStoreId)) {
                DataciteDumpXmlHandler handler = new DataciteDumpXmlHandler(
                        new RecordReceiver<DataSetReference>() {
                            @Override
                            public void receive(DataSetReference object) throws IOException {
                                try {
                                    mos.write(namedOutputDataset, new AvroKey<DataSetReference>(object));
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        },
                        new RecordReceiver<DatasetToMDStore>() {
                            @Override
                            public void receive(DatasetToMDStore object) throws IOException {
                                try {
                                    mos.write(namedOutputDatasetToMDStore, new AvroKey<DatasetToMDStore>(object));
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }, mdStoreId);

                saxParser.parse(new InputSource(new StringReader(record)), handler);
                currentCount++;
                if (currentCount % progressLogInterval == 0) {
                    log.info("current progress: " + currentCount + ", last package of " + progressLogInterval
                            + " processed in " + ((System.currentTimeMillis() - startTime) / 1000) + " secs");
                    startTime = System.currentTimeMillis();
                }
            }
            log.info("total number of processed records for mdstore " + mdStoreId + ": " + currentCount);
            
        } catch (ServiceFacadeException e) {
            throw new RuntimeException("unable to instantiate MDStore service", e);
        } catch (SAXException e) {
            throw new RuntimeException("unable to parse dataset record", e);
        }
    }
    
}
