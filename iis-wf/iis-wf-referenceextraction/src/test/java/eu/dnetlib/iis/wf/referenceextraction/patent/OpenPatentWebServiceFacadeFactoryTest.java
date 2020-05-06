package eu.dnetlib.iis.wf.referenceextraction.patent;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.*;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;

public class OpenPatentWebServiceFacadeFactoryTest {

    @Test
    public void testRemoteAccess() throws Exception {
        // FIXME remove this method after running several tests
        
        // testing scenarios
        // invalid credential, missing credential
        // missing patent record
        
        
        Configuration conf = new Configuration();
        
        conf.set(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT, "1000");
        conf.set(PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT, "1000");
        
        conf.set(PARAM_CONSUMER_KEY, "fzkakRNW3punGAstva3jyN2uffw91tn2");
        conf.set(PARAM_CONSUMER_SECRET, "CIBLZeO34hCK6rwx");
        
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_HOST, "ops.epo.org");
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_PORT, "443");
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME, "https");
        //!!!! MUSI BYĆ Z '/' na początku!!!
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT, "/3.2/auth/accesstoken");
        
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_HOST, "ops.epo.org");
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_PORT, "443");
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_SCHEME, "https");
        //!!!! MUSI BYĆ Z '/' na początku!!!
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT, "/3.2/rest-services/published-data/publication/docdb");
        
        OpenPatentWebServiceFacadeFactory facadeFactory = new OpenPatentWebServiceFacadeFactory();
        PatentServiceFacade serviceFacade = facadeFactory.create(conf);
        
        assertNotNull(serviceFacade);
        
        ImportedPatent.Builder patentMetaBuilder = ImportedPatent.newBuilder();
        patentMetaBuilder.setApplnNr("irrelevant");
        patentMetaBuilder.setPublnAuth("WO");
        patentMetaBuilder.setPublnNr("0042078");
        patentMetaBuilder.setPublnKind("A1");
        ImportedPatent importedPatent = patentMetaBuilder.build();
        
        for (int i=0; i< 10; i++) {
            long startTime = System.currentTimeMillis();
            String result = serviceFacade.getPatentMetadata(importedPatent);
            System.out.println("iteration no " + i + ", got result in " + (System.currentTimeMillis()-startTime) + " ms");
            assertNotNull(result);
            System.out.println(result);
        }

    }
}
