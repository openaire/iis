package eu.dnetlib.iis.wf.importer.content;

import java.util.Map;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.wf.importer.facade.ObjectStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building ObjectStore facade mocks.
 * 
 * @author mhorst
 *
 */
public class MockObjectStoreFacadeFactory implements ServiceFacadeFactory<ObjectStoreFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ObjectStoreFacade instantiate(Map<String, String> parameters) {
        return new MockObjectStoreFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * ObjectStore mock providing static metadata records.
     *
     */
    private static class MockObjectStoreFacade implements ObjectStoreFacade {
        
        private static final String[] metaLocations = new String[] {
                "/eu/dnetlib/iis/wf/importer/content_url/data/input/meta_html.json",
                "/eu/dnetlib/iis/wf/importer/content_url/data/input/meta_pdf.json",
                "/eu/dnetlib/iis/wf/importer/content_url/data/input/meta_pdf2.json",
                "/eu/dnetlib/iis/wf/importer/content_url/data/input/meta_webcrawl.json",
                "/eu/dnetlib/iis/wf/importer/content_url/data/input/meta_xml.json"
        };
        

        //------------------------ LOGIC --------------------------

        @Override
        public Iterable<String> deliverObjects(String objectStoreId, long from, long until)
                throws ServiceFacadeException {
            return ClassPathResourceProvider.getResourcesContents(metaLocations);
        }

    }
}

