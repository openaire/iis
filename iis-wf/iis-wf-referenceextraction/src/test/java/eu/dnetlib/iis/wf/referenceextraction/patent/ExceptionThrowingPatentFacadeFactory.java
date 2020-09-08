package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.Serializable;
import java.util.Map;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Simple factory producing {@link PatentServiceFacade} throwing {@link RuntimeException}.
 * 
 * @author mhorst
 *
 */
public class ExceptionThrowingPatentFacadeFactory implements ServiceFacadeFactory<PatentServiceFacade>, Serializable {
    
    private static final long serialVersionUID = 1L;

    @Override
    public PatentServiceFacade instantiate(Map<String, String> parameters) {
        return new PatentServiceFacade() {
            private static final long serialVersionUID = 1L;

            @Override
            public String getPatentMetadata(ImportedPatent patent) throws Exception {
                throw new RuntimeException("unexpected call for patent: " + patent.getApplnNr());
            }
        };
    }
    
    
}
