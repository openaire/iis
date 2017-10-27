package eu.dnetlib.iis.wf.ptm;

import java.util.Map;
import java.util.Set;

import org.springframework.web.client.RestTemplate;

import eu.dnetlib.ptm.service.Command;
import eu.dnetlib.ptm.service.ExecutionReport;
import eu.dnetlib.ptm.service.PtmException;
import eu.dnetlib.ptm.service.PtmService;

/**
 * RESTful PTM service facade.
 * 
 * @author mhorst
 *
 */
public class PtmServiceFacade implements PtmService {
    

    /**
     * Remote RESTful PTM service location.
     */
    private final String serviceLocation;
    
    /**
     * Restful template to be used for communication.
     */
    private final RestTemplate restTemplate;
    
    
    /**
     * @param serviceLocation remote RESTful PTM service location
     */
    public PtmServiceFacade(String serviceLocation) {
        if (serviceLocation.charAt(serviceLocation.length()-1) != '/') {
            this.serviceLocation = serviceLocation + '/';
        } else {
            this.serviceLocation = serviceLocation;    
        }
        this.restTemplate = new RestTemplate();
    }

    @Override
    public String annotate(Command command) {
        return executeRestJob("annotate", command != null ? command.getMap() : null);
    }

    @Override
    public String topicModeling(Command command) {
        return executeRestJob("topic-modeling", command != null ? command.getMap() : null);
    }

    @Override
    public ExecutionReport getReport(String jobId) throws PtmException {
        return restTemplate.getForObject(serviceLocation + "report/{jobId}", ExecutionReport.class, jobId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<String> listJobs() {
        return restTemplate.getForObject(serviceLocation + "list", Set.class);
    }
    
    @Override
    public boolean cancel(String jobId) throws PtmException {
        return restTemplate.getForObject(serviceLocation + "cancel/{jobId}", Boolean.class, jobId);
    }
    
    // -------------------------------- PRIVATE ----------------------------------------
    
    /**
     * Executes REST job providing job identifier.
     * @param action action name to be executed
     * @param params set of input parameters
     * @return job identifier
     */
    private String executeRestJob(String action, Map<String,String> params) {
        
        StringBuilder strBuilder = new StringBuilder(serviceLocation);
        
        strBuilder.append(action);
        
        if (params != null && params.size() > 0) {
            strBuilder.append('?');
            for (String currentParamName : params.keySet()) {
                strBuilder.append("map[");
                strBuilder.append(currentParamName);
                strBuilder.append("]={");
                strBuilder.append(currentParamName);
                strBuilder.append("}&");
            }
            strBuilder.deleteCharAt(strBuilder.length()-1);
        }
        
        return restTemplate.getForObject(strBuilder.toString(), String.class, params);
    }


}
