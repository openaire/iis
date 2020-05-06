package eu.dnetlib.iis.wf.referenceextraction.patent;

import org.apache.hadoop.conf.Configuration;

/**
 * {@link PatentServiceFacade} factory interface.
 * 
 * @author mhorst
 *
 */
public interface PatentServiceFacadeFactory {

    
    /**
     * Create patent service facade instance.
     */
    PatentServiceFacade create(Configuration conf);
}
