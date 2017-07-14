package eu.dnetlib.iis.wf.export.actionmanager.entity;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

/**
 * Static entities filter.
 * Retrives resource location from system property: static.filter.resource.location.
 * When property not set - empty RDD is provided.
 * 
 * @author mhorst
 *
 */
public class StaticFilterMock implements EntityFilter {
    
    
    protected static final String SYSTEM_PROPERTY_RESOURCE_LOCATION = "static.filter.resource.location";
    
    //------------------------ LOGIC --------------------------

    @Override
    public JavaRDD<CharSequence> provideRDD(JavaSparkContext sc, String relationsPath, String entitiesPath) {
        try {
            String resourceLocation = System.getProperty(SYSTEM_PROPERTY_RESOURCE_LOCATION);
            if (resourceLocation != null) {
                return sc.parallelize(Lists.newArrayList(IOUtils.toString(StaticFilterMock.class.getResourceAsStream(resourceLocation), "utf8")));    
            } else {
                return sc.parallelize(Lists.newArrayList());
            }
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }

}
