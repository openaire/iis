package eu.dnetlib.iis.wf.export.actionmanager.entity;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Entity filter interface.
 * 
 * @author mhorst
 *
 */
public interface EntityFilter {

    /**
     * Filters entities based on matched relations.
     * @param sc spark context 
     * @param relationsPath path pointing to entity relations
     * @param entitiesPath path pointing to entities to be filtered
     * @return
     */
    JavaRDD<CharSequence> provideRDD(JavaSparkContext sc, String relationsPath, String entitiesPath);
}
