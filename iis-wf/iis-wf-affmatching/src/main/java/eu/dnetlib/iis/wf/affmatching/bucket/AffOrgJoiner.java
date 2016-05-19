package eu.dnetlib.iis.wf.affmatching.bucket;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import scala.Tuple2;

/**
 * 
 * Contract for services which join {@link AffMatchAffiliation}s to {@link AffMatchOrganization}s. <br/>
 * An implementation of this class will define a specific rule of joining affiliations with organizations.
 * 
 * @author Łukasz Dumiszewski
*/

public interface AffOrgJoiner extends Serializable {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Joins the given affiliations with organizations according to a certain rule. Returns joined pairs of {@link AffMatchAffiliation}
     * and {@link AffMatchAffiliation}.
     */
    public JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> join(JavaRDD<AffMatchAffiliation> affiliations, JavaRDD<AffMatchOrganization> organizations);
    
}
