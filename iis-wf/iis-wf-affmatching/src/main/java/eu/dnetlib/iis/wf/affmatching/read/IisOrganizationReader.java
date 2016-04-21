package eu.dnetlib.iis.wf.affmatching.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * Implementation of {@link OrganizationReader} that reads IIS organizations, objects of {@link Organization} written
 * in avro files. 
 * 
 * @author Łukasz Dumiszewski
*/

public class IisOrganizationReader implements Serializable, OrganizationReader {

    
    private static final long serialVersionUID = 1L;
    
    
    private SparkAvroLoader avroLoader = new SparkAvroLoader();
    
    private OrganizationConverter organizationConverter = new OrganizationConverter();
    
    

    //------------------------ LOGIC --------------------------
    
    /**
     * Reads {@link Organization}s written as avro files under <code>inputPath</code>
     */
    @Override
    public JavaRDD<AffMatchOrganization> readOrganizations(JavaSparkContext sc, String inputPath) {

        JavaRDD<Organization> sourceOrganizations = avroLoader.loadJavaRDD(sc, inputPath, Organization.class);
        
        JavaRDD<AffMatchOrganization> organizations = sourceOrganizations.map(srcOrg -> organizationConverter.convert(srcOrg));
    
        return organizations;
    }


    //------------------------ SETTERS --------------------------

    public void setAvroLoader(SparkAvroLoader avroLoader) {
        this.avroLoader = avroLoader;
    }

    public void setOrganizationConverter(OrganizationConverter organizationConverter) {
        this.organizationConverter = organizationConverter;
    }
    
}
