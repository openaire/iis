package eu.dnetlib.iis.wf.affmatching.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.affmatching.model.ProjectOrganization;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * Implementation of {@link ProjectOrganizationReader} that reads IIS relations,
 * objects of {@link ProjectToOrganization} written in avro files.
 * 
 * @author mhorst
 */

public class IisProjectOrganizationReader implements Serializable, ProjectOrganizationReader {

	private static final long serialVersionUID = 1L;

	private SparkAvroLoader avroLoader = new SparkAvroLoader();

	private ProjectOrganizationConverter converter = new ProjectOrganizationConverter();

	// ------------------------ LOGIC --------------------------

	/**
	 * Reads {@link Organization}s written as avro files under
	 * <code>inputPath</code>
	 */
	@Override
	public JavaRDD<ProjectOrganization> readProjectOrganization(JavaSparkContext sc, String inputPath) {
		return avroLoader.loadJavaRDD(sc, inputPath, ProjectToOrganization.class)
				.map(srcProjOrg -> converter.convert(srcProjOrg));
	}

	// ------------------------ SETTERS --------------------------

	public void setAvroLoader(SparkAvroLoader avroLoader) {
		this.avroLoader = avroLoader;
	}

	public void setProjectOrganizationConverter(ProjectOrganizationConverter converter) {
		this.converter = converter;
	}

}
