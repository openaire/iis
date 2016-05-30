package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * Implementation of {@link DocumentProjectReader} that reads IIS relations,
 * objects of {@link DocumentToProject} written in avro files.
 * 
 * @author mhorst
 */

public class IisInferredDocumentProjectReader implements DocumentProjectReader {

    private static final long serialVersionUID = 1L;

    private SparkAvroLoader avroLoader = new SparkAvroLoader();

    private InferredDocumentProjectConverter converter = new InferredDocumentProjectConverter();

    // ------------------------ LOGIC --------------------------

    /**
     * Reads {@link AffMatchDocumentProject}s rdd written as avro files under <code>inputPath</code>
     * with {@link DocumentToProject} schema.
     */
    @Override
    public JavaRDD<AffMatchDocumentProject> readDocumentProjects(JavaSparkContext sc, String inputPath) {
        return avroLoader.loadJavaRDD(sc, inputPath, DocumentToProject.class)
                .map(srcDocProj -> converter.convert(srcDocProj));
    }

    // ------------------------ SETTERS --------------------------

    public void setAvroLoader(SparkAvroLoader avroLoader) {
        this.avroLoader = avroLoader;
    }

    public void setDocumentProjectConverter(InferredDocumentProjectConverter converter) {
        this.converter = converter;
    }

}
