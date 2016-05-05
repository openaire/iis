package eu.dnetlib.iis.wf.affmatching.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.model.DocumentProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * Implementation of {@link DocumentProjectReader} that reads IIS relations,
 * objects of {@link DocumentToProject} written in avro files.
 * 
 * @author mhorst
 */

public class IisDocumentProjectReader implements Serializable, DocumentProjectReader {

    private static final long serialVersionUID = 1L;

    private SparkAvroLoader avroLoader = new SparkAvroLoader();

    private DocumentProjectConverter converter = new DocumentProjectConverter();

    // ------------------------ LOGIC --------------------------

    /**
     * Reads {@link Organization}s written as avro files under <code>inputPath</code>
     */
    @Override
    public JavaRDD<DocumentProject> readDocumentProject(JavaSparkContext sc, String inputPath) {
        return avroLoader.loadJavaRDD(sc, inputPath, DocumentToProject.class)
                .map(srcDocProj -> converter.convert(srcDocProj));
    }

    // ------------------------ SETTERS --------------------------

    public void setAvroLoader(SparkAvroLoader avroLoader) {
        this.avroLoader = avroLoader;
    }

    public void setDocumentProjectConverter(DocumentProjectConverter converter) {
        this.converter = converter;
    }

}
