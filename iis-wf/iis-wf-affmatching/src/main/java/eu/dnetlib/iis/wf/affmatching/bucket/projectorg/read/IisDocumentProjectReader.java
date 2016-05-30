package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * Implementation of {@link DocumentProjectReader} that reads
 * {@link DocumentToProject} objects written in avro files.
 * 
 * @author madryk
 *
 */
public class IisDocumentProjectReader implements DocumentProjectReader {

    private static final long serialVersionUID = 1L;


    private SparkAvroLoader avroLoader = new SparkAvroLoader();
    
    private DocumentProjectConverter converter = new DocumentProjectConverter();


    //------------------------ LOGIC --------------------------
    
    /**
     * Reads {@link AffMatchDocumentProject}s rdd written as avro files under <code>inputPath</code>
     * with {@link DocumentToProject} schema.
     */
    @Override
    public JavaRDD<AffMatchDocumentProject> readDocumentProjects(JavaSparkContext sc, String inputPath) {
        
        Preconditions.checkNotNull(sc);
        Preconditions.checkArgument(StringUtils.isNotBlank(inputPath));
        
        return avroLoader.loadJavaRDD(sc, inputPath, DocumentToProject.class)
            .map(docProj -> converter.convert(docProj));
        
    }

}
