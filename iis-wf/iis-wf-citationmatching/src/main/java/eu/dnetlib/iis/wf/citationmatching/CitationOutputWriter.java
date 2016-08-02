package eu.dnetlib.iis.wf.citationmatching;

import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import pl.edu.icm.coansys.citations.OutputWriter;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Writer of output {@link Citation}s
 * 
 * @author madryk
 *
 */
public class CitationOutputWriter implements OutputWriter<Citation, NullWritable>, Serializable {

    private static final long serialVersionUID = 1L;


    private SparkAvroSaver avroSaver = new SparkAvroSaver();

    private CitationMatchingReporter citationMatchingReporter;


    //------------------------ LOGIC --------------------------

    /**
     * Writes rdd with {@link Citation}s to path specified as argument
     */
    @Override
    public void writeMatchedCitations(JavaPairRDD<Citation, NullWritable> matchedCitations, String path) {

        JavaRDD<Citation> matchedCitationsKeys = matchedCitations.keys();
        matchedCitationsKeys.cache();
        
        avroSaver.saveJavaRDD(matchedCitationsKeys, Citation.SCHEMA$, path);

        citationMatchingReporter.report(matchedCitationsKeys);
    }

    
    //------------------------ SETTERS --------------------------
    
    public void setCitationMatchingReporter(CitationMatchingReporter citationMatchingReporter) {
        this.citationMatchingReporter = citationMatchingReporter;
    }

}
