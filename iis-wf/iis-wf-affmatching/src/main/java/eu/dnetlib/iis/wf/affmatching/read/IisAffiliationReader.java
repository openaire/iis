package eu.dnetlib.iis.wf.affmatching.read;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

import java.io.Serializable;

/**
 * Implementation of {@link AffiliationReader} that reads IIS affiliations, {@link Affiliation} objects from avro files.

 * @author Łukasz Dumiszewski
*/
public class IisAffiliationReader implements Serializable, AffiliationReader {

    private static final long serialVersionUID = 1L;

    private AffiliationConverter affiliationConverter = new AffiliationConverter();

    private SparkAvroLoader sparkAvroLoader = new SparkAvroLoader();

    //------------------------ LOGIC --------------------------

    /**
     * Reads {@link Affiliation}s written as avro files under <code>inputPath</code>
     */
    @Override
    public JavaRDD<AffMatchAffiliation> readAffiliations(JavaSparkContext sc, String inputPath) {
        Preconditions.checkNotNull(sc);
        Preconditions.checkArgument(StringUtils.isNotBlank(inputPath));

        JavaRDD<ExtractedDocumentMetadata> sourceAffiliations = sparkAvroLoader
                .loadJavaRDD(sc, inputPath, ExtractedDocumentMetadata.class);

        return sourceAffiliations.flatMap(srcAff -> affiliationConverter.convert(srcAff).iterator());
    }
}
