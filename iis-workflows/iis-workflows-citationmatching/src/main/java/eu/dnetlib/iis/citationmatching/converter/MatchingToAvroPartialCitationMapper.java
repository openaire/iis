package eu.dnetlib.iis.citationmatching.converter;

import eu.dnetlib.iis.citationmatching.converter.entity_id.CitEntityId;
import eu.dnetlib.iis.citationmatching.converter.entity_id.DocEntityId;
import eu.dnetlib.iis.citationmatching.schemas.PartialCitation;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import pl.edu.icm.coansys.citations.data.TextWithBytesWritable;

import java.io.IOException;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class MatchingToAvroPartialCitationMapper extends Mapper<TextWithBytesWritable, Text, AvroKey<String>, AvroValue<PartialCitation>> {
    private static class DocEntityIdAndConfidence {
        private final DocEntityId docEntityId;
        private final float confidence;

        public DocEntityIdAndConfidence(DocEntityId docEntityId, float confidence) {
            this.docEntityId = docEntityId;
            this.confidence = confidence;
        }

        public DocEntityId getDocEntityId() {
            return docEntityId;
        }

        public float getConfidence() {
            return confidence;
        }

        public static DocEntityIdAndConfidence fromCoAnSysString(String s) {
            String[] destParts = s.split(":", 2);
            float confidence = Float.parseFloat(destParts[0]);
            DocEntityId docEntityId = DocEntityId.parseFrom(destParts[1]);
            return new DocEntityIdAndConfidence(docEntityId, confidence);
        }
    }

    private final Logger log = Logger.getLogger(MatchingToAvroPartialCitationMapper.class);

    @Override
    protected void map(TextWithBytesWritable src, Text dest, Context context) throws IOException, InterruptedException {
        try {
            CitEntityId citEntityId = CitEntityId.parseFrom(src.text().toString());
            DocEntityIdAndConfidence docEntityIdAndConfidence =
                    DocEntityIdAndConfidence.fromCoAnSysString(dest.toString());

            PartialCitation citation = new PartialCitation();
            citation.setSourceDocumentId(citEntityId.getSourceDocumentId());
            citation.setPosition(citEntityId.getPosition());
            citation.setDestinationDocumentId(docEntityIdAndConfidence.getDocEntityId().getDocumentId());
            citation.setConfidenceLevel(docEntityIdAndConfidence.getConfidence());
//          writing only when matched
            if (citation.getDestinationDocumentId()!=null) {
            	context.write(new AvroKey<String>(src.text().toString()), new AvroValue<PartialCitation>(citation));	
            }
            
        } catch (Exception e) {
            log.error("Error while converting match (" + src.toString() + ", " + dest.toString() + ")", e);
        }
    }
}
