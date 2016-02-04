package eu.dnetlib.iis.workflows.citationmatching.converter;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.workflows.citationmatching.converter.entity_id.CitEntityId;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import pl.edu.icm.coansys.citations.data.CitationMatchingProtos;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

import java.io.IOException;

/**
 * @author Mateusz Fedoryszak
 */
public class AvroDocumentMetadataReferencesToProtoBufMatchableEntityMapper
        extends Mapper<AvroKey<DocumentMetadata>, NullWritable, Text, BytesWritable> {
    private final static int MAX_CITATION_LENGTH = 10000;

    private final Logger log = Logger.getLogger(AvroDocumentMetadataReferencesToProtoBufMatchableEntityMapper.class);

    private final Text citIdWritable = new Text();
    private final BytesWritable docMetaWritable = new BytesWritable();

    @Override
    protected void map(AvroKey<DocumentMetadata> avro, NullWritable ignore, Context context)
            throws IOException, InterruptedException {
        String docId = null;
        try {
            docId = avro.datum().getId().toString();
            for (ReferenceMetadata reference : avro.datum().getReferences()) {
                String citId = null;
                try {
                    // TODO MiconCodeReview: I would extract a method 'private static String getCitId(AvroKey<DocumentMetadata> fromAvro).'
                    citId = new CitEntityId(avro.datum().getId().toString(), reference.getPosition()).toString();
                    citIdWritable.set(citId);

                    byte[] metaBytes = referenceToMatchableEntityData(citId, reference).toByteArray();
                    docMetaWritable.set(metaBytes, 0, metaBytes.length);

                    context.write(citIdWritable, docMetaWritable);
                } catch (Exception e) {
                    log.error(
                            "Error while processing " + (citId != null ? "citation " + citId : "document " + docId), e);
                }
            }
        } catch (Exception e) {
            log.error("Error" + (docId != null ? " while processing document " + docId : ""), e);
        }
    }

    public static CitationMatchingProtos.MatchableEntityData referenceToMatchableEntityData(
            String id, ReferenceMetadata reference) {
//        CitationMatchingProtos.MatchableEntityData.Builder builder = MatchableEntity.fromBasicMetadata(id,
//                Util.avroBasicMetadataToProtoBuf(reference.getBasicMetadata())).data().toBuilder();
//
//        String rawText;
//        if (reference.getRawText() != null) {
//            rawText = reference.getRawText().toString();
//        }
//        else {
//            rawText = builder.getAuthor() + ": " + builder.getTitle() + ". " + builder.getSource() +
//                    " (" + builder.getYear() + ") " + builder.getPages();
//        }
//        if (rawText.length() > MAX_CITATION_LENGTH) {
//            throw new IllegalArgumentException("Citation string exceeds length limit (" + MAX_CITATION_LENGTH + ").");
//        }
//        builder.addAuxiliary(CitationMatchingProtos.KeyValue.newBuilder().setKey("rawText").setValue(rawText));
//        return builder.build();
    	return null;
    }
}
