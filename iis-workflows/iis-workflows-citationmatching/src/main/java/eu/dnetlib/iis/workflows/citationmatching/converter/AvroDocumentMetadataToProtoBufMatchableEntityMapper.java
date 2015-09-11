package eu.dnetlib.iis.workflows.citationmatching.converter;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.workflows.citationmatching.converter.entity_id.DocEntityId;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import pl.edu.icm.coansys.citations.data.MatchableEntity;

import java.io.IOException;

/**
 * @author Mateusz Fedoryszak
 */
public class AvroDocumentMetadataToProtoBufMatchableEntityMapper
        extends Mapper<AvroKey<DocumentMetadata>, NullWritable, Text, BytesWritable> {
    private final Logger log = Logger.getLogger(AvroDocumentMetadataToProtoBufMatchableEntityMapper.class);

    private final Text docIdWritable = new Text();
    private final BytesWritable docMetaWritable = new BytesWritable();

    @Override
    protected void map(AvroKey<DocumentMetadata> avro, NullWritable ignore, Context context)
            throws IOException, InterruptedException {
        String docId = null;
        try {
            // TODO MiconCodeReview: I would extract a method 'private static String getDocId(AvroKey<DocumentMetadata> fromAvro)'
            docId = new DocEntityId(avro.datum().getId().toString()).toString();
            docIdWritable.set(docId);

            // TODO MiconCodeReview: I would extract a method 'private static MatchableEntity getMatchableEntity(AvroKey<DocumentMetadata> fromAvro)'
            MatchableEntity entity = MatchableEntity.fromBasicMetadata(docId,
                    Util.avroBasicMetadataToProtoBuf(avro.datum().getBasicMetadata()));
            byte[] metaBytes = entity.data().toByteArray();
            docMetaWritable.set(metaBytes, 0, metaBytes.length);

            context.write(docIdWritable, docMetaWritable);
        } catch (Exception e) {
            log.error("Error" + (docId != null ? " while processing document " + docId : ""), e);
        }
    }
}
