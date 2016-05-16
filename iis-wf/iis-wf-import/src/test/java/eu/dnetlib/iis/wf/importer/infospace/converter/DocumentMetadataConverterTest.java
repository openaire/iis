package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.wf.importer.infospace.converter.DocumentMetadataConverter;

/**
 * {@link DocumentMetadataConverter} test class.
 * @author mhorst
 *
 */
public class DocumentMetadataConverterTest {
	
	@Test
	public void testMerge() throws Exception {
		OafEntity.Builder builder = OafEntity.newBuilder();
		builder.mergeFrom(buildEntityWithSubject("id1", "regular keyword", "iis:document_classes"));
		builder.mergeFrom(buildEntityWithSubject("id2", "inferred keyword", null));
		OafEntity oafEntity = builder.build();
		assertEquals(oafEntity.getResult().getMetadata().getSubjectCount(), 2);
		assertEquals(oafEntity.getId(),"id2");
	}
	
	@Test
	public void testMergeToBuilder() throws Exception {
		OafEntity entity1 = buildEntityWithSubject("id1", "regular keyword", "iis:document_classes");
		OafEntity.Builder builder = entity1.toBuilder();
		builder.mergeFrom(buildEntityWithSubject("id2", "inferred keyword", null));
		OafEntity oafEntity = builder.build();
		assertEquals(oafEntity.getResult().getMetadata().getSubjectCount(), 2);
		assertEquals(oafEntity.getId(),"id2");
	}
	
	private static OafEntity buildEntityWithSubject(String id, String subjectValue, String inferenceProvenance) {
		OafEntity.Builder entityBuilder = OafEntity.newBuilder();
		entityBuilder.setType(Type.result);
		entityBuilder.setId(id);
		Result.Builder resultBuilder = Result.newBuilder();
		Metadata.Builder metaBuilder = Metadata.newBuilder();
		metaBuilder.addSubject(StructuredProperty.newBuilder()
				.setValue("inferred keyword")
				.setDataInfo(DataInfo.newBuilder()
						.setInferenceprovenance("iis:document_classes")
						.setProvenanceaction(Qualifier.newBuilder().build())));
		resultBuilder.setMetadata(metaBuilder);
		entityBuilder.setResult(resultBuilder);
		return entityBuilder.build();
	}
}
