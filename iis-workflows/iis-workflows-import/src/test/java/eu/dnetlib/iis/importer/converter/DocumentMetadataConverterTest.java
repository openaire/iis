package eu.dnetlib.iis.importer.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.mapred.IISDataImporterMapper;

/**
 * {@link DocumentMetadataConverter} test class.
 * @author mhorst
 *
 */
public class DocumentMetadataConverterTest {

	@Test
	public void testCompare() throws Exception {
		Set<byte[]> sortedSet = new TreeSet<byte[]>(new Comparator<byte[]>() {
			@Override
			public int compare(byte[] o1, byte[] o2) {
				return IISDataImporterMapper.compare(o1, o2);
			}
		});
		
		String str1 = "test_01";
		String str2 = "test_012";
		String str3 = "test_2";
		
		sortedSet.add(str3.getBytes());
		sortedSet.add(str2.getBytes());
		sortedSet.add(str1.getBytes());
		Iterator<byte[]> it = sortedSet.iterator();
		assertEquals(str1, new String(it.next()));
		assertEquals(str2, new String(it.next()));
		assertEquals(str3, new String(it.next()));
		assertFalse(it.hasNext());
		
		assertEquals(IISDataImporterMapper.compare(
				"body".getBytes(HBaseConstants.STATIC_FIELDS_ENCODING_UTF8), 
				HBaseConstants.QUALIFIER_BODY), 0);
	}
	
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
