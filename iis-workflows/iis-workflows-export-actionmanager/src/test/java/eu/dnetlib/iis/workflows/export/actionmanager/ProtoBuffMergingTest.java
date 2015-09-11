package eu.dnetlib.iis.workflows.export.actionmanager;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Context;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;

public class ProtoBuffMergingTest {


	@Test
	public void testMergeArrays() throws Exception {
		String contextId1 = "ctxId1";
		String contextId2 = "ctxId2";
		
		Oaf oaf1 = buildOaf(contextId1);
		Oaf oaf2 = buildOaf(contextId2);
		Oaf mergedBuilder = Oaf.newBuilder().mergeFrom(oaf1).mergeFrom(oaf2).build();
		assertEquals(1, oaf1.getEntity().getResult().getMetadata().getContextCount());
		assertEquals(1, oaf2.getEntity().getResult().getMetadata().getContextCount());
		assertEquals(2, mergedBuilder.getEntity().getResult().getMetadata().getContextCount());
		assertEquals(contextId1, mergedBuilder.getEntity().getResult().getMetadata().getContext(0).getId());
		assertEquals(contextId2, mergedBuilder.getEntity().getResult().getMetadata().getContext(1).getId());
	}
	
	Oaf buildOaf(String contextId) {
		Oaf.Builder oafBuilder = Oaf.newBuilder();
		oafBuilder.setKind(Kind.entity);
		oafBuilder.setTimestamp(System.currentTimeMillis());
		OafEntity.Builder entityBuilder = OafEntity.newBuilder();
		entityBuilder.setId("id");
		entityBuilder.setType(Type.result);
		Result.Builder resultBuilder = Result.newBuilder();
		Metadata.Builder metaBuilder = Metadata.newBuilder();
		metaBuilder.addContext(Context.newBuilder().setId(contextId).build());
		resultBuilder.setMetadata(metaBuilder.build());
		entityBuilder.setResult(resultBuilder.build());
		oafBuilder.setEntity(entityBuilder.build());
		return oafBuilder.build();
	}
}
