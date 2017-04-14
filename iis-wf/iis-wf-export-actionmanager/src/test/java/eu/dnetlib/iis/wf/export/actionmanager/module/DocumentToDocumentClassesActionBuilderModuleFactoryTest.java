package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.common.hbase.HBaseConstants.CLASSIFICATION_HIERARCHY_SEPARATOR;
import static eu.dnetlib.iis.common.hbase.HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
import static eu.dnetlib.iis.common.hbase.HBaseConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClass;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClasses;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;

/**
 * @author mhorst
 *
 */
public class DocumentToDocumentClassesActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToDocumentClasses> {

    
    // ------------------------ CONSTRUCTORS -----------------------------
    
    public DocumentToDocumentClassesActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToDocumentClassesActionBuilderModuleFactory.class, AlgorithmName.document_classes);
    }

    // ------------------------ TESTS ------------------------------------
    
    @Test
    public void testBuildEmptyClasses() throws Exception {
     // given
        String docId = "documentId";
        ActionBuilderModule<DocumentToDocumentClasses> module =  factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(DocumentToDocumentClasses.newBuilder().setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }
    
    @Test
    public void testBuildBelowThreshold() throws Exception {
     // given
        String docId = "docId";
        float confidenceLevel = 0.4f;
        ActionBuilderModule<DocumentToDocumentClasses> module =  factory.instantiate(config, agent, actionSetId);

        // execute
        List<AtomicAction> actions =  module.build(buildDocumentToDocumentClasses(docId, confidenceLevel));
        
        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }
    
    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "docId";
        float confidenceLevel = 0.6f;
        ActionBuilderModule<DocumentToDocumentClasses> module =  factory.instantiate(config, agent, actionSetId);

        // execute
        List<AtomicAction> actions =  module.build(buildDocumentToDocumentClasses(docId, confidenceLevel));
        
        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction action = actions.get(0);
        assertNotNull(action);
        assertEquals(agent, action.getAgent());
        assertNotNull(action.getRowKey());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(docId, action.getTargetRowKey());
        assertEquals(Type.result.toString(), action.getTargetColumnFamily());
        assertOaf(action.getTargetValue(), docId, confidenceLevel);
    }
    
    
    // ------------------------ PRIVATE ----------------------------------
    
    private static DocumentToDocumentClasses buildDocumentToDocumentClasses(String docId, float confidenceLevel) {
        DocumentToDocumentClasses.Builder builder = DocumentToDocumentClasses.newBuilder();
        builder.setDocumentId(docId);
        
        DocumentClasses.Builder docClassBuilder = DocumentClasses.newBuilder();
        docClassBuilder.setACMClasses(Lists.newArrayList(DocumentClass.newBuilder()
                .setConfidenceLevel(confidenceLevel).setClassLabels(Lists.newArrayList("acm1", "acm2")).build()));
        docClassBuilder.setArXivClasses(Lists.newArrayList(DocumentClass.newBuilder()
                .setConfidenceLevel(confidenceLevel).setClassLabels(Lists.newArrayList("arxiv1", "arxiv2")).build()));
        docClassBuilder.setDDCClasses(Lists.newArrayList(DocumentClass.newBuilder()
                .setConfidenceLevel(confidenceLevel).setClassLabels(Lists.newArrayList("ddc1", "ddc2")).build()));
        docClassBuilder.setMeshEuroPMCClasses(Lists.newArrayList(DocumentClass.newBuilder()
                .setConfidenceLevel(confidenceLevel).setClassLabels(Lists.newArrayList("mesh1", "mesh2")).build()));
        docClassBuilder.setWoSClasses(Lists.newArrayList(DocumentClass.newBuilder()
                .setConfidenceLevel(confidenceLevel).setClassLabels(Lists.newArrayList("wos1", "wos2")).build()));
        
        builder.setClasses(docClassBuilder.build());
        return builder.build();
    }
    
    private void assertOaf(byte[] oafBytes, String docId, float confidenceLevel) throws InvalidProtocolBufferException {
        assertNotNull(oafBytes);
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.mergeFrom(oafBytes);
        Oaf oaf = oafBuilder.build();
        assertNotNull(oaf);

        assertTrue(KindProtos.Kind.entity == oaf.getKind());
        assertNotNull(oaf.getEntity());
        assertEquals(docId, oaf.getEntity().getId());
        assertNotNull(oaf.getEntity().getResult());
        assertEquals(5, oaf.getEntity().getResult().getMetadata().getSubjectCount());

        assertSubject("arxiv1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "arxiv2", 
                HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_ARXIV, 
                confidenceLevel, oaf.getEntity().getResult().getMetadata().getSubject(0));
        
        assertSubject("ddc1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "ddc2", 
                HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_DDC, 
                confidenceLevel, oaf.getEntity().getResult().getMetadata().getSubject(1));
        
        assertSubject("wos1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "wos2", 
                HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_WOS, 
                confidenceLevel, oaf.getEntity().getResult().getMetadata().getSubject(2));
        
        assertSubject("mesh1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "mesh2", 
                HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_MESHEUROPMC, 
                confidenceLevel, oaf.getEntity().getResult().getMetadata().getSubject(3));
        
        assertSubject("acm1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "acm2", 
                HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_ACM, 
                confidenceLevel, oaf.getEntity().getResult().getMetadata().getSubject(4));
    }
    
    private void assertSubject(String expectedValue, String expectedTaxonomy,  
            float confidenceLevel, StructuredProperty subject) {
        assertNotNull(subject);
        
        assertNotNull(subject.getQualifier());
        assertEquals(expectedTaxonomy, subject.getQualifier().getClassid());
        assertEquals(expectedTaxonomy, subject.getQualifier().getClassname());
        assertEquals(SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES, subject.getQualifier().getSchemeid());
        assertEquals(SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES, subject.getQualifier().getSchemename());
        
        assertEquals(expectedValue, subject.getValue());

        assertNotNull(subject.getDataInfo());
        float normalizedTrust = confidenceLevel * CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(subject.getDataInfo().getTrust()), 0.0001);
    }
}
