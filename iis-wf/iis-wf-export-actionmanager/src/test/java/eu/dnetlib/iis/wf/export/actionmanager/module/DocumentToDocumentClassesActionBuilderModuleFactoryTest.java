package eu.dnetlib.iis.wf.export.actionmanager.module;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClass;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClasses;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import org.junit.jupiter.api.Test;

import java.util.List;

import static eu.dnetlib.iis.common.InfoSpaceConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author mhorst
 *
 */
public class DocumentToDocumentClassesActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToDocumentClasses, Result> {

    
    // ------------------------ CONSTRUCTORS -----------------------------
    
    public DocumentToDocumentClassesActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToDocumentClassesActionBuilderModuleFactory.class, AlgorithmName.document_classes);
    }

    // ------------------------ TESTS ------------------------------------
    
    @Test
    public void testBuildEmptyClasses() throws Exception {
        // given
        String docId = "documentId";
        ActionBuilderModule<DocumentToDocumentClasses, Result> module =  factory.instantiate(config);
        
        // execute
        List<AtomicAction<Result>> actions = module.build(DocumentToDocumentClasses.newBuilder().setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }
    
    @Test
    public void testBuildBelowThreshold() throws Exception {
        // given
        String docId = "docId";
        float confidenceLevel = 0.4f;
        ActionBuilderModule<DocumentToDocumentClasses, Result> module =  factory.instantiate(config);

        // execute
        List<AtomicAction<Result>> actions =  module.build(buildDocumentToDocumentClasses(docId, confidenceLevel));
        
        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }
    
    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "docId";
        float confidenceLevel = 0.6f;
        ActionBuilderModule<DocumentToDocumentClasses, Result> module =  factory.instantiate(config);

        // execute
        List<AtomicAction<Result>> actions =  module.build(buildDocumentToDocumentClasses(docId, confidenceLevel));
        
        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction<Result> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Result.class, action.getClazz());
        assertOaf(action.getPayload(), docId, confidenceLevel);
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
    
    private void assertOaf(Result result, String docId, float confidenceLevel) throws InvalidProtocolBufferException {
        assertNotNull(result);

        assertEquals(docId, result.getId());
        assertNotNull(result.getSubject());
        assertEquals(5, result.getSubject().size());

        assertSubject("arxiv1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "arxiv2", 
                InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_ARXIV, 
                confidenceLevel, result.getSubject().get(0));
        
        assertSubject("ddc1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "ddc2", 
                InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_DDC, 
                confidenceLevel, result.getSubject().get(1));
        
        assertSubject("wos1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "wos2", 
                InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_WOS, 
                confidenceLevel, result.getSubject().get(2));
        
        assertSubject("mesh1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "mesh2", 
                InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_MESHEUROPMC, 
                confidenceLevel, result.getSubject().get(3));
        
        assertSubject("acm1" + CLASSIFICATION_HIERARCHY_SEPARATOR + "acm2", 
                InfoSpaceConstants.SEMANTIC_CLASS_TAXONOMIES_ACM, 
                confidenceLevel, result.getSubject().get(4));
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
