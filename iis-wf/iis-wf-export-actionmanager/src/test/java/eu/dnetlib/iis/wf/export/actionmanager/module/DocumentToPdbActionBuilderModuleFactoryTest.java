package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.module.DocumentToPdbActionBuilderModuleFactory.EXPORT_PDB_URL_ROOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.ResultProtos.Result.ExternalReference;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;

/**
 * @author mhorst
 *
 */
public class DocumentToPdbActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToConceptIds> {

    private static final String ROOT_URL = "http://some_root_url/";
    
    
    // ------------------------ CONSTRUCTORS -----------------------------
    
    public DocumentToPdbActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToPdbActionBuilderModuleFactory.class, AlgorithmName.document_pdb);
    }

    @Before
    public void initUrlRootParam() {
        config.set(EXPORT_PDB_URL_ROOT, ROOT_URL);
    }
    
    // ------------------------ TESTS ------------------------------------
    
    @Test
    public void testBuildEmptyConcepts() throws Exception {
     // given
        String docId = "documentId";
        ActionBuilderModule<DocumentToConceptIds> module =  factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(
                DocumentToConceptIds.newBuilder().setConcepts(Collections.emptyList()).setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }

    @Test(expected=RuntimeException.class)
    public void testBuildWithoutRootUrl() throws Exception {
     // given
        String docId = "documentId";
        String pdbId = "pdbId";
        float confidenceLevel = 1f;
        config.unset(EXPORT_PDB_URL_ROOT);
        ActionBuilderModule<DocumentToConceptIds> module =  factory.instantiate(config, agent, actionSetId);
        
        // execute
        module.build(buildDocumentToConceptIds(docId, pdbId, confidenceLevel));
    }

    
    @Test
    public void testBuild() throws Exception {
     // given
        String docId = "documentId";
        String pdbId = "pdbId";
        float confidenceLevel = 1f;
        ActionBuilderModule<DocumentToConceptIds> module =  factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(buildDocumentToConceptIds(docId, pdbId, confidenceLevel));

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
        assertOaf(action.getTargetValue(), docId, pdbId, confidenceLevel);
    }
    

    // ----------------------- PRIVATE --------------------------

    private static DocumentToConceptIds buildDocumentToConceptIds(String docId, String pdbId, float confidenceLevel) {
        DocumentToConceptIds.Builder builder = DocumentToConceptIds.newBuilder();
        builder.setDocumentId(docId);
        Concept concept = Concept.newBuilder().setId(pdbId).setConfidenceLevel(confidenceLevel).build();
        builder.setConcepts(Lists.newArrayList(concept));
        return builder.build();
    }

    private void assertOaf(byte[] oafBytes, String docId, String pdbId, float confidenceLevel) throws InvalidProtocolBufferException {
        assertNotNull(oafBytes);
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.mergeFrom(oafBytes);
        Oaf oaf = oafBuilder.build();
        assertNotNull(oaf);

        assertTrue(KindProtos.Kind.entity == oaf.getKind());
        assertNotNull(oaf.getEntity());
        assertEquals(docId, oaf.getEntity().getId());
        assertNotNull(oaf.getEntity().getResult());
        assertEquals(1, oaf.getEntity().getResult().getExternalReferenceCount());
        ExternalReference externalRef = oaf.getEntity().getResult().getExternalReferenceList().get(0);
        assertNotNull(externalRef);
        
        assertEquals(DocumentToPdbActionBuilderModuleFactory.SITENAME, externalRef.getSitename());
        assertEquals(pdbId, externalRef.getRefidentifier());
        assertEquals(ROOT_URL + pdbId, externalRef.getUrl());
        
        assertNotNull(externalRef.getQualifier());
        assertEquals(DocumentToPdbActionBuilderModuleFactory.CLASS, externalRef.getQualifier().getClassid());
        assertEquals(DocumentToPdbActionBuilderModuleFactory.CLASS, externalRef.getQualifier().getClassname());
        assertEquals(DocumentToPdbActionBuilderModuleFactory.SCHEME, externalRef.getQualifier().getSchemeid());
        assertEquals(DocumentToPdbActionBuilderModuleFactory.SCHEME, externalRef.getQualifier().getSchemename());
        
        assertNotNull(externalRef.getDataInfo());

        float normalizedTrust = confidenceLevel * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(externalRef.getDataInfo().getTrust()), 0.0001);
    }
}
