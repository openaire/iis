package eu.dnetlib.iis.wf.export.actionmanager.module;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.ExternalReference;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.module.DocumentToPdbActionBuilderModuleFactory.EXPORT_PDB_URL_ROOT;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class DocumentToPdbActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToConceptIds, Result> {

    private static final String ROOT_URL = "http://some_root_url/";
    
    
    // ------------------------ CONSTRUCTORS -----------------------------
    
    public DocumentToPdbActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToPdbActionBuilderModuleFactory.class, AlgorithmName.document_pdb);
    }

    @BeforeEach
    public void initUrlRootParam() {
        config.set(EXPORT_PDB_URL_ROOT, ROOT_URL);
    }
    
    // ------------------------ TESTS ------------------------------------
    
    @Test
    public void testBuildEmptyConcepts() throws Exception {
     // given
        String docId = "documentId";
        ActionBuilderModule<DocumentToConceptIds, Result> module =  factory.instantiate(config);
        
        // execute
        List<AtomicAction<Result>> actions = module.build(
                DocumentToConceptIds.newBuilder().setConcepts(Collections.emptyList()).setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }

    @Test
    public void testBuildWithoutRootUrl() {
     // given
        String docId = "documentId";
        String pdbId = "pdbId";
        float confidenceLevel = 1f;
        config.unset(EXPORT_PDB_URL_ROOT);
        ActionBuilderModule<DocumentToConceptIds, Result> module =  factory.instantiate(config);
        
        // execute
        assertThrows(RuntimeException.class, () -> module.build(buildDocumentToConceptIds(docId, pdbId, confidenceLevel)));
    }

    
    @Test
    public void testBuild() throws Exception {
     // given
        String docId = "documentId";
        String pdbId = "pdbId";
        float confidenceLevel = 1f;
        ActionBuilderModule<DocumentToConceptIds, Result> module =  factory.instantiate(config);
        
        // execute
        List<AtomicAction<Result>> actions = module.build(buildDocumentToConceptIds(docId, pdbId, confidenceLevel));

        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction<Result> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Result.class, action.getClazz());
        assertOaf(action.getPayload(), docId, pdbId, confidenceLevel);
    }
    

    // ----------------------- PRIVATE --------------------------

    private static DocumentToConceptIds buildDocumentToConceptIds(String docId, String pdbId, float confidenceLevel) {
        DocumentToConceptIds.Builder builder = DocumentToConceptIds.newBuilder();
        builder.setDocumentId(docId);
        Concept concept = Concept.newBuilder().setId(pdbId).setConfidenceLevel(confidenceLevel).build();
        builder.setConcepts(Lists.newArrayList(concept));
        return builder.build();
    }

    private void assertOaf(Result result, String docId, String pdbId, float confidenceLevel) throws InvalidProtocolBufferException {
        assertNotNull(result);

        assertEquals(docId, result.getId());
        assertNotNull(result.getExternalReference());
        assertEquals(1, result.getExternalReference().size());
        ExternalReference externalRef = result.getExternalReference().get(0);
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
