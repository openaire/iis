package eu.dnetlib.iis.wf.export.actionmanager.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.ResultProtos.Result.ExternalReference;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.export.schemas.DocumentToSoftwareUrls;
import eu.dnetlib.iis.export.schemas.SoftwareUrl;

/**
 * @author mhorst
 *
 */
public class DocumentToSoftwareUrlActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToSoftwareUrls> {

    private final String docId = "documentId";

    private final String softwareUrl = "https://github.com/openaire/iis";
    
    private final String repositoryName = "GitHub";

    private final float matchStrength = 0.9f;

    
    // ----------------------- CONSTRUCTORS --------------------------    
    
    public DocumentToSoftwareUrlActionBuilderModuleFactoryTest()  throws Exception {
        super(DocumentToSoftwareUrlActionBuilderModuleFactory.class, AlgorithmName.document_software_url);
    }


    // ----------------------- TESTS ---------------------------------

    @Test(expected = TrustLevelThresholdExceededException.class)
    public void testBuildBelowThreshold() throws Exception {
        // given
        DocumentToSoftwareUrls documentToSoftwareBelowThreshold = buildDocumentToSoftwareUrl(
                docId, softwareUrl, repositoryName, 0.4f);
        ActionBuilderModule<DocumentToSoftwareUrls> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        module.build(documentToSoftwareBelowThreshold);
    }

    @Test
    public void testBuildEmptyReferences() throws Exception {
     // given
        String docId = "documentId";
        ActionBuilderModule<DocumentToSoftwareUrls> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(
                DocumentToSoftwareUrls.newBuilder().setSoftwareUrls(Collections.emptyList()).setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }
    
    @Test
    public void testBuild() throws Exception {
        // given
        ActionBuilderModule<DocumentToSoftwareUrls> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(buildDocumentToSoftwareUrl(docId, softwareUrl, repositoryName, matchStrength));

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
        assertOaf(action.getTargetValue());
    }

    // ----------------------- PRIVATE --------------------------

    private static DocumentToSoftwareUrls buildDocumentToSoftwareUrl(String docId, String softUrl, 
            String repositoryName, float confidenceLevel) {
        DocumentToSoftwareUrls.Builder builder = DocumentToSoftwareUrls.newBuilder();
        builder.setDocumentId(docId);
        SoftwareUrl.Builder softBuilder = SoftwareUrl.newBuilder();
        softBuilder.setSoftwareUrl(softUrl);
        softBuilder.setRepositoryName(repositoryName);
        softBuilder.setConfidenceLevel(confidenceLevel);
        builder.setSoftwareUrls(Lists.newArrayList(softBuilder.build()));
        return builder.build();
    }

    private void assertOaf(byte[] oafBytes) throws InvalidProtocolBufferException {
        assertNotNull(oafBytes);
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.mergeFrom(oafBytes);
        Oaf oaf = oafBuilder.build();
        assertNotNull(oaf);

        assertTrue(KindProtos.Kind.entity == oaf.getKind());
        assertNotNull(oaf.getEntity());
        assertEquals(docId, oaf.getEntity().getId());
        assertNotNull(oaf.getEntity().getResult());
        assertEquals(1, oaf.getEntity().getResult().getExternalReferenceList().size());
        ExternalReference externalReference = oaf.getEntity().getResult().getExternalReferenceList().get(0);
        assertNotNull(externalReference);
        assertEquals(softwareUrl, externalReference.getUrl());
        assertEquals(repositoryName, externalReference.getSitename());
        
        assertNotNull(externalReference.getQualifier());
        assertNotNull(externalReference.getDataInfo());

        float normalizedTrust = matchStrength * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(externalReference.getDataInfo().getTrust()), 0.0001);
    }
}
