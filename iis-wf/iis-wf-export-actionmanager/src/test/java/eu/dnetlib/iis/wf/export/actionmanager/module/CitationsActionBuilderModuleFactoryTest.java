package eu.dnetlib.iis.wf.export.actionmanager.module;

import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.citations.TypedId;
import eu.dnetlib.iis.common.model.extrainfo.converter.CitationsExtraInfoConverter;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.Map.Entry;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 * 
 */
public class CitationsActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<Citations, Result> {


    private String docId = "documentId";

    
    // ----------------------- CONSTRUCTORS -------------------
    
    
    public CitationsActionBuilderModuleFactoryTest() throws Exception {
        super(CitationsActionBuilderModuleFactory.class, AlgorithmName.document_referencedDocuments);
    }

    // ----------------------- TESTS --------------------------

    @Test
    public void testBuildEmptyCitations() throws Exception {
        // given
        ActionBuilderModule<Citations, Result> module =  factory.instantiate(config);
        
        // execute
        List<AtomicAction<Result>> actions = module.build(
                Citations.newBuilder().setCitations(Collections.emptyList()).setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }
    
    @Test
    public void testBuild() throws Exception {
        // given
        ActionBuilderModule<Citations, Result> module =  factory.instantiate(config);
        CitationEntry citationEntry = buildCitationEntry();
        Citations.Builder builder = Citations.newBuilder();
        builder.setDocumentId(docId);
        builder.setCitations(Lists.newArrayList(citationEntry));
        
        // execute
        List<AtomicAction<Result>> actions = module.build(builder.build());

        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction<Result> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Result.class, action.getClazz());
        assertOaf(action.getPayload(), citationEntry);
    }

    @Test
    public void testConversion() {
        SortedSet<BlobCitationEntry> sortedCitations = new TreeSet<BlobCitationEntry>();
        
        CitationEntry.Builder rawTextCitationEntryBuilder = CitationEntry.newBuilder();
        rawTextCitationEntryBuilder.setPosition(44);
        rawTextCitationEntryBuilder.setRawText("[44] S. Mukhi and R. Nigam, “Constraints on ’rare’ dyon decays,” JHEP 12 (2008) 056, 0809.1157.");
        rawTextCitationEntryBuilder.setExternalDestinationDocumentIds(Collections.<CharSequence,CharSequence>emptyMap());
        
        CitationEntry.Builder internalCitationEntryBuilder = CitationEntry.newBuilder();
        internalCitationEntryBuilder.setRawText("Rugama, Y., Kloosterman, J. L., Winkelman, A., 2004. Prog. Nucl. Energy 44, 1-12.");
        internalCitationEntryBuilder.setPosition(100);
        internalCitationEntryBuilder.setDestinationDocumentId("od______2367::00247be440c2188b82d5905b5b1e22bb");
        internalCitationEntryBuilder.setConfidenceLevel(0.8f);
        internalCitationEntryBuilder.setExternalDestinationDocumentIds(Collections.<CharSequence,CharSequence>emptyMap());
        
        CitationEntry.Builder externalPmidCitationEntryBuilder = CitationEntry.newBuilder();
        externalPmidCitationEntryBuilder.setRawText("[5] A. Sen, “Walls of Marginal Stability and Dyon Spectrum in N=4 Supersymmetric String Theories,” JHEP 05 (2007) 039, hep-th/0702141.");
        externalPmidCitationEntryBuilder.setPosition(5);
        Map<CharSequence, CharSequence> externalPmidDestinationDocumentIds = new HashMap<CharSequence, CharSequence>();
        externalPmidDestinationDocumentIds.put("pmid", "20856923");
        externalPmidCitationEntryBuilder.setExternalDestinationDocumentIds(externalPmidDestinationDocumentIds);
        
        CitationEntry.Builder externalDoiCitationEntryBuilder = CitationEntry.newBuilder();
        externalDoiCitationEntryBuilder.setRawText("[17] N. Koblitz. Hyperelliptic cryptosystems. J. Cryptology, 1(3):139–150, 1989.");
        externalDoiCitationEntryBuilder.setPosition(17);
        Map<CharSequence, CharSequence> externalDoiDestinationDocumentIds = new HashMap<CharSequence, CharSequence>();
        externalDoiDestinationDocumentIds.put("doi", "10.1186/1753-6561-5-S6-P38");
        externalDoiDestinationDocumentIds.put("custom-id", "12345");
        externalDoiCitationEntryBuilder.setExternalDestinationDocumentIds(externalDoiDestinationDocumentIds);
        
        sortedCitations.add(CitationsActionBuilderModuleUtils.build(internalCitationEntryBuilder.build()));
        sortedCitations.add(CitationsActionBuilderModuleUtils.build(externalPmidCitationEntryBuilder.build()));
        sortedCitations.add(CitationsActionBuilderModuleUtils.build(externalDoiCitationEntryBuilder.build()));
        sortedCitations.add(CitationsActionBuilderModuleUtils.build(rawTextCitationEntryBuilder.build()));
        
        CitationsExtraInfoConverter converter = new CitationsExtraInfoConverter();
        String citationsXML = converter.serialize(sortedCitations);
        
//      checking deserialization
        SortedSet<BlobCitationEntry> deserializedCitations = (SortedSet<BlobCitationEntry>) converter.deserialize(citationsXML);
        assertEquals(sortedCitations.size(), deserializedCitations.size());
        Iterator<BlobCitationEntry> sortedIt = sortedCitations.iterator();
        Iterator<BlobCitationEntry> deserializedIt = deserializedCitations.iterator();
        for (int i=0; i<sortedCitations.size(); i++) {
            assertEquals(sortedIt.next(), deserializedIt.next());
        }
    }
    
    // ----------------------- PRIVATE --------------------------

    private CitationEntry buildCitationEntry() {
        CitationEntry.Builder citationEntryBuilder = CitationEntry.newBuilder();
        citationEntryBuilder.setPosition(1);
        citationEntryBuilder.setRawText("citation raw text");
        citationEntryBuilder.setDestinationDocumentId("50|dest-id");
        Map<CharSequence, CharSequence> extIds = new HashMap<>();
        extIds.put("extIdType", "extIdValue");
        citationEntryBuilder.setExternalDestinationDocumentIds(extIds);
        citationEntryBuilder.setConfidenceLevel(0.9f);
        return citationEntryBuilder.build();
    }

    private void assertOaf(Result result, CitationEntry sourceEntry) {
        assertNotNull(result);
        assertEquals(docId, result.getId());
        assertNotNull(result.getExtraInfo());
        assertEquals(1, result.getExtraInfo().size());
        ExtraInfo extraInfo = result.getExtraInfo().get(0);
        assertNotNull(extraInfo);
        assertEquals(ExtraInfoConstants.NAME_CITATIONS, extraInfo.getName());
        assertEquals(ExtraInfoConstants.TYPOLOGY_CITATIONS, extraInfo.getTypology());
        assertEquals(StaticConfigurationProvider.ACTION_TRUST_0_9, extraInfo.getTrust());
        assertEquals(((AbstractActionBuilderFactory<Citations, Result>) factory).buildInferenceProvenance(), extraInfo.getProvenance());

        assertTrue(StringUtils.isNotBlank(extraInfo.getValue()));
        CitationsExtraInfoConverter converter = new CitationsExtraInfoConverter();
        Set<BlobCitationEntry> citationEntries = converter.deserialize(extraInfo.getValue());
        assertNotNull(citationEntries);
        assertEquals(1, citationEntries.size());
        BlobCitationEntry citationEntry = citationEntries.iterator().next();
        assertNotNull(citationEntry);
        assertEquals(sourceEntry.getPosition().intValue(), citationEntry.getPosition());
        assertEquals(sourceEntry.getRawText(), citationEntry.getRawText());
        List<TypedId> ids = citationEntry.getIdentifiers();
        assertNotNull(ids);
        assertEquals(2, ids.size());
        assertEquals(ExtraInfoConstants.CITATION_TYPE_OPENAIRE, ids.get(0).getType());
        assertEquals(sourceEntry.getDestinationDocumentId(), ids.get(0).getValue());
        Entry<CharSequence, CharSequence> sourceExtIdEntry = sourceEntry.getExternalDestinationDocumentIds()
                .entrySet().iterator().next();
        assertEquals(sourceExtIdEntry.getKey(), ids.get(1).getType());
        assertEquals(sourceExtIdEntry.getValue(), ids.get(1).getValue());
    }
    
}
