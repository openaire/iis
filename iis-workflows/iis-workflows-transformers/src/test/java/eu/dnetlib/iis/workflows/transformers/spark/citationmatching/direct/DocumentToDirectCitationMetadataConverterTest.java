package eu.dnetlib.iis.workflows.transformers.spark.citationmatching.direct;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.PublicationType;

/**
 * 
 * @author madryk
 *
 */
public class DocumentToDirectCitationMetadataConverterTest {

	private DocumentToDirectCitationMetadataConverter converter = new DocumentToDirectCitationMetadataConverter();
	
	
	private ExtractedDocumentMetadataMergedWithOriginal.Builder baseDocMetadataBuilder;
	
	private DocumentMetadata.Builder baseDocDirectCitationMetadataBuilder;
	
	
	@Before
	public void setUp() {
		baseDocMetadataBuilder = ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
				.setId("id-1")
				.setExternalIdentifiers(buildExternalIds("doc_id_key_1", "doc_id_key_2"))
				.setPublicationTypeName("typeName")
				.setReferences(Lists.newArrayList(buildReferenceMetadata(23, buildExternalIds("ref_id_key_1", "ref_id_key_2"))))
				.setPublicationType(new PublicationType(false, false));
		
		
		baseDocDirectCitationMetadataBuilder = DocumentMetadata.newBuilder()
				.setId("id-1")
				.setExternalIdentifiers(buildExternalIds("doc_id_key_1", "doc_id_key_2"))
				.setPublicationTypeName("typeName")
				.setReferences(Lists.newArrayList(buildCitationReferenceMetadata(23, buildExternalIds("ref_id_key_1", "ref_id_key_2"))));
	}
	
	
	//------------------------ TESTS --------------------------
	
	@Test
	public void convert() {
		
		// given
		
		ExtractedDocumentMetadataMergedWithOriginal docMetadata = baseDocMetadataBuilder.build();
		
		
		// execute
		
		DocumentMetadata retDocDirectCitationMetadata = converter.convert(docMetadata);
		
		
		// assert
		
		DocumentMetadata expectedDocDirectCitationMetadata = baseDocDirectCitationMetadataBuilder.build();
		
		assertEquals(expectedDocDirectCitationMetadata, retDocDirectCitationMetadata);
	}
	
	
	@Test
	public void convert_NULL_EXTERNAL_IDENTIFIERS() {

		// given
		
		ExtractedDocumentMetadataMergedWithOriginal docMetadata = baseDocMetadataBuilder
				.clearExternalIdentifiers()
				.build();
		
		
		// execute
		
		DocumentMetadata retDocDirectCitationMetadata = converter.convert(docMetadata);
		
		
		// assert
		
		DocumentMetadata expectedDocDirectCitationMetadata = baseDocDirectCitationMetadataBuilder
				.clearExternalIdentifiers()
				.build();
		
		assertEquals(expectedDocDirectCitationMetadata, retDocDirectCitationMetadata);
	}
	
	
	@Test
	public void convert_NULL_REFERENCES() {

		// given
		
		ExtractedDocumentMetadataMergedWithOriginal docMetadata = baseDocMetadataBuilder
				.clearReferences()
				.build();
		
		
		// execute
		
		DocumentMetadata retDocDirectCitationMetadata = converter.convert(docMetadata);
		
		
		// assert
		
		DocumentMetadata expectedDocDirectCitationMetadata = baseDocDirectCitationMetadataBuilder
				.setReferences(Lists.newArrayList())
				.build();
		
		assertEquals(expectedDocDirectCitationMetadata, retDocDirectCitationMetadata);
	}
	
	
	@Test
	public void conver_NULL_REFERENCES_EXTERNAL_IDS() {

		// given
		
		ExtractedDocumentMetadataMergedWithOriginal docMetadata = baseDocMetadataBuilder
				.setReferences(Lists.newArrayList(buildReferenceMetadata(24, null)))
				.build();
		
		
		// execute
		
		DocumentMetadata retDocDirectCitationMetadata = converter.convert(docMetadata);
		
		
		// assert
		
		DocumentMetadata expectedDocDirectCitationMetadata = baseDocDirectCitationMetadataBuilder
				.setReferences(Lists.newArrayList())
				.build();
		
		assertEquals(expectedDocDirectCitationMetadata, retDocDirectCitationMetadata);
	}
	
	
	@Test
	public void conver_EMPTY_REFERENCES_EXTERNAL_IDS() {

		// given
		
		ExtractedDocumentMetadataMergedWithOriginal docMetadata = baseDocMetadataBuilder
				.setReferences(Lists.newArrayList(buildReferenceMetadata(24, buildExternalIds())))
				.build();
		
		
		// execute
		
		DocumentMetadata retDocDirectCitationMetadata = converter.convert(docMetadata);
		
		
		// assert
		
		DocumentMetadata expectedDocDirectCitationMetadata = baseDocDirectCitationMetadataBuilder
				.setReferences(Lists.newArrayList())
				.build();
		
		assertEquals(expectedDocDirectCitationMetadata, retDocDirectCitationMetadata);
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private ReferenceMetadata buildReferenceMetadata(Integer position, Map<CharSequence, CharSequence> externalIds) {
		ReferenceBasicMetadata referenceBasicMetadata = ReferenceBasicMetadata.newBuilder()
				.setExternalIds(externalIds)
				.build();
		
		ReferenceMetadata referenceMetadata = ReferenceMetadata.newBuilder()
				.setPosition(position)
				.setBasicMetadata(referenceBasicMetadata)
				.build();
		
		return referenceMetadata;
	}
	
	private eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata buildCitationReferenceMetadata(Integer position, Map<CharSequence, CharSequence> externalIdKeys) {
		return eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata.newBuilder()
				.setPosition(position)
				.setExternalIds(externalIdKeys)
				.build();
		
	}
	
	private Map<CharSequence, CharSequence> buildExternalIds(String ... idKeys) {
		return Maps.asMap(Sets.newHashSet(idKeys), x -> "value_for_" + x);
	}
}
