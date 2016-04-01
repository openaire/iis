define avro_load_metadata AvroStorage('$schema_input');

define avro_store_citation AvroStorage('$schema_output');

define NullToEmptyBag datafu.pig.bags.NullToEmptyBag();
define DeduplicateIdsWithDocumentType eu.dnetlib.iis.wf.citationmatching.direct.udfs.DeduplicateIdsWithDocumentType;

documentMetadata = load '$input' using avro_load_metadata;

-- wygenerowanie mapowań pmid_to_oaid i doi_to_oaid na podstawie inputu
doi_to_oaid_with_nulls = foreach documentMetadata generate externalIdentifiers#'doi' as originalId:chararray, id as newId:chararray;
doi_to_oaid_nondedup = filter doi_to_oaid_with_nulls by originalId is not null;
doi_to_oaid_nondedup_groupped = group doi_to_oaid_nondedup by originalId;
doi_to_oaid = foreach doi_to_oaid_nondedup_groupped {
    first_record = LIMIT doi_to_oaid_nondedup 1;
    generate group as originalId, flatten(first_record.newId) as newId;
}

pmid_to_oaid_with_nulls = foreach documentMetadata generate externalIdentifiers#'pmid' as originalId:chararray, id as newId:chararray, publicationTypeName as publicationTypeName;

-- DEBUG: skipping deduplication (disabled)
-- pmid_to_oaid = filter pmid_to_oaid_with_nulls by originalId is not null;
pmid_to_oaid_nondedup = filter pmid_to_oaid_with_nulls by originalId is not null;
pmid_to_oaid_nondedup_groupped = group pmid_to_oaid_nondedup by originalId;
pmid_to_oaid = foreach pmid_to_oaid_nondedup_groupped {
     idsWithPublicationType = foreach pmid_to_oaid_nondedup generate newId, publicationTypeName;
     dedupIdsWithPublicationType = DeduplicateIdsWithDocumentType(idsWithPublicationType);
     generate group as originalId, flatten(dedupIdsWithPublicationType.newId) as newId;
 }

docWithRefsFlat = foreach documentMetadata generate id, flatten(NullToEmptyBag(references));
docWithBasicMetadataFlat = foreach docWithRefsFlat generate id, flatten(references::externalIds), flatten(references::position);

workingCitation = foreach docWithBasicMetadataFlat generate
	id as sourceId:chararray,
	references::position as position:int,
	null as destinationDocumentId:chararray,
	references::externalIds#'pmid' as pmid:chararray,
	references::externalIds#'doi' as doi:chararray;

-- joining with pmid_to_oaid mappings
joinedWithPmid = join workingCitation by pmid left, pmid_to_oaid by originalId;
workingCitationWithDestIdFromPmid = foreach joinedWithPmid generate
	workingCitation::sourceId as sourceId,
	workingCitation::doi as doi,
	workingCitation::position as position,
	pmid_to_oaid::newId as destinationDocumentId;

-- joining with doi_to_oaid mappings
joinedWithDoi = join workingCitationWithDestIdFromPmid by doi left, doi_to_oaid by originalId;

workingCitationWithDestIdFromPmidAndDoi = foreach joinedWithDoi generate
	workingCitationWithDestIdFromPmid::sourceId as sourceId,
	workingCitationWithDestIdFromPmid::position as position,
-- 	overriding pmid matched citation with doi matched citation if found 
	(doi_to_oaid::newId is not null ? doi_to_oaid::newId : workingCitationWithDestIdFromPmid::destinationDocumentId) as destinationDocumentId;

output_citation = foreach workingCitationWithDestIdFromPmidAndDoi generate
	sourceId as sourceDocumentId, position, destinationDocumentId;

-- accepting only matched citations
output_citation_matched = filter output_citation by destinationDocumentId is not null;

store output_citation_matched into '$output' using avro_store_citation;