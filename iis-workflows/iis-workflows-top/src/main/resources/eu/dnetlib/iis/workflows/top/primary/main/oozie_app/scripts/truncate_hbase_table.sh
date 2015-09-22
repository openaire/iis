#!/bin/bash

import_hbase_input_table=$1

echo "truncating table $import_hbase_input_table"

echo "disable '$import_hbase_input_table'; drop '$import_hbase_input_table'; create '$import_hbase_input_table', 'datasource', 'datasourceOrganization_provision_isProvidedBy', 'datasourceOrganization_provision_provides', 'organization', 'organizationOrganization_dedupSimilarity_isSimilarTo', 'organizationOrganization_dedup_isMergedIn', 'organizationOrganization_dedup_merges', 'person', 'personPerson_coauthorship_isCoauthorOf', 'personPerson_dedupSimilarity_isSimilarTo', 'personPerson_dedup_isMergedIn', 'personPerson_dedup_merges', 'personResult_authorship_hasAuthor', 'personResult_authorship_isAuthorOf', 'project', 'projectOrganization_participation_hasParticipant', 'projectOrganization_participation_isParticipant', 'projectPerson_contactPerson_hasContact', 'projectPerson_contactPerson_isContact', 'result', 'resultProject_outcome_isProducedBy', 'resultProject_outcome_produces', 'resultResult_dedupSimilarity_isSimilarTo', 'resultResult_dedup_isMergedIn', 'resultResult_dedup_merges', 'resultResult_publicationDataset_isRelatedTo', 'resultResult_similarity_hasAmongTopNSimilarDocuments', 'resultResult_similarity_isAmongTopNSimilarDocuments'" | hbase shell

