package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_ABSTRACT;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_ACRONYM;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_CALLID;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_CITATIONID;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_DOI;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_END_DATE;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_FULLTEXT;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_FUNDING_LEVEL_0;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_FUNDING_LEVEL_1;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_FUNDING_LEVEL_2;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_GRANTID;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_PDBCODE;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_PMCID;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_PROJECTID;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_PUBID;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_REFERENCE;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_START_DATE;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.FIELD_TITLE;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.JOIN_TYPE_INNER;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.JOIN_TYPE_LEFTSEMI;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.JOIN_TYPE_LEFT_OUTER;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.junit.Test;

public class AvroToRdbTransformerUtilsTest {


    //------------------------ TESTS --------------------------
    
    @Test
    public void testFilterProject() throws Exception {
        
        // given
        DataFrame project = mock(DataFrame.class);
        DataFrame projectFiltered = mock(DataFrame.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        Column fundingClassColumn = mock(Column.class);
        String fundingClassWhitelist = "whitelist";

        // filtering specs
        Column fundingClassRLike = mock(Column.class);
        
        when(project.col("fundingClass")).thenReturn(fundingClassColumn);
        when(fundingClassColumn.rlike(fundingClassWhitelist)).thenReturn(fundingClassRLike);
        
        when(project.filter(fundingClassRLike)).thenReturn(projectFiltered);
        
        // selecting specs
        Column projectIdColumn = mock(Column.class);
        Column projectIdColumnAsRdbField = mock(Column.class);        
        when(project.col("id")).thenReturn(projectIdColumn);
        when(projectIdColumn.as(FIELD_PUBID)).thenReturn(projectIdColumnAsRdbField);
        
        Column fundingLevelsColumn = mock(Column.class);
        when(project.col("fundingLevels")).thenReturn(fundingLevelsColumn);
        
        Column fundingLevelsItem0Column = mock(Column.class);
        when(fundingLevelsColumn.getItem(0)).thenReturn(fundingLevelsItem0Column);
        Column fundingLevelsItem0ColumnAsRdbField = mock(Column.class);
        when(fundingLevelsItem0Column.as(FIELD_FUNDING_LEVEL_0)).thenReturn(fundingLevelsItem0ColumnAsRdbField);
        
        Column fundingLevelsItem1Column = mock(Column.class);
        when(fundingLevelsColumn.getItem(1)).thenReturn(fundingLevelsItem1Column);
        Column fundingLevelsItem1ColumnAsRdbField = mock(Column.class);
        when(fundingLevelsItem1Column.as(FIELD_FUNDING_LEVEL_1)).thenReturn(fundingLevelsItem1ColumnAsRdbField);
        
        Column fundingLevelsItem2Column = mock(Column.class);
        when(fundingLevelsColumn.getItem(2)).thenReturn(fundingLevelsItem2Column);
        Column fundingLevelsItem2ColumnAsRdbField = mock(Column.class);
        when(fundingLevelsItem2Column.as(FIELD_FUNDING_LEVEL_2)).thenReturn(fundingLevelsItem2ColumnAsRdbField);
        
        Column projectGrantIdColumn = mock(Column.class);
        Column projectGrantIdColumnAsRdbField = mock(Column.class);        
        when(project.col("projectGrantId")).thenReturn(projectGrantIdColumn);
        when(projectIdColumn.as(FIELD_GRANTID)).thenReturn(projectGrantIdColumnAsRdbField);
        
        Column projectAcronymColumn = mock(Column.class);
        Column projectAcronymColumnAsRdbField = mock(Column.class);        
        when(project.col("projectAcronym")).thenReturn(projectAcronymColumn);
        when(projectIdColumn.as(FIELD_ACRONYM)).thenReturn(projectAcronymColumnAsRdbField);
        
        Column titleColumn = mock(Column.class);
        Column titleColumnAsRdbField = mock(Column.class);        
        when(project.col("title")).thenReturn(titleColumn);
        when(projectIdColumn.as(FIELD_TITLE)).thenReturn(titleColumnAsRdbField);
        
        Column callIdColumn = mock(Column.class);
        Column callIdColumnAsRdbField = mock(Column.class);        
        when(project.col("callId")).thenReturn(callIdColumn);
        when(projectIdColumn.as(FIELD_CALLID)).thenReturn(callIdColumnAsRdbField);
        
        Column startDateColumn = mock(Column.class);
        Column startDateColumnAsRdbField = mock(Column.class);        
        when(project.col("startDate")).thenReturn(startDateColumn);
        when(projectIdColumn.as(FIELD_START_DATE)).thenReturn(startDateColumnAsRdbField);
        
        Column endDateColumn = mock(Column.class);
        Column endDateColumnAsRdbField = mock(Column.class);        
        when(project.col("endDate")).thenReturn(endDateColumn);
        when(projectIdColumn.as(FIELD_END_DATE)).thenReturn(endDateColumnAsRdbField);
        
        //TODO drop all any() occurences and include static explode() method in test execution path
        when(projectFiltered.select(any(Column.class), any(Column.class), 
                any(Column.class), any(Column.class), any(Column.class), any(Column.class), any(Column.class), any(Column.class), any(Column.class), any(Column.class), any(Column.class))).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.filterProject(project, fundingClassWhitelist);
        
        // assert
        assertTrue(result == toBeReturned);
        
    }
    
    @Test
    public void testFilterPubProject() throws Exception {
        
        // given
        DataFrame project = mock(DataFrame.class);
        DataFrame documentToProject = mock(DataFrame.class);
        DataFrame documentJoinedWithProjectDetails = mock(DataFrame.class);
        DataFrame pubGrantFiltered = mock(DataFrame.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        float confidenceLevelThreshold = 0.9f;
        
        // joining specs
        Column documentToProjectIdColumn = mock(Column.class);
        Column projectIdColumn = mock(Column.class);
        Column projectJoinColumn = mock(Column.class);
        
        when(project.col(FIELD_PROJECTID)).thenReturn(projectIdColumn);
        when(documentToProject.col(FIELD_PROJECTID)).thenReturn(documentToProjectIdColumn);
        when(documentToProjectIdColumn.equalTo(projectIdColumn)).thenReturn(projectJoinColumn);
        
        when(documentToProject.join(project, projectJoinColumn, JOIN_TYPE_INNER)).thenReturn(documentJoinedWithProjectDetails);

        // filtering specs
        Column confidenceLevelColumn = mock(Column.class);
        Column confidenceLevelGreaterEq = mock(Column.class);
        
        when(documentJoinedWithProjectDetails.col("confidenceLevel")).thenReturn(confidenceLevelColumn);
        when(confidenceLevelColumn.$greater$eq(confidenceLevelThreshold)).thenReturn(confidenceLevelGreaterEq);

        when(documentJoinedWithProjectDetails.filter(confidenceLevelGreaterEq)).thenReturn(pubGrantFiltered);
        
        // selecting specs
        Column joinedDocumentIdColumnAsRdbField = mock(Column.class);
        Column joinedProjectGrantIdColumnAsRdbField = mock(Column.class);
        Column joinedDocumentIdColumn = mock(Column.class);
        
        when(documentToProject.col("documentId")).thenReturn(joinedDocumentIdColumn);
        when(joinedDocumentIdColumn.as(FIELD_PUBID)).thenReturn(joinedDocumentIdColumnAsRdbField);
        
        when(documentToProjectIdColumn.as(FIELD_PROJECTID)).thenReturn(joinedProjectGrantIdColumnAsRdbField);
        
        when(pubGrantFiltered.select(
                joinedDocumentIdColumnAsRdbField, joinedProjectGrantIdColumnAsRdbField)).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.filterPubProject(documentToProject, project, confidenceLevelThreshold);
        
        // assert
        assertTrue(result == toBeReturned);
    }

    @Test
    public void testFilterMetadata() throws Exception {
        // given
        DataFrame metadata = mock(DataFrame.class);
        DataFrame text = mock(DataFrame.class);
        DataFrame pubGrant = mock(DataFrame.class);
        
        DataFrame metadataSubset = mock(DataFrame.class);
        DataFrame metadataJoinedWithText =  mock(DataFrame.class);
        DataFrame metadataFiltered = mock(DataFrame.class);
        
        // limiting subset of fields specs
        Column metadataIdColumn = mock(Column.class);
        Column metadataPubIdColumn = mock(Column.class);
        Column metadataTitleColumn = mock(Column.class);
        Column metadataAbstractColumn = mock(Column.class);
        Column metadataLanguageColumn = mock(Column.class);
        Column metadataExternalIdsColumn = mock(Column.class);
        Column metadataExternalIdFieldDOIColumn = mock(Column.class);
        Column metadataExternalIdAsDOIColumn = mock(Column.class);
        Column metadataYearColumn = mock(Column.class);
        Column metadataKeywordsColumn = mock(Column.class);
        
        when(metadata.col("id")).thenReturn(metadataIdColumn);
        when(metadataIdColumn.as(FIELD_PUBID)).thenReturn(metadataPubIdColumn);
        
        when(metadata.col(FIELD_TITLE)).thenReturn(metadataTitleColumn);
        when(metadata.col(FIELD_ABSTRACT)).thenReturn(metadataAbstractColumn);
        when(metadata.col("language")).thenReturn(metadataLanguageColumn);
        when(metadata.col("externalIdentifiers")).thenReturn(metadataExternalIdsColumn);
        when(metadataExternalIdsColumn.getField("doi")).thenReturn(metadataExternalIdFieldDOIColumn);
        when(metadataExternalIdFieldDOIColumn.as(FIELD_DOI)).thenReturn(metadataExternalIdAsDOIColumn);
        
        when(metadata.col("year")).thenReturn(metadataYearColumn);
        when(metadata.col("keywords")).thenReturn(metadataKeywordsColumn);
        
        when(metadata.select(metadataPubIdColumn, metadataTitleColumn, metadataAbstractColumn, metadataLanguageColumn, 
                metadataExternalIdAsDOIColumn, metadataYearColumn, metadataKeywordsColumn)).thenReturn(metadataSubset);

        // text joining specs
        Column metadataSubsetPubIdColumn = mock(Column.class);
        Column dedupedTextIdColumn = mock(Column.class);
        Column metadataJoinedWithTextIdColumn = mock(Column.class);
        DataFrame dedupedText = mock(DataFrame.class);
        
        when(text.dropDuplicates(new String[] {"id"})).thenReturn(dedupedText);
        
        when(metadataSubset.col(FIELD_PUBID)).thenReturn(metadataSubsetPubIdColumn);
        when(dedupedText.col("id")).thenReturn(dedupedTextIdColumn);
        when(metadataSubsetPubIdColumn.equalTo(dedupedTextIdColumn)).thenReturn(metadataJoinedWithTextIdColumn);
        
        when(metadataSubset.join(dedupedText, metadataJoinedWithTextIdColumn, JOIN_TYPE_LEFT_OUTER)).thenReturn(metadataJoinedWithText);

        // filtering by metadata specs
        Column languageColumn = mock(Column.class);
        Column languageNullColumn = mock(Column.class);
        Column languageEngColumn = mock(Column.class);
        Column languageValidColumn = mock(Column.class);
        
        Column abstractColumn = mock(Column.class);
        Column abstractNotNullColumn = mock(Column.class);
        Column textColumn = mock(Column.class);
        Column textNotNullColumn = mock(Column.class);
        Column abstractOrTextNotNullColumn = mock(Column.class);
        
        Column filterColumn = mock(Column.class);
        
        when(metadataJoinedWithText.col(FIELD_ABSTRACT)).thenReturn(abstractColumn);
        when(abstractColumn.isNotNull()).thenReturn(abstractNotNullColumn);
        when(metadataJoinedWithText.col("text")).thenReturn(textColumn);
        when(textColumn.isNotNull()).thenReturn(textNotNullColumn);
        when(abstractNotNullColumn.or(textNotNullColumn)).thenReturn(abstractOrTextNotNullColumn);
        
        when(metadataJoinedWithText.col("language")).thenReturn(languageColumn);
        when(languageColumn.isNull()).thenReturn(languageNullColumn);
        when(languageColumn.equalTo("eng")).thenReturn(languageEngColumn);
        when(languageNullColumn.or(languageEngColumn)).thenReturn(languageValidColumn);

        when(abstractOrTextNotNullColumn.and(languageValidColumn)).thenReturn(filterColumn);
        
        when(metadataJoinedWithText.filter(filterColumn)).thenReturn(metadataFiltered);
        
        // filtering by pubgrant relation specs
        Column metadataFilteredPubIdColumn = mock(Column.class);
        Column pubGrantPubIdColumn = mock(Column.class);
        Column joinColumn = mock(Column.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        when(metadataFiltered.col(FIELD_PUBID)).thenReturn(metadataFilteredPubIdColumn);
        when(pubGrant.col(FIELD_PUBID)).thenReturn(pubGrantPubIdColumn);
        when(metadataFilteredPubIdColumn.equalTo(pubGrantPubIdColumn)).thenReturn(joinColumn);
        when(metadataFiltered.join(pubGrant, joinColumn, JOIN_TYPE_LEFTSEMI)).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.filterMetadata(metadata, text, pubGrant);
        
        // assert
        assertTrue(result == toBeReturned);
    }
    
    @Test
    public void testBuildPubKeyword() throws Exception {
        // given
        Column metadataPubIdColumn = mock(Column.class);
        Column metadataKeywordsColumn = mock(Column.class);
        DataFrame metadata = mock(DataFrame.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        when(metadata.col(FIELD_PUBID)).thenReturn(metadataPubIdColumn);
        when(metadata.col("keywords")).thenReturn(metadataKeywordsColumn);
        //TODO drop both any() occurences and include static explode() method in test execution path
        when(metadata.select(any(Column.class), any(Column.class))).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.buildPubKeyword(metadata);
        
        // assert
        assertTrue(result == toBeReturned);
    }
    
    @Test
    public void testBuildPubFulltext() throws Exception {
        // given
        Column metadataPubIdColumn = mock(Column.class);
        Column metadataTextColumn = mock(Column.class);
        Column metadataTextNotNullColumn = mock(Column.class);
        Column metadataFullextColumn = mock(Column.class);
        DataFrame metadata = mock(DataFrame.class);
        DataFrame metadataFiltered = mock(DataFrame.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        when(metadata.col(FIELD_PUBID)).thenReturn(metadataPubIdColumn);
        when(metadata.col("text")).thenReturn(metadataTextColumn);
        when(metadataTextColumn.isNotNull()).thenReturn(metadataTextNotNullColumn);
        when(metadataTextColumn.as(FIELD_FULLTEXT)).thenReturn(metadataFullextColumn);
        
        when(metadata.filter(metadataTextNotNullColumn)).thenReturn(metadataFiltered);
        when(metadataFiltered.select(metadataPubIdColumn, metadataFullextColumn)).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.buildPubFulltext(metadata);
        
        // assert
        assertTrue(result == toBeReturned);
    }
    
    
    @Test
    public void testFilterCitation() throws Exception {
        // given
        float confidenceLevelThreshold = 0.9f;
        
        DataFrame citation = mock(DataFrame.class);
        DataFrame publicationId = mock(DataFrame.class);
        
        DataFrame filtered = mock(DataFrame.class);
        DataFrame selected = mock(DataFrame.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        // filtering specs
        Column destinationDocumentIdColumn = mock(Column.class);
        Column destinationDocumentIdNotNullColumn = mock(Column.class);
        Column confidenceLevelColumn = mock(Column.class);
        Column confidenceLevelGreaterEqColumn = mock(Column.class);
        Column filterColumn = mock(Column.class);
        
        when(citation.col("entry.destinationDocumentId")).thenReturn(destinationDocumentIdColumn);
        when(destinationDocumentIdColumn.isNotNull()).thenReturn(destinationDocumentIdNotNullColumn);
        when(citation.col("entry.confidenceLevel")).thenReturn(confidenceLevelColumn);
        when(confidenceLevelColumn.$greater$eq(confidenceLevelThreshold)).thenReturn(confidenceLevelGreaterEqColumn);
        when(destinationDocumentIdNotNullColumn.and(confidenceLevelGreaterEqColumn)).thenReturn(filterColumn);
        when(citation.filter(filterColumn)).thenReturn(filtered);
        
        // selecting specs
        Column sourceDocumentIdColumn = mock(Column.class);
        Column citationPubIdColumn = mock(Column.class);
        Column citationIdColumn = mock(Column.class);
        Column rawTextColumn = mock(Column.class);
        Column referenceColumn = mock(Column.class);
        
        when(citation.col("sourceDocumentId")).thenReturn(sourceDocumentIdColumn);
        when(sourceDocumentIdColumn.as(FIELD_PUBID)).thenReturn(citationPubIdColumn);
        when(destinationDocumentIdColumn.as(FIELD_CITATIONID)).thenReturn(citationIdColumn);
        when(citation.col("entry.rawText")).thenReturn(rawTextColumn);
        when(rawTextColumn.as(FIELD_REFERENCE)).thenReturn(referenceColumn);
        when(filtered.select(citationPubIdColumn, citationIdColumn, referenceColumn)).thenReturn(selected);
        
        // filtering by publication id specs
        Column selectedCitationPubIdColumn = mock(Column.class);
        Column publicationIdPubIdColumn = mock(Column.class);
        Column joinColumn = mock(Column.class);

        when(selected.col(FIELD_PUBID)).thenReturn(selectedCitationPubIdColumn);
        when(publicationId.col(FIELD_PUBID)).thenReturn(publicationIdPubIdColumn);
        when(selectedCitationPubIdColumn.equalTo(publicationIdPubIdColumn)).thenReturn(joinColumn);
        when(selected.join(publicationId, joinColumn, JOIN_TYPE_LEFTSEMI)).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.filterCitation(citation, publicationId, confidenceLevelThreshold);
        
        // assert
        assertTrue(result == toBeReturned);
    }
    
    @Test
    public void testBuildPubCitation() throws Exception {
        // given
        Column metadataPubIdColumn = mock(Column.class);
        Column metadataCitationIdColumn = mock(Column.class);
        
        DataFrame citation = mock(DataFrame.class);
        DataFrame citationSelected = mock(DataFrame.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        when(citation.col(FIELD_PUBID)).thenReturn(metadataPubIdColumn);
        when(citation.col(FIELD_CITATIONID)).thenReturn(metadataCitationIdColumn);

        when(citation.select(metadataPubIdColumn, metadataCitationIdColumn)).thenReturn(citationSelected);
        when(citationSelected.dropDuplicates()).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.buildPubCitation(citation);
        
        // assert
        assertTrue(result == toBeReturned);
    }
    
    @Test
    public void testBuildPubPDBCodes() throws Exception {
        // given
        float confidenceLevelThreshold = 0.9f;
        
        DataFrame documentToPdb = mock(DataFrame.class);
        DataFrame publicationId = mock(DataFrame.class);
        DataFrame filteredByPubId = mock(DataFrame.class);
        DataFrame filteredByConfidenceLevel = mock(DataFrame.class);
        DataFrame selected = mock(DataFrame.class);
        DataFrame toBeReturned = mock(DataFrame.class);
        
        // filtering by publication id specs
        Column documentToPdbDocumentIdColumn = mock(Column.class);
        Column publicationIdPubIdColumn = mock(Column.class);
        Column joinColumn = mock(Column.class);
        
        when(documentToPdb.col("documentId")).thenReturn(documentToPdbDocumentIdColumn);
        when(publicationId.col(FIELD_PUBID)).thenReturn(publicationIdPubIdColumn);
        when(documentToPdbDocumentIdColumn.equalTo(publicationIdPubIdColumn)).thenReturn(joinColumn);
        when(documentToPdb.join(publicationId, joinColumn, JOIN_TYPE_LEFTSEMI)).thenReturn(filteredByPubId);

        // filtering by confidence level specs
        Column filteredConfidenceLevelColumn = mock(Column.class);
        Column filteredConfidenceLevelGreaterEqColumn = mock(Column.class);
        
        when(filteredByPubId.col("confidenceLevel")).thenReturn(filteredConfidenceLevelColumn);
        when(filteredConfidenceLevelColumn.$greater$eq(confidenceLevelThreshold)).thenReturn(filteredConfidenceLevelGreaterEqColumn);
        when(filteredByPubId.filter(filteredConfidenceLevelGreaterEqColumn)).thenReturn(filteredByConfidenceLevel);
        
        // selecting specs
        Column filteredDocumentIdColumn = mock(Column.class);
        Column filteredConceptIdColumn = mock(Column.class);
        Column filteredPmcIdColumn = mock(Column.class);
        Column filteredPdbCodeColumn = mock(Column.class);
        
        when(filteredByPubId.col("documentId")).thenReturn(filteredDocumentIdColumn);
        when(filteredDocumentIdColumn.as(FIELD_PMCID)).thenReturn(filteredPmcIdColumn);
        when(filteredByPubId.col("conceptId")).thenReturn(filteredConceptIdColumn);
        when(filteredConceptIdColumn.as(FIELD_PDBCODE)).thenReturn(filteredPdbCodeColumn);
        when(filteredByConfidenceLevel.select(filteredPmcIdColumn, filteredPdbCodeColumn)).thenReturn(selected);
        
        // distinct specs
        when(selected.distinct()).thenReturn(toBeReturned);
        
        // execute
        DataFrame result = AvroToRdbTransformerUtils.buildPubPDBCodes(documentToPdb, publicationId, confidenceLevelThreshold);
        
        // assert
        assertTrue(result == toBeReturned);
    }
    
}
