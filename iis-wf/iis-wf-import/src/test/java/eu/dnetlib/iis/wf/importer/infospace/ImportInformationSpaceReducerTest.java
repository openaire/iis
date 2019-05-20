package eu.dnetlib.iis.wf.importer.infospace;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_INFERENCE_PROVENANCE_BLACKLIST;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_MERGE_BODY_WITH_UPDATES;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SKIP_DELETED_BY_INFERENCE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_TRUST_LEVEL_THRESHOLD;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceReducer.OUTPUT_NAME_DEDUP_MAPPING;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceReducer.OUTPUT_NAME_DOCUMENT_META;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceReducer.OUTPUT_NAME_DATASET_META;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceReducer.OUTPUT_NAME_DOCUMENT_PROJECT;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceReducer.OUTPUT_NAME_ORGANIZATION;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceReducer.OUTPUT_NAME_PROJECT;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceReducer.OUTPUT_NAME_PROJECT_ORGANIZATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;
import com.googlecode.protobuf.format.JsonFormat;

import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.infospace.converter.OafRelToAvroConverterTestBase;



/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ImportInformationSpaceReducerTest {

    private static final String PUBLISHER_DEFAULT = "default publisher";
    
    private static final String INFERENCE_PROVENANCE_DEFAULT = "iis";
    
    private static final String TRUST_DEFAULT = "0.5";
    
    
    @Mock
    private Context context;
    
    @Mock
    private MultipleOutputs multipleOutputs;
    
    @Captor
    private ArgumentCaptor<String> mosKeyCaptor;
    
    @Captor
    private ArgumentCaptor<AvroKey<?>> mosValueCaptor;

    
    private ImportInformationSpaceReducer reducer;

    
    @Before
    public void init() throws Exception {
        reducer = new ImportInformationSpaceReducer() {
            
            @Override
            protected MultipleOutputs instantiateMultipleOutputs(Context context) {
                return multipleOutputs;
            }
            
        };
    }
    
    // ------------------------------------- TESTS -----------------------------------
    
    @Test(expected=NullPointerException.class)
    public void testSetupNoParams() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        reducer.setup(context);
    }

    @Test
    public void testSetupWithOutputDirsSet() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        reducer.setup(context);
    }
    
    @Test
    public void testReduceWithUnrecognizedId() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String id = "id";
        Text key = new Text(id);
        List<InfoSpaceRecord> values = new ArrayList<>();
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
        
    }
    
    @Test
    public void testReduceResultWithRelations() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String resultId = "resultId";
        String id = InfoSpaceConstants.ROW_PREFIX_RESULT + resultId;
        Text key = new Text(id);
        InfoSpaceRecord bodyRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildResultEntity(resultId))));
        
        String updatedPublisher = "updated publisher";
        InfoSpaceRecord updateRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text("update"),
                new Text(JsonFormat.printToString(buildResultEntity(resultId, updatedPublisher, false))));
        
        String projectId = "projectId";
        InfoSpaceRecord resProjRelRecord = new InfoSpaceRecord(
                new Text(reducer.resProjColumnFamily),
                new Text("resProjQualifier"),
                new Text(JsonFormat.printToString(buildRel(resultId, projectId))));
        
        String dedupedId = "dedupedId";
        InfoSpaceRecord dedupMappingRelRecord = new InfoSpaceRecord(
                new Text(reducer.dedupMappingColumnFamily),
                new Text("dedupQualifier"),
                new Text(JsonFormat.printToString(buildRel(resultId, dedupedId))));
        
        List<InfoSpaceRecord> values = Lists.newArrayList(
                bodyRecord, updateRecord, resProjRelRecord, dedupMappingRelRecord);
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(3)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // doc meta
        assertEquals(conf.get(OUTPUT_NAME_DOCUMENT_META), mosKeyCaptor.getAllValues().get(0));
        DocumentMetadata docMeta = (DocumentMetadata) mosValueCaptor.getAllValues().get(0).datum();
        assertNotNull(docMeta);
        assertEquals(resultId, docMeta.getId());
        assertEquals(PUBLISHER_DEFAULT, docMeta.getPublisher());
        
        // result project rel
        assertEquals(conf.get(OUTPUT_NAME_DOCUMENT_PROJECT), mosKeyCaptor.getAllValues().get(1));
        DocumentToProject docProjRel = (DocumentToProject) mosValueCaptor.getAllValues().get(1).datum();
        assertNotNull(docProjRel);
        assertEquals(resultId, docProjRel.getDocumentId());
        assertEquals(projectId, docProjRel.getProjectId());
        // dedup mapping rel
        assertEquals(conf.get(OUTPUT_NAME_DEDUP_MAPPING), mosKeyCaptor.getAllValues().get(2));
        IdentifierMapping dedupMappingRel = (IdentifierMapping) mosValueCaptor.getAllValues().get(2).datum();
        assertNotNull(dedupMappingRel);
        assertEquals(dedupedId, dedupMappingRel.getOriginalId());
        assertEquals(resultId, dedupMappingRel.getNewId());
    }
    
    @Test
    public void testReduceResultWithMergedBodyUpdates() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        conf.set(IMPORT_MERGE_BODY_WITH_UPDATES, "true");
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String resultId = "resultId";
        String id = InfoSpaceConstants.ROW_PREFIX_RESULT + resultId;
        Text key = new Text(id);
        InfoSpaceRecord bodyRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildResultEntity(resultId))));
        
        String updatedPublisher = "updated publisher";
        InfoSpaceRecord updateRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text("update"),
                new Text(JsonFormat.printToString(buildResultEntity(resultId, updatedPublisher, false))));
        
        List<InfoSpaceRecord> values = Lists.newArrayList(
                bodyRecord, updateRecord);
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(1)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // doc meta
        assertEquals(conf.get(OUTPUT_NAME_DOCUMENT_META), mosKeyCaptor.getValue());
        DocumentMetadata docMeta = (DocumentMetadata) mosValueCaptor.getValue().datum();
        assertNotNull(docMeta);
        assertEquals(resultId, docMeta.getId());
        assertEquals(updatedPublisher, docMeta.getPublisher());
    }
    
    @Test
    public void testReduceResultWithoutBody() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String resultId = "resultId";
        String id = InfoSpaceConstants.ROW_PREFIX_RESULT + resultId;
        Text key = new Text(id);
        List<InfoSpaceRecord> values = Collections.emptyList();
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
    }
    
    @Test
    public void testReduceProject() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String projectId = "projectId";
        String id = InfoSpaceConstants.ROW_PREFIX_PROJECT + projectId;
        Text key = new Text(id);
        List<InfoSpaceRecord> values = Lists.newArrayList(new InfoSpaceRecord(
                new Text(Type.project.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildProjectEntity(projectId)))));
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(1)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        assertEquals(conf.get(OUTPUT_NAME_PROJECT), mosKeyCaptor.getValue());
        Project project = (Project) mosValueCaptor.getValue().datum();
        assertNotNull(project);
        // in-depth output validation is not subject of this test case
        assertEquals(projectId, project.getId());
    }
    
    @Test
    public void testReduceOrganization() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String orgId = "organizationId";
        String id = InfoSpaceConstants.ROW_PREFIX_ORGANIZATION + orgId;
        Text key = new Text(id);
        List<InfoSpaceRecord> values = Lists.newArrayList(new InfoSpaceRecord(
                new Text(Type.organization.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildOrganizationEntity(orgId)))));
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(1)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        assertEquals(conf.get(OUTPUT_NAME_ORGANIZATION), mosKeyCaptor.getValue());
        Organization project = (Organization) mosValueCaptor.getValue().datum();
        assertNotNull(project);
        // in-depth output validation is not subject of this test case
        assertEquals(orgId, project.getId());
    }
    
    @Test
    public void testReduceEntityWithoutBody() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String orgId = "organizationId";
        String id = InfoSpaceConstants.ROW_PREFIX_ORGANIZATION + orgId;
        Text key = new Text(id);
        List<InfoSpaceRecord> values = Collections.emptyList();
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
    }
    
    @Test
    public void testReduceEntityWithRelations() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String projectId = "projectId";
        String id = InfoSpaceConstants.ROW_PREFIX_PROJECT + projectId;
        Text key = new Text(id);
        
        InfoSpaceRecord bodyRecord = new InfoSpaceRecord(
                new Text(Type.project.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildProjectEntity(projectId))));
        
        String organizationId = "organizationId";
        InfoSpaceRecord projOrgRelRecord = new InfoSpaceRecord(
                new Text(reducer.projOrgColumnFamily),
                new Text("projOrgQualifier"),
                new Text(JsonFormat.printToString(buildRel(projectId, organizationId))));
        
        List<InfoSpaceRecord> values = Lists.newArrayList(bodyRecord, projOrgRelRecord);
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(2)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // project
        assertEquals(conf.get(OUTPUT_NAME_PROJECT), mosKeyCaptor.getAllValues().get(0));
        Project project = (Project) mosValueCaptor.getAllValues().get(0).datum();
        assertNotNull(project);
        assertEquals(projectId, project.getId());
        // project organization rel
        assertEquals(conf.get(OUTPUT_NAME_PROJECT_ORGANIZATION), mosKeyCaptor.getAllValues().get(1));
        ProjectToOrganization projOrgRel = (ProjectToOrganization) mosValueCaptor.getAllValues().get(1).datum();
        assertNotNull(projOrgRel);
        assertEquals(projectId, projOrgRel.getProjectId());
        assertEquals(organizationId, projOrgRel.getOrganizationId());
    }
    
    @Test
    public void testReduceResultBelowTrustLevelThreshold() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        conf.set(IMPORT_TRUST_LEVEL_THRESHOLD, "0.8");
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String resultId = "resultId";
        String id = InfoSpaceConstants.ROW_PREFIX_RESULT + resultId;
        Text key = new Text(id);
        InfoSpaceRecord bodyRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildResultEntity(resultId))));
        
        List<InfoSpaceRecord> values = Lists.newArrayList(bodyRecord);
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
    }
    
    @Test
    public void testReduceResultWithProvenanceBlacklist() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        conf.set(IMPORT_INFERENCE_PROVENANCE_BLACKLIST, INFERENCE_PROVENANCE_DEFAULT);
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String resultId = "resultId";
        String id = InfoSpaceConstants.ROW_PREFIX_RESULT + resultId;
        Text key = new Text(id);
        InfoSpaceRecord bodyRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildResultEntity(resultId))));
        
        List<InfoSpaceRecord> values = Lists.newArrayList(bodyRecord);
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
    }
    
    @Test
    public void testReduceResultDeletedByInference() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        conf.set(IMPORT_SKIP_DELETED_BY_INFERENCE, "true");
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String resultId = "resultId";
        String id = InfoSpaceConstants.ROW_PREFIX_RESULT + resultId;
        Text key = new Text(id);
        InfoSpaceRecord bodyRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildResultEntity(resultId, true))));
        
        List<InfoSpaceRecord> values = Lists.newArrayList(bodyRecord);
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
    }
    
    @Test
    public void testReduceResultAcceptingDeletedByInference() throws Exception {
     // given
        Configuration conf = setOutputDirs(new Configuration());
        conf.set(IMPORT_SKIP_DELETED_BY_INFERENCE, "false");
        doReturn(conf).when(context).getConfiguration();

        reducer.setup(context);
        
        String resultId = "resultId";
        String id = InfoSpaceConstants.ROW_PREFIX_RESULT + resultId;
        Text key = new Text(id);
        InfoSpaceRecord bodyRecord = new InfoSpaceRecord(
                new Text(Type.result.name()),
                new Text(OafBodyWithOrderedUpdates.BODY_QUALIFIER_NAME),
                new Text(JsonFormat.printToString(buildResultEntity(resultId, true))));
        
        List<InfoSpaceRecord> values = Lists.newArrayList(bodyRecord);
        
        // execute
        reducer.reduce(key, values, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(1)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // doc meta
        assertEquals(conf.get(OUTPUT_NAME_DOCUMENT_META), mosKeyCaptor.getValue());
        DocumentMetadata docMeta = (DocumentMetadata) mosValueCaptor.getValue().datum();
        assertNotNull(docMeta);
        assertEquals(resultId, docMeta.getId());
    }
    
    @Test
    public void testCleanup() throws Exception {
        // given
        Configuration conf = setOutputDirs(new Configuration());
        doReturn(conf).when(context).getConfiguration();
        reducer.setup(context);
        
        // execute
        reducer.cleanup(context);
        
        // assert
        verify(multipleOutputs, times(1)).close();
    }
    
    // ------------------------------------- PRIVATE -----------------------------------

    private Configuration setOutputDirs(Configuration conf) {
        conf.set(OUTPUT_NAME_DEDUP_MAPPING, "dedupMappingOutput");
        conf.set(OUTPUT_NAME_DOCUMENT_META, "docMetaOutput");
        conf.set(OUTPUT_NAME_DATASET_META, "datasetMetaOutput");
        conf.set(OUTPUT_NAME_DOCUMENT_PROJECT, "docProjectOutput");
        conf.set(OUTPUT_NAME_ORGANIZATION, "orgOutput");
        conf.set(OUTPUT_NAME_PROJECT, "projectOutput");
        conf.set(OUTPUT_NAME_PROJECT_ORGANIZATION, "projOrgOutput");
        return conf;
    }
    
    private Oaf buildResultEntity(String id, String publisher, boolean deletedByInference) {
        OafEntity.Builder entityBuilder = OafEntity.newBuilder();
        entityBuilder.setType(Type.result);
        entityBuilder.setId(id);
        Result.Builder resultBuilder = Result.newBuilder();
        eu.dnetlib.data.proto.ResultProtos.Result.Metadata.Builder metaBuilder = eu.dnetlib.data.proto.ResultProtos.Result.Metadata.newBuilder();
        metaBuilder.setPublisher(StringField.newBuilder().setValue(publisher).build());
        resultBuilder.setMetadata(metaBuilder.build());
        entityBuilder.setResult(resultBuilder.build());
        
        DataInfo.Builder dataInfoBuilder = DataInfo.newBuilder();
        dataInfoBuilder.setTrust(TRUST_DEFAULT);
        dataInfoBuilder.setInferred(true);
        dataInfoBuilder.setInferenceprovenance(INFERENCE_PROVENANCE_DEFAULT);
        dataInfoBuilder.setProvenanceaction(Qualifier.newBuilder().build());
        dataInfoBuilder.setDeletedbyinference(deletedByInference);
        
        return Oaf.newBuilder().setKind(Kind.entity).setEntity(entityBuilder.build())
                .setDataInfo(dataInfoBuilder.build()).build();
    }
    
    private Oaf buildResultEntity(String id) {
        return buildResultEntity(id, PUBLISHER_DEFAULT, false);
    }
    
    private Oaf buildResultEntity(String id, boolean deletedByInference) {
        return buildResultEntity(id, PUBLISHER_DEFAULT, deletedByInference);
    }
    
    private Oaf buildProjectEntity(String id) {
        OafEntity.Builder entityBuilder = OafEntity.newBuilder();
        entityBuilder.setType(Type.project);
        entityBuilder.setId(id);
        eu.dnetlib.data.proto.ProjectProtos.Project.Builder projectBuilder = eu.dnetlib.data.proto.ProjectProtos.Project
                .newBuilder();
        projectBuilder.setMetadata(eu.dnetlib.data.proto.ProjectProtos.Project.Metadata.newBuilder().setCode(
                StringField.newBuilder().setValue("grant-id").build()).build());
        entityBuilder.setProject(projectBuilder.build());
        return Oaf.newBuilder().setKind(Kind.entity).setEntity(entityBuilder.build()).build();
    }
    
    private Oaf buildOrganizationEntity(String id) {
        OafEntity.Builder entityBuilder = OafEntity.newBuilder();
        entityBuilder.setType(Type.organization);
        entityBuilder.setId(id);
        eu.dnetlib.data.proto.OrganizationProtos.Organization.Builder orgBuilder = eu.dnetlib.data.proto.OrganizationProtos.Organization
                .newBuilder();
        orgBuilder.setMetadata(eu.dnetlib.data.proto.OrganizationProtos.Organization.Metadata.newBuilder().setLegalname(
                StringField.newBuilder().setValue("legal-name").build()).build());
        entityBuilder.setOrganization(orgBuilder.build());
        return Oaf.newBuilder().setKind(Kind.entity).setEntity(entityBuilder.build()).build();
    }
    
    private Oaf buildRel(String sourceId, String targetId) {
        return Oaf.newBuilder().setKind(Kind.relation).setRel(
                OafRelToAvroConverterTestBase.createOafRelObject(sourceId, targetId)).build();
    }
}
