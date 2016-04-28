package eu.dnetlib.iis.wf.importer.mapred;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.HBASE_ENCODING;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_APPROVED_DATASOURCES_CSV;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_INFERENCE_PROVENANCE_BLACKLIST;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_MERGE_BODY_WITH_UPDATES;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SKIP_DELETED_BY_INFERENCE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_TRUST_LEVEL_THRESHOLD;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.mapreduce.util.OafRelDecoder;
import eu.dnetlib.data.proto.DedupProtos.Dedup;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.PersonResultProtos.PersonResult.Authorship;
import eu.dnetlib.data.proto.ProjectOrganizationProtos.ProjectOrganization;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultProjectProtos.ResultProject.Outcome;
import eu.dnetlib.data.proto.TypeProtos;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.utils.ByteArrayUtils;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.Person;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.converter.DeduplicationMappingConverter;
import eu.dnetlib.iis.wf.importer.converter.DocumentMetadataConverter;
import eu.dnetlib.iis.wf.importer.converter.DocumentToProjectConverter;
import eu.dnetlib.iis.wf.importer.converter.OrganizationConverter;
import eu.dnetlib.iis.wf.importer.converter.PersonConverter;
import eu.dnetlib.iis.wf.importer.converter.ProjectConverter;
import eu.dnetlib.iis.wf.importer.converter.ProjectToOrganizationConverter;
import eu.dnetlib.iis.wf.importer.input.approver.ComplexApprover;
import eu.dnetlib.iis.wf.importer.input.approver.DataInfoBasedApprover;
import eu.dnetlib.iis.wf.importer.input.approver.FieldApprover;
import eu.dnetlib.iis.wf.importer.input.approver.OriginDatasourceApprover;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * IIS generic data importer mapper reading columns from InformationSpace HBase table. 
 * Handles all kinds of entities: result, person and project along with their relations.
 * 
 * Notice: hbase table model is created by: 
 * iis-wf/iis-wf-import/src/main/resources/eu/dnetlib/iis/wf/importer/hbasedump/oozie_app/scripts/truncate_hbase_table.sh
 * script. All changes introduced in {@link IISDataImporterMapper} must be in sync with the model created by the script.
 * 
 * @author mhorst
 *
 */
public class IISDataImporterMapper extends TableMapper<NullWritable, NullWritable> {

	private static final Logger log = Logger.getLogger(IISDataImporterMapper.class);
	
	private static final String OUTPUT_NAME_DOCUMENT_META = "output.name.document_meta";
	
	private static final String OUTPUT_NAME_DOCUMENT_PROJECT = "output.name.document_project";
	
	private static final String OUTPUT_NAME_PROJECT = "output.name.project";
	
	private static final String OUTPUT_NAME_PERSON = "output.name.person";
	
	private static final String OUTPUT_NAME_DEDUP_MAPPING = "output.name.dedup_mapping";

	private static final String OUTPUT_NAME_ORGANIZATION = "output.name.organization";
	
	private static final String OUTPUT_NAME_PROJECT_ORGANIZATION = "output.name.project_organization";
	
	private String outputNameDocumentMeta;
	
	private String outputNameDocumentProject;
	
	private String outputNameProject;
	
	private String outputNamePerson;
	
	private String outputNameDedupMapping;
	
	private String outputNameOrganization;
	
	private String outputNameProjectOrganization;
	
	private String encoding = HBaseConstants.STATIC_FIELDS_ENCODING_UTF8;

	private MultipleOutputs mos;
	
	private ResultApprover resultApprover;
	
	private FieldApprover fieldApprover;
	
	private DocumentMetadataConverter docMetaConverter;
	
	private DocumentToProjectConverter docProjectConverter;
	
	private DeduplicationMappingConverter deduplicationMappingConverter;
	
	private PersonConverter personConverter;
	
	private ProjectConverter projectConverter;
	
	private OrganizationConverter organizationConverter;
	
	private ProjectToOrganizationConverter projectOrganizationConverter;
	
	
	/**
	 * Flag indicating Oaf retrieved from body CF should be merged with all update collumns.
	 * Set to false by default.
	 */
	private boolean mergeBodyWithUpdates;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
//		setting parameters
		if (context.getConfiguration().get(
				HBASE_ENCODING)!=null) {
			this.encoding = context.getConfiguration().get(
					HBASE_ENCODING);	
		}
		mergeBodyWithUpdates = context.getConfiguration().get(IMPORT_MERGE_BODY_WITH_UPDATES)!=null?
				Boolean.valueOf(context.getConfiguration().get(IMPORT_MERGE_BODY_WITH_UPDATES)):false;
		
		DataInfoBasedApprover dataInfoBasedApprover = new DataInfoBasedApprover(
				context.getConfiguration().get(IMPORT_INFERENCE_PROVENANCE_BLACKLIST)!=null &&
						!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(context.getConfiguration().get(IMPORT_INFERENCE_PROVENANCE_BLACKLIST))?
						context.getConfiguration().get(IMPORT_INFERENCE_PROVENANCE_BLACKLIST):null,
						context.getConfiguration().get(IMPORT_SKIP_DELETED_BY_INFERENCE)!=null?
						Boolean.valueOf(context.getConfiguration().get(IMPORT_SKIP_DELETED_BY_INFERENCE)):true,
						context.getConfiguration().get(IMPORT_TRUST_LEVEL_THRESHOLD)!=null &&
						!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(context.getConfiguration().get(IMPORT_TRUST_LEVEL_THRESHOLD))?
								Float.valueOf(context.getConfiguration().get(IMPORT_TRUST_LEVEL_THRESHOLD)):null);

		String approvedDatasourcesCSV = context.getConfiguration().get(
				IMPORT_APPROVED_DATASOURCES_CSV);
		if (approvedDatasourcesCSV!=null && !approvedDatasourcesCSV.isEmpty() &&
				!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(approvedDatasourcesCSV)) {
//			approving contents from specified datasources only
			String[] split = StringUtils.split(approvedDatasourcesCSV, DEFAULT_CSV_DELIMITER);
			OriginDatasourceApprover datasourceApprover;
			Collection<TypeProtos.Type> entityTypes = Arrays.asList(
					TypeProtos.Type.person, TypeProtos.Type.result);
			if (split.length==1) {
				datasourceApprover = new OriginDatasourceApprover(
						entityTypes, Collections.singleton(split[0]));
			} else {
				datasourceApprover = new OriginDatasourceApprover(
						entityTypes, Arrays.asList(split));		
			}
			this.resultApprover = new ComplexApprover(
					datasourceApprover, dataInfoBasedApprover);
		} else {
			this.resultApprover = dataInfoBasedApprover;
		}
//		field approver
		this.fieldApprover = dataInfoBasedApprover;
		
//		initializing converters
		docMetaConverter = new DocumentMetadataConverter(encoding, 
				this.resultApprover, this.fieldApprover,
				getCollumnFamily(RelType.personResult, SubRelType.authorship, 
						Authorship.RelName.hasAuthor.toString()));
		deduplicationMappingConverter = new DeduplicationMappingConverter(
				encoding, resultApprover, 
				getCollumnFamily(RelType.resultResult, SubRelType.dedup, 
						Dedup.RelName.merges.toString()));
		docProjectConverter = new DocumentToProjectConverter(
				encoding, resultApprover, 
				getCollumnFamily(RelType.resultProject, SubRelType.outcome, 
						Outcome.RelName.isProducedBy.toString()));
		personConverter = new PersonConverter(encoding, resultApprover);
		projectConverter = new ProjectConverter(encoding, resultApprover);
		organizationConverter = new OrganizationConverter();
		projectOrganizationConverter = new ProjectToOrganizationConverter(
				encoding, resultApprover, 
				getCollumnFamily(RelType.projectOrganization, SubRelType.participation, 
						ProjectOrganization.Participation.RelName.hasParticipant.toString()));
		mos = new MultipleOutputs(context);
		
//		setting output subdirectory names
		outputNameDocumentMeta = context.getConfiguration().get(OUTPUT_NAME_DOCUMENT_META);
		if (outputNameDocumentMeta==null) {
			throw new RuntimeException("document metadata output name not provided!");
		}
		outputNameDocumentProject = context.getConfiguration().get(OUTPUT_NAME_DOCUMENT_PROJECT);
		if (outputNameDocumentProject==null) {
			throw new RuntimeException("document project relation output name not provided!");
		}
		outputNameProject = context.getConfiguration().get(OUTPUT_NAME_PROJECT);
		if (outputNameProject==null) {
			throw new RuntimeException("project output name not provided!");
		}
		outputNamePerson = context.getConfiguration().get(OUTPUT_NAME_PERSON);
		if (outputNamePerson==null) {
			throw new RuntimeException("person output name not provided!");
		}
		outputNameDedupMapping = context.getConfiguration().get(OUTPUT_NAME_DEDUP_MAPPING);
		if (outputNameDedupMapping==null) {
			throw new RuntimeException("deduplication mapping output name not provided!");
		}
        
        outputNameOrganization = context.getConfiguration().get(OUTPUT_NAME_ORGANIZATION);
        if (outputNameOrganization==null) {
            throw new RuntimeException("organization output name not provided!");
        }
        
        outputNameProjectOrganization = context.getConfiguration().get(OUTPUT_NAME_PROJECT_ORGANIZATION);
        if (outputNameProjectOrganization==null) {
            throw new RuntimeException("project to organization output name not provided!");
        }
    }
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		try {
			super.cleanup(context);	
		} finally {
			mos.close();	
		}
	}
	
	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws InterruptedException, IOException {
		
	    final byte[] idBytes = row.get();
		
		if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_RESULT)) {
			handleResult(idBytes, value, context);
		} 
		
		else if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_PERSON)) {
			handlePerson(idBytes, value, context);
		} 
		
		else if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_PROJECT)) {
			handleProject(idBytes, value, context);
		}
		
        else if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_ORGANIZATION)) {
            handleOrganization(idBytes, value, context);
        }
    }

	
	//------------------------ PRIVATE --------------------------
	
	/**
	 * Handles result row.
	 * @param row
	 * @param value
	 * @param context
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void handleResult(final byte[] idBytes, 
			Result value, Context context) throws InterruptedException, IOException {
		Oaf oafObj = buildOafObject(idBytes, value, HBaseConstants.getCollumnFamily(Type.result));
		if (resultApprover.approveBeforeBuilding(oafObj)) {
			mos.write(outputNameDocumentMeta, new AvroKey<DocumentMetadata>(
					docMetaConverter.buildObject(value, oafObj)));
//			hadling project relations
			DocumentToProject[] docProjects = docProjectConverter.buildObject(value, oafObj);
			if (docProjects!=null && docProjects.length>0) {
				for (DocumentToProject docProj : docProjects) {
					mos.write(outputNameDocumentProject, new AvroKey<DocumentToProject>(docProj));	
				}
			}
//			handling dedupRel relations, required for contents deduplication and identifiers translation 
			IdentifierMapping[] dedupMappings = deduplicationMappingConverter.buildObject(value, oafObj);
			if (dedupMappings!=null && dedupMappings.length>0) {
				for (IdentifierMapping dedupMapping : dedupMappings) {
					mos.write(outputNameDedupMapping, new AvroKey<IdentifierMapping>(dedupMapping));	
				}
			}
		}
	}
	
	/**
	 * Handles person row.
	 * @param idBytes
	 * @param value
	 * @param context
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void handlePerson(final byte[] idBytes, 
			Result value, Context context) throws InterruptedException, IOException {
		Oaf oafObj = buildOafObject(idBytes, value, HBaseConstants.getCollumnFamily(Type.person));
		if (resultApprover.approveBeforeBuilding(oafObj)) {
			Person personCandidate = personConverter.buildObject(value, oafObj);
			if (personCandidate!=null) {
				mos.write(outputNamePerson, new AvroKey<Person>(personCandidate));
			}
		}
	}
	
	/**
	 * Handles organization row.
	 * 
	 */
	private void handleOrganization(final byte[] idBytes, Result value, Context context) throws InterruptedException, IOException {
		
	    Oaf oafObj = buildOafObject(idBytes, value, HBaseConstants.getCollumnFamily(Type.organization));
		
	    if (resultApprover.approveBeforeBuilding(oafObj)) {
	        
			Organization organization = organizationConverter.buildObject(value, oafObj);
			
			if (organization != null) {
			    
				mos.write(outputNameOrganization, new AvroKey<Organization>(organization));
			}
		}
	    
	}
	
	
	 /**
     * Handles project row.
     * @param idBytes
     * @param value
     * @param context
     * @throws InterruptedException
     * @throws IOException
     */
    private void handleProject(final byte[] idBytes, 
            Result value, Context context) throws InterruptedException, IOException {
        Oaf oafObj = buildOafObject(idBytes, value, HBaseConstants.getCollumnFamily(Type.project));
        if (resultApprover.approveBeforeBuilding(oafObj)) {
            Project projectCandidate = projectConverter.buildObject(value, oafObj);
            if (projectCandidate!=null) {
                mos.write(outputNameProject, new AvroKey<Project>(projectCandidate));
            }
//			hadling participants
			ProjectToOrganization[] projOrgs = projectOrganizationConverter.buildObject(value, oafObj);
			if (projOrgs!=null && projOrgs.length>0) {
				for (ProjectToOrganization projOrg : projOrgs) {
					mos.write(outputNameProjectOrganization, new AvroKey<ProjectToOrganization>(projOrg));	
				}
			}
        }
    }
	
	/**
	 * Builds {@link Oaf} object from HBase result.
	 * @param idBytes
	 * @param value
	 * @param mainColumnFamily
	 * @return {@link Oaf} object built from HBase result
	 * @throws UnsupportedEncodingException
	 * @throws InvalidProtocolBufferException
	 */
	private Oaf buildOafObject(byte[] idBytes, 
			Result value, byte[] mainColumnFamily) 
					throws UnsupportedEncodingException, InvalidProtocolBufferException {
//		always obtaining Oaf from body first
		byte[] oaBodyfBytes = value.getValue(
				mainColumnFamily, 
				HBaseConstants.QUALIFIER_BODY);
		if (oaBodyfBytes!=null) {
			Oaf.Builder oafBuilder = Oaf.newBuilder();
			oafBuilder.mergeFrom(oaBodyfBytes);
			if (mergeBodyWithUpdates) {
				NavigableMap<byte[],byte[]> resultUpdates = value.getFamilyMap(
						mainColumnFamily);
				if (resultUpdates.size()>1) {
					for (Entry<byte[], byte[]> entry : resultUpdates.entrySet()) {
						if (compare(entry.getKey(), HBaseConstants.QUALIFIER_BODY)!=0) {
//							omitting already retrieved body
							oafBuilder.mergeFrom(entry.getValue());
						}
					}
				}
			}
			return oafBuilder.build();
		} else {
			log.error("got null body content for row " + 
					new String(idBytes, getEncoding()));
			return null;
		}	
	}
	
	/**
	 * Returns data encoding.
	 * @return data encoding
	 */
	public String getEncoding() {
		return encoding;
	}
	
	private static final byte[] getCollumnFamily(RelType relType, SubRelType subRelType,
			String relClass) {
		try {
			return OafRelDecoder.getCFQ(relType, subRelType, relClass).getBytes(
				HBaseConstants.STATIC_FIELDS_ENCODING_UTF8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
	
}

