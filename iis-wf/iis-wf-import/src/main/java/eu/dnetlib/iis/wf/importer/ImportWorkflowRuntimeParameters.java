package eu.dnetlib.iis.wf.importer;

/**
 * Import realated workflow parameters.
 * @author mhorst
 *
 */
public final class ImportWorkflowRuntimeParameters {
	
	// parameter names
	
	public static final String IMPORT_INFERENCE_PROVENANCE_BLACKLIST = "import.inference.provenance.blacklist";
	public static final String IMPORT_SKIP_DELETED_BY_INFERENCE = "import.skip.deleted.by.inference";
	public static final String IMPORT_TRUST_LEVEL_THRESHOLD = "import.trust.level.threshold";
	public static final String IMPORT_APPROVED_DATASOURCES_CSV = "import.approved.datasources.csv";
	public static final String IMPORT_APPROVED_COLUMNFAMILIES_CSV = "import.approved.columnfamilies.csv";
	public static final String IMPORT_MERGE_BODY_WITH_UPDATES = "import.merge.body.with.updates";
	public static final String IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV = "import.content.approved.objectstores.csv";
	public static final String IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV = "import.content.blacklisted.objectstores.csv";
	
	public static final String IMPORT_CONTENT_OBJECT_STORE_LOC = "import.content.object.store.location";
    public static final String IMPORT_CONTENT_OBJECT_STORE_S3_ENDPOINT = "import.content.object.store.s3.endpoint";
	public static final String IMPORT_CONTENT_OBJECT_STORE_IDS_CSV = "import.content.object.store.ids.csv";
	public static final String IMPORT_CONTENT_MAX_FILE_SIZE_MB = "import.content.max.file.size.mb";
	public static final String IMPORT_CONTENT_CONNECTION_TIMEOUT = "import.content.connection.timeout";
	public static final String IMPORT_CONTENT_READ_TIMEOUT = "import.content.read.timeout";

	public static final String IMPORT_MDSTORE_IDS_CSV = "import.mdstore.ids.csv";
	public static final String IMPORT_MDSTORE_SERVICE_LOCATION = "import.mdstore.service.location";
	public static final String IMPORT_MDSTORE_RECORD_MAXLENGTH = "import.mdstore.record.maxlength";
	
	public static final String IMPORT_ISLOOKUP_SERVICE_LOCATION = "import.islookup.service.location";
	public static final String IMPORT_VOCABULARY_CODE = "import.vocabulary.code";
	public static final String IMPORT_VOCABULARY_OUTPUT_FILENAME = "import.vocabulary.output.filename";
	
	public static final String IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT = "import.resultset.client.read.timeout";
	public static final String IMPORT_RESULT_SET_CLIENT_CONNECTION_TIMEOUT = "import.resultset.client.connection.timeout";
	public static final String IMPORT_RESULT_SET_PAGESIZE = "import.resultset.pagesize";
	
	
	public static final String HBASE_ENCODING = "hbase.table.encoding";
	
	public static final String IMPORT_FACADE_FACTORY_CLASS = "import.facade.factory.classname";
	
	// default values
	
	public static final String RESULTSET_READ_TIMEOUT_DEFAULT_VALUE = "60000";
	public static final String RESULTSET_CONNECTION_TIMEOUT_DEFAULT_VALUE = "60000";
	public static final String RESULTSET_PAGESIZE_DEFAULT_VALUE = "100";
	
	private ImportWorkflowRuntimeParameters() {}
	
}
