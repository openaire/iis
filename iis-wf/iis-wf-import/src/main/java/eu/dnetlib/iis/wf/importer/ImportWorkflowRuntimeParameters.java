package eu.dnetlib.iis.wf.importer;

/**
 * Import realated workflow parameters.
 * @author mhorst
 *
 */
public abstract class ImportWorkflowRuntimeParameters {

	private ImportWorkflowRuntimeParameters() {}
	
	public static final String IMPORT_INFERENCE_PROVENANCE_BLACKLIST = "import.inference.provenance.blacklist";
	public static final String IMPORT_SKIP_DELETED_BY_INFERENCE = "import.skip.deleted.by.inference";
	public static final String IMPORT_TRUST_LEVEL_THRESHOLD = "import.trust.level.threshold";
	public static final String IMPORT_APPROVED_DATASOURCES_CSV = "import.approved.datasources.csv";
	public static final String IMPORT_APPROVED_COLUMNFAMILIES_CSV = "import.approved.columnfamilies.csv";
	public static final String IMPORT_MERGE_BODY_WITH_UPDATES = "import.merge.body.with.updates";
	public static final String IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV = "import.content.approved.objectstores.csv";
	public static final String IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV = "import.content.blacklisted.objectstores.csv";
	public static final String IMPORT_CONTENT_OBJECSTORE_PAGESIZE = "import.content.objectstore.resultset.pagesize";
	
	public static final String IMPORT_HBASE_TABLE_NAME = "import.hbase.table.name";
		
	public static final String IMPORT_CONTENT_OBJECT_STORE_LOC = "import.content.object.store.location";
	public static final String IMPORT_CONTENT_LOOKUP_SERVICE_LOC = "import.content.lookup.service.location";
	public static final String IMPORT_CONTENT_OBJECT_STORE_IDS_CSV = "import.content.object.store.ids.csv";
	public static final String IMPORT_CONTENT_MAX_FILE_SIZE_MB = "import.content.max.file.size.mb";
	public static final String IMPORT_CONTENT_CONNECTION_TIMEOUT = "import.content.connection.timeout";
	public static final String IMPORT_CONTENT_READ_TIMEOUT = "import.content.read.timeout";

	public static final String IMPORT_DATACITE_MDSTORE_IDS_CSV = "import.datacite.mdstore.ids.csv";
	public static final String IMPORT_DATACITE_MDSTORE_PAGESIZE = "import.datacite.mdstore.resultset.pagesize";
	public static final String IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION = "import.datacite.mdstore.service.location";
	
	public static final String IMPORT_DATABASE_SERVICE_LOCATION = "import.database.service.location";
	public static final String IMPORT_DATABASE_SERVICE_DBNAME = "import.database.service.dbname";
	
	public static final String IMPORT_ISLOOKUP_SERVICE_LOCATION = "import.islookup.service.location";
	public static final String IMPORT_VOCABULARY_CODE = "import.vocabulary.code";
	public static final String IMPORT_VOCABULARY_OUTPUT_FILENAME = "import.vocabulary.output.filename";
	
	public static final String IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT = "import.resultset.client.read.timeout";
	
	public static final String HBASE_ENCODING = "hbase.table.encoding";
	
	public static final String IMPORT_FACADE_FACTORY_CLASS = "import.facade.factory.classname";
}
