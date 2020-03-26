package eu.dnetlib.iis.wf.importer;

/**
 * Import realated workflow parameters.
 * @author mhorst
 *
 */
public final class ImportWorkflowRuntimeParameters {
	
	// parameter names
	
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
	
	public static final String IMPORT_SOFTWARE_HERITAGE_PAGE_SIZE = "import.software.heritage.page.size";
	public static final String IMPORT_SOFTWARE_HERITAGE_START_INDEX = "import.software.heritage.start.index";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_HOST = "import.software.heritage.endpoint.host";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_PORT = "import.software.heritage.endpoint.port";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_SCHEME = "import.software.heritage.endpoint.scheme";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_URI_ROOT = "import.software.heritage.endpoint.uri.root";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RATELIMIT_DELAY = "import.software.heritage.endpoint.ratelimit.delay";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RETRY_COUNT = "import.software.heritage.endpoint.retry.count";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_READ_TIMEOUT = "import.software.heritage.endpoint.read.timeout";
	public static final String IMPORT_SOFTWARE_HERITAGE_ENDPOINT_CONNECTION_TIMEOUT = "import.software.heritage.endpoint.connection.timeout";
	
	public static final String IMPORT_ISLOOKUP_SERVICE_LOCATION = "import.islookup.service.location";
	public static final String IMPORT_VOCABULARY_CODE = "import.vocabulary.code";
	public static final String IMPORT_VOCABULARY_OUTPUT_FILENAME = "import.vocabulary.output.filename";
	
	public static final String IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT = "import.resultset.client.read.timeout";
	public static final String IMPORT_RESULT_SET_CLIENT_CONNECTION_TIMEOUT = "import.resultset.client.connection.timeout";
	public static final String IMPORT_RESULT_SET_PAGESIZE = "import.resultset.pagesize";
	
	public static final String IMPORT_FACADE_FACTORY_CLASS = "import.facade.factory.classname";
	
	// default values
	
	public static final String RESULTSET_READ_TIMEOUT_DEFAULT_VALUE = "60000";
	public static final String RESULTSET_CONNECTION_TIMEOUT_DEFAULT_VALUE = "60000";
	public static final String RESULTSET_PAGESIZE_DEFAULT_VALUE = "100";
	public static final String SOFTWARE_HERITAGE_PAGE_SIZE_DEFAULT_VALUE = "100";
	
	private ImportWorkflowRuntimeParameters() {}
	
}
