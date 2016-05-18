package eu.dnetlib.iis.wf.export.actionmanager;

public abstract class ExportWorkflowRuntimeParameters {

	private ExportWorkflowRuntimeParameters() {}
	
	public static final String EXPORT_TRUST_LEVEL_THRESHOLD = "export.trust.level.threshold";
	
	public static final String EXPORT_ACTION_SETID = "export.action.setid";
	public static final String EXPORT_ENTITY_ACTION_SETID = "export.entity.action.setid";
	
	public static final String EXPORT_SEQ_FILE_ACTIVE = "export.seq.file.active";
	public static final String EXPORT_SEQ_FILE_OUTPUT_DIR_ROOT = "export.seq.file.output.dir.root";
	public static final String EXPORT_SEQ_FILE_OUTPUT_DIR_NAME = "export.seq.file.output.dir.name";
	
	public static final char EXPORT_ALGORITHM_PROPERTY_SEPARATOR = '.';	
	
	public static final String EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME = "export.action.builder.factory.classname";
	
	public static final String EXPORT_DOCUMENTSSIMILARITY_THRESHOLD = "export.documentssimilarity.threshold";
	
	public static final String EXPORT_ENTITY_MDSTORE_SERVICE_LOCATION = "export.entity.mdstore.service.location";
	
	public static final String EXPORT_ENTITY_MDSTORE_ID = "export.entity.mdstore.id";
}
