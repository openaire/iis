package eu.dnetlib.iis.wf.importer.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DocumentToMDStore;

/**
 * Datacite XML dump SAX handler.
 * Notice: writer is not being closed by handler.
 * Created outside, let it be closed outside as well.
 * @author mhorst
 *
 */
public class DataciteDumpXmlHandler extends DefaultHandler {

	private static final String ELEM_HEADER = "oai:header";
	private static final String ELEM_PAYLOAD = "payload";
	private static final String ELEM_METADATA = "metadata";
	
	private static final String ELEM_RESOURCE = "resource";

	public static final String ELEM_IDENTIFIER = "identifier";
	public static final String ELEM_OBJ_IDENTIFIER = "dri:objIdentifier";
	
	private static final String ELEM_CREATOR = "creator";
	private static final String ELEM_CREATOR_NAME = "creatorName";
	
	private static final String ELEM_TITLES = "titles";
	private static final String ELEM_TITLE = "title";
	
	private static final String ELEM_DESCRIPTION = "description";
	private static final String ELEM_PUBLISHER = "publisher";
	private static final String ELEM_PUBLICATION_YEAR = "publicationYear";
	private static final String ELEM_FORMATS = "formats";
	private static final String ELEM_FORMAT = "format";
	private static final String ELEM_RESOURCE_TYPE = "resourceType";
	
	private static final String ATTRIBUTE_ID_TYPE = "identifierType";
	private static final String ATTRIBUTE_RESOURCE_TYPE_GENERAL = "resourceTypeGeneral";
	
//	lowercased identifier types
	public static final String ID_TYPE_DOI = "doi";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private Stack<String> parents;
	
	private StringBuilder currentValue = new StringBuilder();
	
	private String headerId = null;
	
	private String idType = null;
	private String idValue = null;
	private List<CharSequence> creatorNames = null;
	private List<CharSequence> titles = null;
	private List<CharSequence> formats = null;
	private String publisher = null;
	private String description = null;
	private String publicationYear = null;
	private String resourceTypeClass = null;
	private String resourceTypeValue = null;
	
	private final RecordReceiver<DataSetReference> datasetReceiver;
	
	private final RecordReceiver<DocumentToMDStore> datasetToMDStoreReceived;
	
	private final String mainIdFieldName;
	
	private final String mdStoreId;
	
	/**
	 * Default constructor.
	 * @param datasetReceiver
	 * @param datasetToMDStoreReceived
	 * @param mainIdFieldName field name to be used as main identifier. Introduced because of differences between MDStore records and XML dump records.
	 * @param mdStoreId
	 */
	public DataciteDumpXmlHandler(RecordReceiver<DataSetReference> datasetReceiver,
			RecordReceiver<DocumentToMDStore> datasetToMDStoreReceived,
			String mainIdFieldName, String mdStoreId) {
		super();
		this.datasetReceiver = datasetReceiver;
		this.datasetToMDStoreReceived = datasetToMDStoreReceived;
		this.mainIdFieldName = mainIdFieldName;
		this.mdStoreId = mdStoreId;
	}
	
	/**
	 * Default constructor.
	 * @param datasetReceiver
	 * @param datasetToMDStoreReceived
	 * @param mainIdFieldName field name to be used as main identifier. Introduced because of differences between MDStore records and XML dump records.
	 * @param mdStoreId
	 */
	public DataciteDumpXmlHandler(RecordReceiver<DataSetReference> datasetReceiver,
			RecordReceiver<DocumentToMDStore> datasetToMDStoreReceived,
			String mdStoreId) {
		this(datasetReceiver, datasetToMDStoreReceived, ELEM_OBJ_IDENTIFIER, mdStoreId);
	}
	
	@Override
	public void startDocument() throws SAXException {
		parents = new Stack<String>();
		clearAllFields();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (isWithinElement(qName, mainIdFieldName, ELEM_HEADER)) {
//			identifierType attribute is mandatory
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_IDENTIFIER, ELEM_RESOURCE)) {
//			identifierType attribute is mandatory
			this.idType = attributes.getValue(ATTRIBUTE_ID_TYPE).toLowerCase();	
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_CREATOR_NAME, ELEM_CREATOR)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_TITLE, ELEM_TITLES)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_FORMAT, ELEM_FORMATS)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_DESCRIPTION, ELEM_RESOURCE)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_PUBLISHER, ELEM_RESOURCE)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_PUBLICATION_YEAR, ELEM_RESOURCE)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_RESOURCE_TYPE, ELEM_RESOURCE)) {
			this.resourceTypeClass = attributes.getValue(ATTRIBUTE_RESOURCE_TYPE_GENERAL);
			this.currentValue = new StringBuilder();
		} 
		this.parents.push(qName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
		if (isWithinElement(qName, mainIdFieldName, ELEM_HEADER)) {
			this.headerId = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_IDENTIFIER, ELEM_RESOURCE)) {
			this.idValue = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_CREATOR_NAME, ELEM_CREATOR) 
				&& this.currentValue.length()>0) {
			if (this.creatorNames==null) {
				this.creatorNames = new ArrayList<CharSequence>();
			}
			this.creatorNames.add(this.currentValue.toString().trim());
		} else if (isWithinElement(qName, ELEM_TITLE, ELEM_TITLES)
				&& this.currentValue.length()>0) {
			if (this.titles==null) {
				this.titles = new ArrayList<CharSequence>();
			}
			this.titles.add(this.currentValue.toString().trim());
		} else if (isWithinElement(qName, ELEM_FORMAT, ELEM_FORMATS)
				&& this.currentValue.length()>0) {
			if (this.formats==null) {
				this.formats = new ArrayList<CharSequence>();
			}
			this.formats.add(this.currentValue.toString().trim());
		} else if (isWithinElement(qName, ELEM_DESCRIPTION, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
			this.description = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_PUBLISHER, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
			this.publisher = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_PUBLICATION_YEAR, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
			this.publicationYear = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_RESOURCE_TYPE, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
			this.resourceTypeValue = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_RESOURCE, ELEM_PAYLOAD) ||
//				temporary hack: the case below is for the records originated from MDStore 
//				where no 'payload' element is present, required until fixing MDStore contents
				isWithinElement(qName, ELEM_RESOURCE, ELEM_METADATA)) {
//			writing whole record
			if (this.idType!=null && this.idValue!=null) {
				try {
					String idValueStr = this.idValue.toString().trim();
					DataSetReference.Builder dataSetRefBuilder = DataSetReference.newBuilder();
					DocumentToMDStore.Builder documentToMDStoreBuilder = DocumentToMDStore.newBuilder();
					documentToMDStoreBuilder.setMdStoreId(this.mdStoreId);
					String datasetId;
					if (this.headerId==null) {
						throw new SAXException("header identifier was not found!");
					}
					if (ELEM_OBJ_IDENTIFIER.equals(mainIdFieldName)) {
						datasetId = new String(
								HBaseConstants.ROW_PREFIX_RESULT, 
								HBaseConstants.STATIC_FIELDS_ENCODING_UTF8) + 
								this.headerId; 
						dataSetRefBuilder.setId(datasetId);	
						documentToMDStoreBuilder.setDocumentId(datasetId);
					} else {
						datasetId = this.headerId;
						dataSetRefBuilder.setId(datasetId);
						documentToMDStoreBuilder.setDocumentId(datasetId);
					}
					dataSetRefBuilder.setReferenceType(this.idType);
					dataSetRefBuilder.setIdForGivenType(idValueStr);
					if (this.creatorNames!=null) {
						dataSetRefBuilder.setCreatorNames(this.creatorNames);
					}
					if (this.titles!=null) {
						dataSetRefBuilder.setTitles(this.titles);
					}
					if (this.formats!=null) {
						dataSetRefBuilder.setFormats(this.formats);
					}
					if (this.description!=null) {
						dataSetRefBuilder.setDescription(this.description);
					}
					if (this.publisher!=null) {
						dataSetRefBuilder.setPublisher(this.publisher);
					}
					if (this.publicationYear!=null) {
						dataSetRefBuilder.setPublicationYear(this.publicationYear);
					}
					if (this.resourceTypeClass!=null) {
						dataSetRefBuilder.setResourceTypeClass(this.resourceTypeClass);
					}
					if (this.resourceTypeValue!=null) {
						dataSetRefBuilder.setResourceTypeValue(this.resourceTypeValue);
					}
					datasetReceiver.receive(dataSetRefBuilder.build());
					datasetToMDStoreReceived.receive(documentToMDStoreBuilder.build());
				} catch (IOException e) {
					throw new SAXException(e);
				}
			} else {
				log.warn("either reference type " + this.idType + 
						" or id value: " + this.idValue + 
						" was null for record id: " + this.headerId);
			}
			clearAllFields();
		}
//		resetting current value;
		this.currentValue = null;
	}

	private void clearAllFields() {
		this.headerId = null;
		this.idType = null;
		this.idValue = null;		
		this.creatorNames = null;
		this.titles = null;
		this.formats = null;
		this.description = null;
		this.publisher = null;
		this.publicationYear = null;
		this.resourceTypeClass = null;
		this.resourceTypeValue = null;
	}
	
	boolean isWithinElement(String qName,
			String expectedElement, String expectedParent) {
		return qName.equals(expectedElement) && !this.parents.isEmpty() && 
				expectedParent.equals(this.parents.peek());
	}
	
	@Override
	public void endDocument() throws SAXException {
		parents.clear();
		parents = null;
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (this.currentValue!=null) {
			this.currentValue.append(ch, start, length);
		}
	}
	
}
