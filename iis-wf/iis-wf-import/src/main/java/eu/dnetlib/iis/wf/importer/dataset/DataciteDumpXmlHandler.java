package eu.dnetlib.iis.wf.importer.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DatasetToMDStore;
import eu.dnetlib.iis.wf.importer.RecordReceiver;

/**
 * Datacite XML dump SAX handler.
 * Notice: writer is not being closed by handler. Created outside, let it be closed outside as well.
 * @author mhorst
 *
 */
public class DataciteDumpXmlHandler extends DefaultHandler {

	private static final String ELEM_HEADER = "header";
	private static final String ELEM_PAYLOAD = "payload";
	private static final String ELEM_METADATA = "metadata";
	
	private static final String ELEM_RESOURCE = "resource";

	public static final String ELEM_IDENTIFIER = "identifier";
	public static final String ELEM_OBJ_IDENTIFIER = "objIdentifier";
	
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
	
	private static final String ELEM_ALTERNATE_IDENTIFIERS = "alternateIdentifiers";
    private static final String ELEM_ALTERNATE_IDENTIFIER = "alternateIdentifier";
	
	private static final String ATTRIBUTE_ID_TYPE = "identifierType";
	private static final String ATTRIBUTE_RESOURCE_TYPE_GENERAL = "resourceTypeGeneral";
	private static final String ATTRIBUTE_ALTERNATE_IDENTIFIER_TYPE = "alternateIdentifierType";
	
//	lowercased identifier types
	public static final String ID_TYPE_DOI = "doi";
	
	private static final Logger log = Logger.getLogger(DataciteDumpXmlHandler.class);
	
	private Stack<String> parents;
	
	private StringBuilder currentValue = new StringBuilder();
	
	private DatasetMetadata datasetMeta = new DatasetMetadata();
	
	private final RecordReceiver<DataSetReference> datasetReceiver;
	
	private final RecordReceiver<DatasetToMDStore> datasetToMDStoreReceived;
	
	private final String mainIdFieldName;
	
	private final String mdStoreId;
	
	// ------------------------ LOGIC --------------------------
	
	/**
	 * @param datasetReceiver dataset object receiver
	 * @param datasetToMDStoreReceived dataset to mdstore relation receiver
	 * @param mainIdFieldName field name to be used as main identifier. Introduced because of differences between MDStore records and XML dump records.
	 * @param mdStoreId mdStore identifier
	 */
	public DataciteDumpXmlHandler(RecordReceiver<DataSetReference> datasetReceiver,
			RecordReceiver<DatasetToMDStore> datasetToMDStoreReceived,
			String mainIdFieldName, String mdStoreId) {
		super();
		this.datasetReceiver = datasetReceiver;
		this.datasetToMDStoreReceived = datasetToMDStoreReceived;
		this.mainIdFieldName = mainIdFieldName;
		this.mdStoreId = mdStoreId;
	}
	
	/**
	 * @param datasetReceiver dataset object receiver
	 * @param datasetToMDStoreReceived dataset to mdstore relation receiver
	 * @param mainIdFieldName field name to be used as main identifier. Introduced because of differences between MDStore records and XML dump records.
	 * @param mdStoreId mdStore identifier
	 */
	public DataciteDumpXmlHandler(RecordReceiver<DataSetReference> datasetReceiver,
			RecordReceiver<DatasetToMDStore> datasetToMDStoreReceived, String mdStoreId) {
		this(datasetReceiver, datasetToMDStoreReceived, ELEM_OBJ_IDENTIFIER, mdStoreId);
	}
	
	@Override
	public void startDocument() throws SAXException {
		parents = new Stack<String>();
		datasetMeta = new DatasetMetadata();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (isWithinElement(localName, mainIdFieldName, ELEM_HEADER)) {
//			identifierType attribute is mandatory
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_IDENTIFIER, ELEM_RESOURCE)) {
//			identifierType attribute is mandatory
		    datasetMeta.idType = attributes.getValue(ATTRIBUTE_ID_TYPE).toLowerCase();	
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_CREATOR_NAME, ELEM_CREATOR)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_TITLE, ELEM_TITLES)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_FORMAT, ELEM_FORMATS)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_DESCRIPTION, ELEM_RESOURCE)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_PUBLISHER, ELEM_RESOURCE)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_PUBLICATION_YEAR, ELEM_RESOURCE)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_RESOURCE_TYPE, ELEM_RESOURCE)) {
		    datasetMeta.resourceTypeClass = attributes.getValue(ATTRIBUTE_RESOURCE_TYPE_GENERAL);
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_ALTERNATE_IDENTIFIER, ELEM_ALTERNATE_IDENTIFIERS)) {
		    datasetMeta.currentAlternateIdentifierType = attributes.getValue(ATTRIBUTE_ALTERNATE_IDENTIFIER_TYPE);
            this.currentValue = new StringBuilder();
        } 
		this.parents.push(localName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
		if (isWithinElement(localName, mainIdFieldName, ELEM_HEADER)) {
		    datasetMeta.headerId = this.currentValue.toString().trim();
		} else if (isWithinElement(localName, ELEM_IDENTIFIER, ELEM_RESOURCE)) {
		    datasetMeta.idValue = this.currentValue.toString().trim();
		} else if (isWithinElement(localName, ELEM_CREATOR_NAME, ELEM_CREATOR) 
				&& this.currentValue.length()>0) {
			if (datasetMeta.creatorNames==null) {
			    datasetMeta.creatorNames = new ArrayList<CharSequence>();
			}
			datasetMeta.creatorNames.add(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_TITLE, ELEM_TITLES)
				&& this.currentValue.length()>0) {
			if (datasetMeta.titles==null) {
			    datasetMeta.titles = new ArrayList<CharSequence>();
			}
			datasetMeta.titles.add(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_FORMAT, ELEM_FORMATS)
				&& this.currentValue.length()>0) {
			if (datasetMeta.formats==null) {
			    datasetMeta.formats = new ArrayList<CharSequence>();
			}
			datasetMeta.formats.add(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_ALTERNATE_IDENTIFIER, ELEM_ALTERNATE_IDENTIFIERS)
                && this.currentValue.length()>0) {
            if (datasetMeta.currentAlternateIdentifierType!=null) {
                if (datasetMeta.alternateIdentifiers==null) {
                    datasetMeta.alternateIdentifiers = Maps.newHashMap();
                }
                datasetMeta.alternateIdentifiers.put(datasetMeta.currentAlternateIdentifierType, this.currentValue.toString().trim());    
            }
        } else if (isWithinElement(localName, ELEM_DESCRIPTION, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
            datasetMeta.description = this.currentValue.toString().trim();
		} else if (isWithinElement(localName, ELEM_PUBLISHER, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
		    datasetMeta.publisher = this.currentValue.toString().trim();
		} else if (isWithinElement(localName, ELEM_PUBLICATION_YEAR, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
		    datasetMeta.publicationYear = this.currentValue.toString().trim();
		} else if (isWithinElement(localName, ELEM_RESOURCE_TYPE, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
		    datasetMeta.resourceTypeValue = this.currentValue.toString().trim();
		} else if (isWithinElement(localName, ELEM_RESOURCE, ELEM_PAYLOAD) ||
//				temporary hack: the case below is for the records originated from MDStore 
//				where no 'payload' element is present, required until fixing MDStore contents
				isWithinElement(localName, ELEM_RESOURCE, ELEM_METADATA)) {
			if (datasetMeta.idType!=null && datasetMeta.idValue!=null) {
			    storeRecords();
			} else {
				log.warn("either reference type " + datasetMeta.idType + " or id value: " + datasetMeta.idValue + 
						" was null for record id: " + datasetMeta.headerId);
			}
			this.datasetMeta = new DatasetMetadata();
		}
//		resetting current value;
		this.currentValue = null;
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
	
	// ------------------------ PRIVATE --------------------------
	
    private void storeRecords() throws SAXException {
        try {
            if (datasetMeta.headerId==null) {
                throw new SAXException("header identifier was not found!");
            }

            String idValueStr = datasetMeta.idValue.trim();
            String datasetId = ELEM_OBJ_IDENTIFIER.equals(mainIdFieldName)?
                    HBaseConstants.ROW_PREFIX_RESULT + datasetMeta.headerId : datasetMeta.headerId;

            DatasetToMDStore.Builder documentToMDStoreBuilder = DatasetToMDStore.newBuilder();
            documentToMDStoreBuilder.setMdStoreId(this.mdStoreId);
            documentToMDStoreBuilder.setDatasetId(datasetId);

            DataSetReference.Builder dataSetRefBuilder = DataSetReference.newBuilder();
            dataSetRefBuilder.setId(datasetId);
            dataSetRefBuilder.setReferenceType(datasetMeta.idType);
            dataSetRefBuilder.setIdForGivenType(idValueStr);

            if (datasetMeta.creatorNames!=null) {
                dataSetRefBuilder.setCreatorNames(datasetMeta.creatorNames);
            }
            if (datasetMeta.titles!=null) {
                dataSetRefBuilder.setTitles(datasetMeta.titles);
            }
            if (datasetMeta.formats!=null) {
                dataSetRefBuilder.setFormats(datasetMeta.formats);
            }
            if (datasetMeta.description!=null) {
                dataSetRefBuilder.setDescription(datasetMeta.description);
            }
            if (datasetMeta.publisher!=null) {
                dataSetRefBuilder.setPublisher(datasetMeta.publisher);
            }
            if (datasetMeta.publicationYear!=null) {
                dataSetRefBuilder.setPublicationYear(datasetMeta.publicationYear);
            }
            if (datasetMeta.resourceTypeClass!=null) {
                dataSetRefBuilder.setResourceTypeClass(datasetMeta.resourceTypeClass);
            }
            if (datasetMeta.resourceTypeValue!=null) {
                dataSetRefBuilder.setResourceTypeValue(datasetMeta.resourceTypeValue);
            }
            if (datasetMeta.alternateIdentifiers!=null) {
                dataSetRefBuilder.setAlternateIdentifiers(datasetMeta.alternateIdentifiers);
            }
            
            datasetReceiver.receive(dataSetRefBuilder.build());
            datasetToMDStoreReceived.receive(documentToMDStoreBuilder.build());
            
        } catch (IOException e) {
            throw new SAXException(e);
        }
    }
	
	boolean isWithinElement(String localName, String expectedElement, String expectedParent) {
		return localName.equals(expectedElement) && !this.parents.isEmpty() && 
				expectedParent.equals(this.parents.peek());
	}
	
}
