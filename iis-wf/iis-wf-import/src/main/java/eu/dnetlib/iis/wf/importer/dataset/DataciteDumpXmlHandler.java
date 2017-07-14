package eu.dnetlib.iis.wf.importer.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
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
	
	private final RecordReceiver<DocumentText> datasetTextReceiver;
	
	private final String mainIdFieldName;
	
	private final String datasetText;
	
	// ------------------------ LOGIC --------------------------
	
	/**
	 * @param datasetReceiver dataset object receiver
	 * @param datasetTextReceived dataset plain text receiver
	 * @param mainIdFieldName field name to be used as main identifier. Introduced because of differences between MDStore records and XML dump records.
	 * @param datasetText dataset plain text
	 */
	public DataciteDumpXmlHandler(RecordReceiver<DataSetReference> datasetReceiver,
			RecordReceiver<DocumentText> datasetTextReceiver,
			String mainIdFieldName, String datasetText) {
		super();
		this.datasetReceiver = datasetReceiver;
		this.datasetTextReceiver = datasetTextReceiver;
		this.mainIdFieldName = mainIdFieldName;
		this.datasetText = datasetText;
	}
	
	/**
	 * @param datasetReceiver dataset object receiver
	 * @param datasetTextReceiver dataset plaintext receiver
	 * @param mainIdFieldName field name to be used as main identifier. Introduced because of differences between MDStore records and XML dump records.
	 * @param datasetText dataset plain text
	 */
	public DataciteDumpXmlHandler(RecordReceiver<DataSetReference> datasetReceiver,
			RecordReceiver<DocumentText> datasetTextReceiver, String datasetText) {
		this(datasetReceiver, datasetTextReceiver, ELEM_OBJ_IDENTIFIER, datasetText);
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
		    datasetMeta.setIdType(attributes.getValue(ATTRIBUTE_ID_TYPE).toLowerCase());	
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
		    datasetMeta.setResourceTypeClass(attributes.getValue(ATTRIBUTE_RESOURCE_TYPE_GENERAL));
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(localName, ELEM_ALTERNATE_IDENTIFIER, ELEM_ALTERNATE_IDENTIFIERS)) {
		    datasetMeta.setCurrentAlternateIdentifierType(attributes.getValue(ATTRIBUTE_ALTERNATE_IDENTIFIER_TYPE));
            this.currentValue = new StringBuilder();
        } 
		this.parents.push(localName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
		if (isWithinElement(localName, mainIdFieldName, ELEM_HEADER)) {
		    datasetMeta.setHeaderId(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_IDENTIFIER, ELEM_RESOURCE)) {
		    datasetMeta.setIdValue(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_CREATOR_NAME, ELEM_CREATOR) 
				&& this.currentValue.length()>0) {
			if (datasetMeta.getCreatorNames()==null) {
			    datasetMeta.setCreatorNames(new ArrayList<CharSequence>());
			}
			datasetMeta.getCreatorNames().add(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_TITLE, ELEM_TITLES)
				&& this.currentValue.length()>0) {
			if (datasetMeta.getTitles()==null) {
			    datasetMeta.setTitles(new ArrayList<CharSequence>());
			}
			datasetMeta.getTitles().add(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_FORMAT, ELEM_FORMATS)
				&& this.currentValue.length()>0) {
			if (datasetMeta.getFormats()==null) {
			    datasetMeta.setFormats(new ArrayList<CharSequence>());
			}
			datasetMeta.getFormats().add(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_ALTERNATE_IDENTIFIER, ELEM_ALTERNATE_IDENTIFIERS)
                && this.currentValue.length()>0) {
            if (datasetMeta.getCurrentAlternateIdentifierType()!=null) {
                if (datasetMeta.getAlternateIdentifiers()==null) {
                    datasetMeta.setAlternateIdentifiers(Maps.newHashMap());
                }
                datasetMeta.getAlternateIdentifiers().put(datasetMeta.getCurrentAlternateIdentifierType(), this.currentValue.toString().trim());    
            }
        } else if (isWithinElement(localName, ELEM_DESCRIPTION, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
            datasetMeta.setDescription(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_PUBLISHER, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
		    datasetMeta.setPublisher(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_PUBLICATION_YEAR, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
		    datasetMeta.setPublicationYear(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_RESOURCE_TYPE, ELEM_RESOURCE)
				&& this.currentValue.length()>0) {
		    datasetMeta.setResourceTypeValue(this.currentValue.toString().trim());
		} else if (isWithinElement(localName, ELEM_RESOURCE, ELEM_PAYLOAD) ||
//				temporary hack: the case below is for the records originated from MDStore 
//				where no 'payload' element is present, required until fixing MDStore contents
				isWithinElement(localName, ELEM_RESOURCE, ELEM_METADATA)) {
			if (datasetMeta.getIdType()!=null && datasetMeta.getIdValue()!=null) {
			    storeRecords();
			} else {
				log.warn("either reference type " + datasetMeta.getIdType() + " or id value: " + datasetMeta.getIdValue() + 
						" was null for record id: " + datasetMeta.getHeaderId());
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
            if (datasetMeta.getHeaderId()==null) {
                throw new SAXException("header identifier was not found!");
            }

            String idValueStr = datasetMeta.getIdValue().trim();
            String datasetId = ELEM_OBJ_IDENTIFIER.equals(mainIdFieldName)?
                    InfoSpaceConstants.ROW_PREFIX_RESULT + datasetMeta.getHeaderId() : datasetMeta.getHeaderId();

            DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
            documentTextBuilder.setId(datasetId);
            documentTextBuilder.setText(this.datasetText);

            DataSetReference.Builder dataSetRefBuilder = DataSetReference.newBuilder();
            dataSetRefBuilder.setId(datasetId);
            dataSetRefBuilder.setReferenceType(datasetMeta.getIdType());
            dataSetRefBuilder.setIdForGivenType(idValueStr);

            if (datasetMeta.getCreatorNames()!=null) {
                dataSetRefBuilder.setCreatorNames(datasetMeta.getCreatorNames());
            }
            if (datasetMeta.getTitles()!=null) {
                dataSetRefBuilder.setTitles(datasetMeta.getTitles());
            }
            if (datasetMeta.getFormats()!=null) {
                dataSetRefBuilder.setFormats(datasetMeta.getFormats());
            }
            if (datasetMeta.getDescription()!=null) {
                dataSetRefBuilder.setDescription(datasetMeta.getDescription());
            }
            if (datasetMeta.getPublisher()!=null) {
                dataSetRefBuilder.setPublisher(datasetMeta.getPublisher());
            }
            if (datasetMeta.getPublicationYear()!=null) {
                dataSetRefBuilder.setPublicationYear(datasetMeta.getPublicationYear());
            }
            if (datasetMeta.getResourceTypeClass()!=null) {
                dataSetRefBuilder.setResourceTypeClass(datasetMeta.getResourceTypeClass());
            }
            if (datasetMeta.getResourceTypeValue()!=null) {
                dataSetRefBuilder.setResourceTypeValue(datasetMeta.getResourceTypeValue());
            }
            if (datasetMeta.getAlternateIdentifiers()!=null) {
                dataSetRefBuilder.setAlternateIdentifiers(datasetMeta.getAlternateIdentifiers());
            }
            
            datasetReceiver.receive(dataSetRefBuilder.build());
            datasetTextReceiver.receive(documentTextBuilder.build());
            
        } catch (IOException e) {
            throw new SAXException(e);
        }
    }
	
	boolean isWithinElement(String localName, String expectedElement, String expectedParent) {
		return localName.equals(expectedElement) && !this.parents.isEmpty() && 
				expectedParent.equals(this.parents.peek());
	}
	
}
