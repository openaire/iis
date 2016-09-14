package eu.dnetlib.iis.wf.importer.database.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.data.transform.xml.AbstractDNetXsltFunctions;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.RecordReceiver;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectConverter;

/**
 * Database project results SAX handler.
 * Notice: writer is not being closed by handler.
 * Created outside, let it be closed outside as well.
 * @author mhorst
 *
 */
public class DatabaseProjectXmlHandler extends DefaultHandler {

	private static final String ELEM_FIELD = "field";
	private static final String ELEM_ITEM = "item";
	
	private static final String ATTRIBUTE_NAME = "name";
	
	private static final String ATTRIBUTE_NAME_VALUE_ACRONYM = "acronym";
	private static final String ATTRIBUTE_NAME_VALUE_PROJECTID = "projectid";
	private static final String ATTRIBUTE_NAME_VALUE_CODE = "code";
	private static final String ATTRIBUTE_NAME_VALUE_JSONEXTRAINFO = "jsonextrainfo";
	private static final String ATTRIBUTE_NAME_VALUE_FUNDINGPATH = "fundingpath";
	
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private Stack<String> parents;
	
	private StringBuilder currentValue = new StringBuilder();
	private String currentName = null;
	
	private String projectId = null;
	private String acronym = null;
	private String code = null;
	private String jsonExtraInfo = null;
	private List<String> fundingTreeList = null;
	
	private int counter = 0;
	
	private final RecordReceiver<Project> receiver;
	
	
	/**
	 * Default constructor.
	 * @param receiver
	 */
	public DatabaseProjectXmlHandler(RecordReceiver<Project> receiver) {
		super();
		this.receiver = receiver;
	}
	
	@Override
	public void startDocument() throws SAXException {
		parents = new Stack<String>();
		clearAllFields();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (isWithinElement(qName, ELEM_FIELD, null)) {
			this.currentValue = new StringBuilder();
			this.currentName = attributes.getValue(ATTRIBUTE_NAME);
		} else if (isWithinElement(qName, ELEM_ITEM, ELEM_FIELD)) {
			this.currentValue = new StringBuilder();
		}
		this.parents.push(qName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
		
		if (isWithinElement(qName, ELEM_FIELD, null)) {
			if (ATTRIBUTE_NAME_VALUE_PROJECTID.equals(this.currentName)) {
				this.projectId = this.currentValue.toString().trim();
			} else if (ATTRIBUTE_NAME_VALUE_ACRONYM.equals(this.currentName)) {
				this.acronym = this.currentValue.toString().trim();
			} else if (ATTRIBUTE_NAME_VALUE_CODE.equals(this.currentName)) {
				this.code = this.currentValue.toString().trim();
			} else if (ATTRIBUTE_NAME_VALUE_JSONEXTRAINFO.equals(this.currentName)) {
                this.jsonExtraInfo = this.currentValue.toString().trim();
			}
		} else if (isWithinElement(qName, ELEM_ITEM, ELEM_FIELD) &&
		        ATTRIBUTE_NAME_VALUE_FUNDINGPATH.equals(this.currentName)) {
			if (fundingTreeList==null) {
				fundingTreeList = new ArrayList<String>();
			}
			fundingTreeList.add(this.currentValue.toString().trim());
		}
		
//		resetting current value;
		this.currentValue = null;
		this.currentName = null;
	}

	private void clearAllFields() {
		this.currentName = null;
		
		this.projectId = null;
		this.acronym = null;
		this.code = null;
		this.jsonExtraInfo = null;
		this.fundingTreeList = null;
	}
	
	boolean isWithinElement(String qName,
			String expectedElement, String expectedParent) {
		return qName.equalsIgnoreCase(expectedElement) && 
				(expectedParent==null || 
				(!this.parents.isEmpty() && expectedParent.equalsIgnoreCase(this.parents.peek())));
	}
	
	@Override
	public void endDocument() throws SAXException {
		Project.Builder projectBuilder = Project.newBuilder();
		if (this.projectId!=null) {
			String[] tokenizedProjectId = StringUtils.splitByWholeSeparator(
					this.projectId, HBaseConstants.ID_NAMESPACE_SEPARATOR);
			if (tokenizedProjectId==null || tokenizedProjectId.length!=2) {
				throw new SAXException("unexpected projectId format: " + this.projectId + 
						", unable to split into two by " + HBaseConstants.ID_NAMESPACE_SEPARATOR);
			}
			projectBuilder.setId(AbstractDNetXsltFunctions.oafId(
					Type.project.name(), 
					tokenizedProjectId[0], tokenizedProjectId[1]));	
		}
		if (ProjectConverter.isAcronymValid(this.acronym)) {
			projectBuilder.setProjectAcronym(this.acronym);	
		}
		if (StringUtils.isNotBlank(this.code)) {
			projectBuilder.setProjectGrantId(this.code);	
		}
		if (StringUtils.isNotBlank(this.jsonExtraInfo)) {
            projectBuilder.setJsonextrainfo(this.jsonExtraInfo);    
        }
		
		if (this.fundingTreeList!=null && this.fundingTreeList.size()>0) {
			try {
				projectBuilder.setFundingClass(
						ProjectConverter.extractFundingClass(fundingTreeList));	
			} catch (IOException e) {
				throw new SAXException(
						"Exception occurred when parsing funding tree JSON object", e);
			}
		}
		
		try {
			receiver.receive(projectBuilder.build());
		} catch (IOException e) {
			throw new SAXException(
					"Exception occurred when building project object", e);
		}
		counter++;
		if (counter%100000==0) {
			log.debug("current progress: " + counter);
		}
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
