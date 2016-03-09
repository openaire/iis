package eu.dnetlib.iis.wf.importer.converter;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * HBase {@link Result} to avro {@link Project} converter.
 * @author mhorst
 *
 */
public class ProjectConverter extends AbstractAvroConverter<Project> {

	protected static final Logger log = Logger.getLogger(ProjectConverter.class);
	
	private static final String ELEM_FUNDING_TREE_PARENT = "parent";
	private static final String ELEM_FUNDING_TREE_NAME = "name";
	
	private static final String FUNDER_FUNDING_SEPARATOR = "::";

	private static final Set<String> ACRONYM_SKIP_LOWERCASED_VALUES = new HashSet<String>(
			Arrays.asList("undefined", "unknown"));
	
	/**
	 * Default constructor.
	 * @param encoding
	 * @param resultApprover
	 */
	public ProjectConverter(String encoding,
			ResultApprover resultApprover) {
		super(encoding, resultApprover);
	}

	@Override
	public Project buildObject(Result source, Oaf resolvedOafObject) throws IOException {
		eu.dnetlib.data.proto.ProjectProtos.Project sourceProject = resolvedOafObject.getEntity()!=null?
				resolvedOafObject.getEntity().getProject():null;
		if (sourceProject==null) {
			log.error("skipping: no project object " +
					"for a row " + new String(source.getRow(), getEncoding()));
			return null;
		}
		if (resolvedOafObject.getEntity().getId()!=null && 
				!resolvedOafObject.getEntity().getId().isEmpty()) {
			Project.Builder builder = Project.newBuilder();
			builder.setId(resolvedOafObject.getEntity().getId());
			if (sourceProject.getMetadata()!=null) {
				if (isAcronymValid(sourceProject.getMetadata().getAcronym())) {
					builder.setProjectAcronym(sourceProject.getMetadata().getAcronym().getValue());
				}
				if (sourceProject.getMetadata().getCode()!=null &&
						sourceProject.getMetadata().getCode().getValue()!=null &&
						!sourceProject.getMetadata().getCode().getValue().isEmpty()) {
					builder.setProjectGrantId(sourceProject.getMetadata().getCode().getValue());
				}
				String extractedFundingClass = extractFundingClass(
						extractStringValues(sourceProject.getMetadata().getFundingtreeList()));
				if (extractedFundingClass!=null && !extractedFundingClass.isEmpty()) {
					builder.setFundingClass(extractedFundingClass);	
				}
			}
			return builder.build();	
		} else {
			log.warn("unable to extract grant number: " +
					"unsupported project id: " + resolvedOafObject.getEntity().getId());
			return null;
		}
	}

	/**
	 * Extracts string values from {@link StringField} list.
	 * @param source
	 * @return string values extracted from {@link StringField} list
	 */
	protected static List<String> extractStringValues(List<StringField> source) {
		if (source!=null) {
			List<String> results = new ArrayList<String>(source.size());
			for (StringField currentField : source) {
				results.add(currentField.getValue());
			}
			return results;
		} else {
			return null;
		}
	}
	
	/**
	 * Verifies whether acronym should be considered as valid.
	 * @param acronym
	 * @return true if valid, false otherwise
	 */
	private static boolean isAcronymValid(StringField acronym) {
		return acronym!=null && isAcronymValid(acronym.getValue());
	}
	
	/**
	 * Verifies whether acronym should be considered as valid.
	 * @param acronym
	 * @return true if valid, false otherwise
	 */
	public static boolean isAcronymValid(String acronym) {
		return acronym!=null && !acronym.isEmpty() && 
				!ACRONYM_SKIP_LOWERCASED_VALUES.contains(acronym.trim().toLowerCase());
	}
	
	/**
	 * Extracts funding class from funding tree defined as XML.
	 * @param fundingTreeXML
	 * @return extracted funding class
	 * @throws IOException 
	 */
	public static String extractFundingClassFromXML(Collection<String> fundingTreeXMLList) throws IOException {
		if (fundingTreeXMLList!=null && fundingTreeXMLList.size()>0) {
			for (String fundingTreeXML : fundingTreeXMLList) {
				if (fundingTreeXML!=null && !fundingTreeXML.isEmpty()) {
					DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
					try {
						DocumentBuilder builder = builderFactory.newDocumentBuilder();
						Document xmlDocument = builder.parse(new InputSource(new StringReader(fundingTreeXML)));
						XPath xPath =  XPathFactory.newInstance().newXPath();
						StringBuilder strBuilder = new StringBuilder();
						strBuilder.append(xPath.compile("//funder/shortname").evaluate(xmlDocument));
						strBuilder.append(FUNDER_FUNDING_SEPARATOR);
						strBuilder.append(xPath.compile("//funding_level_0/name").evaluate(xmlDocument));
						return strBuilder.toString();
					} catch (ParserConfigurationException e) {
					    throw new IOException("exception occurred when processing xml: " + fundingTreeXML, e);
					} catch (SAXException e) {
						throw new IOException("exception occurred when processing xml: " + fundingTreeXML, e);
					} catch (XPathExpressionException e) {
						throw new IOException("exception occurred when processing xml: " + fundingTreeXML, e);
					}	
				}
			}
		}
//		fallback
		return null;
	}
	
	/**
	 * Extracts funding class from funding tree.
	 * @param fundingTreeList
	 * @return extracted funding class
	 * @throws IOException 
	 */
	public static String extractFundingClass(List<String> fundingTreeList) throws IOException {
		return extractFundingClassFromXML(fundingTreeList);
	}
	
}
