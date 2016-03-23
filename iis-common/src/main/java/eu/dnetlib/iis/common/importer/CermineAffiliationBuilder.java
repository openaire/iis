package eu.dnetlib.iis.common.importer;

import static org.apache.commons.lang3.StringUtils.trim;

import java.util.List;

import org.jdom.Element;

import com.google.common.base.Preconditions;

/**
 * Builder of {@link CermineAffiliation} from cermine parsed affiliation
 * 
 * @author mhorst
 * @author Łukasz Dumiszewski 
 * 
 *
 */
public class CermineAffiliationBuilder {

	
	
	//------------------------ LOGIC --------------------------
	

    /**
	 * Creates {@link CermineAffiliation} based on the given node. It is assumed that the node is the result
	 * of the cermine parsing of some affiliation text. 
	 */
	public CermineAffiliation build(Element affNode) {
	    
        Preconditions.checkNotNull(affNode);
        
        CermineAffiliation aff = new CermineAffiliation();
	    
        extractRawText(affNode, aff);
        
        extractInstitution(affNode, aff);

        extractCountry(affNode, aff);
        
        extractAddress(affNode, aff);
	    
        return aff;
    }


	
	//------------------------ PRIVATE --------------------------
	
    
    private static void extractInstitution(Element affNode, CermineAffiliation aff) {
        @SuppressWarnings("unchecked")
        List<Element> institutionNodes = (List<Element>)affNode.getChildren("institution");
        aff.setInstitution(institutionNodes.stream().map(i->i.getTextTrim()).reduce((s1, s2) -> s1 + ", " + s2).orElse(null));
    }

    
    private static void extractCountry(Element affNode, CermineAffiliation aff) {
        Element countryNode = affNode.getChild("country");

        if (countryNode != null) {
            aff.setCountryName(trim(countryNode.getText()));
            aff.setCountryCode(countryNode.getAttributeValue("country"));
        }
    }
    
    
    private static void extractAddress(Element affNode, CermineAffiliation aff) {
        aff.setAddress(trim(affNode.getChildText("addr-line")));
    }

    
    private static void extractRawText(Element affNode, CermineAffiliation aff) {
        String affId = affNode.getAttributeValue("id");
        aff.setRawText(affNode.getValue().replaceFirst(affId, "").trim());
    }




}
