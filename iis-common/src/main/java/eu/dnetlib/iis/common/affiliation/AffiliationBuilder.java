package eu.dnetlib.iis.common.affiliation;

import java.util.List;

import org.jdom.Element;

import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;

/**
 * Affiliation builder.
 * @author mhorst
 *
 */
public class AffiliationBuilder {

	
	/**
	 * Creates affiliation object based on node.
	 * @param node
	 * @return affiliation object
	 */
	public static Affiliation build(Element node) {
		String affId = node.getAttributeValue("id");
        String country = node.getChildText("country");
        String countryCode = null;
        if (node.getChild("country") != null) {
            countryCode = node.getChild("country").getAttributeValue("country");
        }
        String address = node.getChildText("addr-line");
        StringBuilder sb = new StringBuilder();
        List<?> institutions = node.getChildren("institution");
        for (Object institution : institutions) {
            if (!sb.toString().isEmpty()) {
                sb.append(", ");
            }
            sb.append(((Element) institution).getTextTrim());
        }
        String institution = sb.toString().replaceFirst(", $", "");
        if (institution.isEmpty()) {
            institution = null;
        }
        
        return Affiliation.newBuilder()
                .setRawText(node.getValue().trim().replaceFirst(affId, "").trim())
                .setOrganization(institution!=null?institution.trim():null)
                .setAddress(address!=null?address.trim():null)
                .setCountryName(country!=null?country.trim():null)
                .setCountryCode(countryCode)
                .build();
	}
	
}
