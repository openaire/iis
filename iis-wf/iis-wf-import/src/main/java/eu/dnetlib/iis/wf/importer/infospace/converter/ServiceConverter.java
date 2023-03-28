package eu.dnetlib.iis.wf.importer.infospace.converter;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.iis.importer.schemas.Service;

/**
 * Converter of {@link eu.dnetlib.dhp.schema.oaf.Datasource} object into
 * {@link Service}
 * 
 * 
 * @author Marek Horst
 */

public class ServiceConverter implements OafEntityToAvroConverter<eu.dnetlib.dhp.schema.oaf.Datasource, Service> {

	public static final String EOSCTYPE_ID_SERVICE = "Service";

	private static final long serialVersionUID = 2775743245820729376L;

	// ------------------------ LOGIC --------------------------

	/**
	 * Converts {@link eu.dnetlib.dhp.schema.oaf.Datasource} object into
	 * {@link Service}
	 */
	@Override
	public Service convert(eu.dnetlib.dhp.schema.oaf.Datasource datasource) {

		Preconditions.checkNotNull(datasource);

		if (!isService(datasource)) {
			return null;
		}

		Service.Builder serviceBuilder = Service.newBuilder();

		serviceBuilder.setId(datasource.getId());
		serviceBuilder.setName(isStringFieldNotNull(datasource.getOfficialname()) ? datasource.getOfficialname().getValue() : null);
		serviceBuilder.setUrl(isStringFieldNotNull(datasource.getWebsiteurl()) ? datasource.getWebsiteurl().getValue() : null);

		return serviceBuilder.build();
	}

	// ------------------------ PRIVATE --------------------------

	private static boolean isStringFieldNotNull(Field<String> field) {
		return field != null && StringUtils.isNotBlank(field.getValue());
	}

	private static boolean isService(eu.dnetlib.dhp.schema.oaf.Datasource srcOrganization) {

		if (srcOrganization.getEosctype() != null
				&& EOSCTYPE_ID_SERVICE.equals(srcOrganization.getEosctype().getClassid())) {

			return true;
		}

		return false;
	}

}
