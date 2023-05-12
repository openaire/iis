package eu.dnetlib.iis.wf.importer.infospace.converter;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.iis.importer.schemas.Service;

/**
 * Converter of {@link eu.dnetlib.dhp.schema.oaf.Datasource} object into {@link Service}.
 * Apart from conversion there is also filtering rule applied restricting the set of 
 * eligible datasources to the ones collected from the specific source.
 * 
 * 
 * @author Marek Horst
 */

public class ServiceConverter implements OafEntityToAvroConverter<eu.dnetlib.dhp.schema.oaf.Datasource, Service> {

	public static final String EOSCTYPE_ID_SERVICE = "Service";

	private static final long serialVersionUID = 2775743245820729376L;
	
	private final String eligibleCollectedFromDatasourceId;

	// ------------------------ CONSTRUCTORS --------------------------

	/**
	 * @param eligibleCollectedFromId eligible datasource identifier given record was collected from 
	 */
	public ServiceConverter(String eligibleCollectedFromDatasourceId) {
		this.eligibleCollectedFromDatasourceId = eligibleCollectedFromDatasourceId;
	}
	
	// ------------------------ LOGIC --------------------------

	/**
	 * Converts {@link eu.dnetlib.dhp.schema.oaf.Datasource} object into {@link Service}.
	 * Returns null when the input datasource is not considered as a service. 
	 */
	@Override
	public Service convert(eu.dnetlib.dhp.schema.oaf.Datasource datasource) {

		Preconditions.checkNotNull(datasource);

		if (!isEligibleDatasource(datasource)) {
			return null;
		}

		Service.Builder serviceBuilder = Service.newBuilder();

		serviceBuilder.setId(datasource.getId());
		serviceBuilder.setName(isStringFieldMissing(datasource.getOfficialname()) ? null : datasource.getOfficialname().getValue());
		serviceBuilder.setUrl(datasource.getWebsiteurl().getValue());

		return serviceBuilder.build();
	}

	// ------------------------ PRIVATE --------------------------

	private static boolean isStringFieldMissing(Field<String> field) {
		return field == null || StringUtils.isBlank(field.getValue());
	}

	private boolean isEligibleDatasource(eu.dnetlib.dhp.schema.oaf.Datasource datasource) {
		if (isStringFieldMissing(datasource.getWebsiteurl())) {
			return false;
		}
		
		if (eligibleCollectedFromDatasourceId != null) {
			for (KeyValue currentCollectedFrom : datasource.getCollectedfrom()) {
				if (eligibleCollectedFromDatasourceId.equals(currentCollectedFrom.getKey())) {
					return true;
				}
			}
			return false;
		} else {
			return true;
		}
		
	}

}
