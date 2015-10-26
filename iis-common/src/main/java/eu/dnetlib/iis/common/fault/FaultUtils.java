package eu.dnetlib.iis.common.fault;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import eu.dnetlib.iis.audit.schemas.Cause;
import eu.dnetlib.iis.audit.schemas.Fault;

/**
 * {@link Fault} related utilities.
 * @author mhorst
 *
 */
public class FaultUtils {

	/**
	 * Generates {@link Fault} instance based on {@link Throwable}.
	 * @param entityId entity identifier
	 * @param throwable 
	 * @param auditSupplementaryData
	 * @return {@link Fault} instance generated for {@link Throwable}
	 */
	public static Fault exceptionToFault(CharSequence entityId, Throwable throwable,
			Map<CharSequence, CharSequence> auditSupplementaryData) {
		Fault.Builder faultBuilder = Fault.newBuilder();
		faultBuilder.setInputObjectId(entityId);
		faultBuilder.setTimestamp(System.currentTimeMillis());
		faultBuilder.setCode(throwable.getClass().getName());
		faultBuilder.setMessage(throwable.getMessage());
		StringWriter strWriter = new StringWriter();
		PrintWriter pw = new PrintWriter(strWriter);
		throwable.printStackTrace(pw);
		pw.close();
		faultBuilder.setStackTrace(strWriter.toString());
		if (throwable.getCause()!=null) {
			faultBuilder.setCauses(appendThrowableToCauses(
					throwable.getCause(), new ArrayList<Cause>()));
		}
		if (auditSupplementaryData!=null && !auditSupplementaryData.isEmpty()) {
			faultBuilder.setSupplementaryData(auditSupplementaryData);	
		}
		return faultBuilder.build();
	}
	
	protected static List<Cause> appendThrowableToCauses(Throwable e, List<Cause> causes) {
		Cause.Builder causeBuilder = Cause.newBuilder();
		causeBuilder.setCode(e.getClass().getName());
		causeBuilder.setMessage(e.getMessage());
		causes.add(causeBuilder.build());
		if (e.getCause()!=null) {
			return appendThrowableToCauses(
					e.getCause(),causes);
		} else {
			return causes;	
		}
	}
	
}
