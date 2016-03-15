package eu.dnetlib.iis.wf.importer.output;


/**
 * Stateful counter based filename generator.
 * @author mhorst
 *
 */
public class CounterBasedFilenameGenerator<T> implements FilenameGenerator<T> {

	/**
	 * Maximum filename length without suffix.
	 */
	private int maxFileNameLength = 8; 
	
	/**
	 * Default filename suffix.
	 */
	private String defaultSuffix = "pbuf";
			
	/**
	 * Internal counter.
	 */
	private volatile int counter=0;
	
	/* (non-Javadoc)
	 * @see eu.dnetlib.iis.imp.output.FilenameGenerator#generateFileName(java.lang.Object)
	 */
	@Override
	public String generateFileName(T source) {
		StringBuffer strBuff = new StringBuffer();
		fillWithZeroes(strBuff.append(++counter)).append('.').append(defaultSuffix);
		return strBuff.toString();
	}
	
	
	/**
	 * Fills {@link StringBuffer} with zeroes at its beggining.
	 * @param strBuff
	 * @return {@link StringBuffer} filled in with zeroes
	 */
	protected StringBuffer fillWithZeroes(StringBuffer strBuff) {
		while(strBuff.length()<maxFileNameLength) {
			strBuff.insert(0, 0);
		}
		return strBuff;
	}


	public void setDefaultSuffix(String defaultSuffix) {
		this.defaultSuffix = defaultSuffix;
	}

}
