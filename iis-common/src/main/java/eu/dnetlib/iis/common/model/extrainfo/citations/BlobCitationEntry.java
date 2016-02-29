package eu.dnetlib.iis.common.model.extrainfo.citations;

import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * Comparable citation entry.
 * @author mhorst
 *
 */
@XStreamAlias("citation")
public class BlobCitationEntry implements Comparable<BlobCitationEntry> {

	/**
	 * Citation position.
	 */
	@XStreamAsAttribute
	protected int position;
	
	/**
	 * Raw citation text.
	 */
	protected String rawText;
	
	/**
	 * Matched publications identifiers.
	 */
	@XStreamImplicit
	protected List<TypedId> identifiers;
	
	@XStreamOmitField
	private final static Pattern alphaNumChunkPattern = Pattern.compile("(\\d+\\" + 
			new DecimalFormatSymbols(Locale.getDefault()).getDecimalSeparator() + "\\d+)|(\\d+)|(\\D+)");;
	
	
	public BlobCitationEntry() {
		super();
	}

	public BlobCitationEntry(String rawText) {
		this.rawText = rawText;
	}
	
	public String getRawText() {
		return rawText;
	}

	public void setRawText(String rawText) {
		this.rawText = rawText;
	}

	public List<TypedId> getIdentifiers() {
		return identifiers;
	}

	public void setIdentifiers(List<TypedId> identifiers) {
		this.identifiers = identifiers;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((identifiers == null) ? 0 : identifiers.hashCode());
		result = prime * result + position;
		result = prime * result + ((rawText == null) ? 0 : rawText.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BlobCitationEntry other = (BlobCitationEntry) obj;
		if (identifiers == null) {
			if (other.identifiers != null)
				return false;
		} else if (!identifiers.equals(other.identifiers))
			return false;
		if (position != other.position)
			return false;
		if (rawText == null) {
			if (other.rawText != null)
				return false;
		} else if (!rawText.equals(other.rawText))
			return false;
		return true;
	}

	@Override
	public int compareTo(BlobCitationEntry c2) {
		if (c2!=null) {
			if (this.position>c2.position) {
				return 1;
			} else if (this.position<c2.position) {
				return -1;
			} else {
				   if (this.getRawText()!=null) {
						if (c2.getRawText()!=null) {
							int textCompareResult = compareText(this.getRawText(), c2.getRawText());
							if (textCompareResult==0) {
								return compareIdentifiers(this.getIdentifiers(), c2.getIdentifiers());
							} else {
								return textCompareResult;	
							}
						} else {
							return -1;
						}
					} else {
						if (c2.getRawText()!=null) {
							return 1;
						} else {
							return compareIdentifiers(this.getIdentifiers(), c2.getIdentifiers());
						}
					}   
				
			}
		} else {
			if (this.getRawText()!=null) {
				return -1;	
			} else {
				if (this.getIdentifiers()!=null) {
					return -1;
				} else {
//					should we check position in any way?
					return 0;
				}
			}
		}
	}

	private int compareIdentifiers(List<TypedId> ids1, List<TypedId> ids2) {
		   if (ids2!=null) {
			   if (ids1!=null) {
				   if (ids1.equals(ids2)) {
					   return 0;
				   } else {
//					   no matter what value, order by ids is irrelevant
//					   we have to return non-zero value to prevent treating objects as the same
					   return 1;
				   }
			   } else {
				   return -1;
			   }
			} else {
				if (ids1!=null) {
					return 1;
				} else {
					return 0;
				}
			}
	   }
	   
	   private int compareText(String s1, String s2) {
	      int compareValue = 0;
	      Matcher s1ChunkMatcher = alphaNumChunkPattern.matcher(s1);
	      Matcher s2ChunkMatcher = alphaNumChunkPattern.matcher(s2);
	      String s1ChunkValue = null;
	      String s2ChunkValue = null;
	      while (s1ChunkMatcher.find() && s2ChunkMatcher.find() && compareValue == 0) {
	         s1ChunkValue = s1ChunkMatcher.group();
	         s2ChunkValue = s2ChunkMatcher.group();
	         try {
	            // compare double values - ints get converted to doubles. Eg. 100 = 100.0
	            Double s1Double = Double.valueOf(s1ChunkValue);
	            Double s2Double = Double.valueOf(s2ChunkValue);
	            compareValue = s1Double.compareTo(s2Double);
	         } catch (NumberFormatException e) {
	            // not a number, use string comparison.
	            compareValue = s1ChunkValue.compareTo(s2ChunkValue);
	         }
	         // if they are equal thus far, but one has more left, it should come after the one that doesn't.
	         if (compareValue == 0) {
	            if (s1ChunkMatcher.hitEnd() && !s2ChunkMatcher.hitEnd()) {
	               compareValue = -1;
	            } else if (!s1ChunkMatcher.hitEnd() && s2ChunkMatcher.hitEnd()) {
	               compareValue = 1;
	            }
	         }
	      }
	      return compareValue;
	   }

	/**
	 * @return the position
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * @param position the position to set
	 */
	public void setPosition(int position) {
		this.position = position;
	}
}
