package gr.unipi.spatial_join;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

public class MinMaxCoordsParser {
	private double latitude;
	private double longitude;	
	
	public void parse(Text record, int latitudeIndice, int longitudeIndice, String separator) {
		parse(record.toString(), latitudeIndice, longitudeIndice, separator);
	}
	
	private void parse(String record, int latitudeIndice, int longitudeIndice, String separator) {
		latitude = Double.parseDouble(getStringBetweenSeparators(record, latitudeIndice, separator));
		longitude = Double.parseDouble(getStringBetweenSeparators(record, longitudeIndice, separator));
	}
	
	private String getStringBetweenSeparators(String record, int indice, String separator) {
		if (indice == 0) {
			return record.substring(0, record.indexOf(separator));
		}
		
		
		int ordinalIndex = StringUtils.ordinalIndexOf(record, separator, indice);
		if (ordinalIndex == StringUtils.lastOrdinalIndexOf(record, separator, 1))
		{
			return record.substring(ordinalIndex + 1);
		}
		
		return record.substring(ordinalIndex + 1, StringUtils.ordinalIndexOf(record, separator, indice + 1));		
	}
	
	public double getLatitude() {
		return latitude;
	}
	
	public double getLongitude() {
		return longitude;
	}
}
