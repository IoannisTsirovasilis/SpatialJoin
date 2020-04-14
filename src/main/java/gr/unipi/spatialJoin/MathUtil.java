package gr.unipi.spatialJoin;

public class MathUtil {
	private static final int R = 6371; // Radius of the earth
	
	// https://gist.github.com/vananth22/888ed9a22105670e7a4092bdcf0d72e4
	public static double haversineDistance(double lat1, double lat2, double lon1, double lon2) {		 
		double latDistance = toRad(lat2-lat1);
		double lonDistance = toRad(lon2-lon1);
		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + 
				Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
				Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		double distance = R * c;
		
		return distance;
	}
	
	private static double toRad(double value) {
		return value * Math.PI / 180;
	}
}
