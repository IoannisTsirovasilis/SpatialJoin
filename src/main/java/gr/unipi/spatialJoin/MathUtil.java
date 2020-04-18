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
	
	// 0            1             2
	//   ------------------------
	//   |						| 
	// 7 |						| 3
	//	 |						|
	//	 ------------------------
	// 6            5             4                
	public static double pointToRectangleDistance(Point point, Point lowerLeft, Point upperRight) {
		double dLon = Math.max(lowerLeft.getX() - point.getX(), 0);
		double dLat = Math.max(lowerLeft.getY() - point.getY(), 0);
		
		if (dLon > 0) {
			// case 6
			if (dLat > 0) {
				return haversineDistance(point.getY(), lowerLeft.getY(), point.getX(), lowerLeft.getX());
			}
			
			dLat = Math.max(dLat, point.getY() - upperRight.getY());
			
			// case 7
			if (dLat == 0) {
				return haversineDistance(point.getY(), point.getY(), point.getX(), lowerLeft.getX());
			}
			
			// case 0
			return haversineDistance(point.getY(), upperRight.getY(), point.getX(), lowerLeft.getX());
		} 
		
		dLon = Math.max(dLon, point.getX() - upperRight.getX());
		
		if (dLon == 0) {
			// case 5
			if (dLat > 0) {
				return haversineDistance(point.getY(), lowerLeft.getY(), point.getX(), point.getX());
			}
						
			// case 1
			return haversineDistance(point.getY(), upperRight.getY(), point.getX(), point.getX());
		}
		
		// case 4
		if (dLat > 0) {
			return haversineDistance(point.getY(), lowerLeft.getY(), point.getX(), upperRight.getX());
		}
		
		dLat = Math.max(dLat, point.getY() - upperRight.getY());
		
		// case 3
		if (dLat == 0) {
			return haversineDistance(point.getY(), point.getY(), point.getX(), upperRight.getX());
		}
		
		
		// case 2
		return haversineDistance(point.getY(), upperRight.getY(), point.getX(), upperRight.getX());
	}	
	
	private static double toRad(double value) {
		return value * Math.PI / 180;
	}
}
