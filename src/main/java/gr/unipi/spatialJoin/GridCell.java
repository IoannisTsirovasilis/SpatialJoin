package gr.unipi.spatialJoin;

public class GridCell {
	private Point upperLeft;
	private Point upperRight;
	private Point lowerLeft;
	private Point lowerRight;
	private String id;
	
	public GridCell(Point lowerLeft, Point upperRight, String id) {
		this.upperRight = upperRight;
		this.lowerLeft = lowerLeft;
		this.id = id;
	}
	
	public GridCell(Point lowerLeft, Point lowerRight, Point upperLeft, Point upperRight, String id) {
		this.upperLeft = upperLeft;
		this.upperRight = upperRight;
		this.lowerLeft = lowerLeft;
		this.lowerRight = lowerRight;
		this.id = id;
	}
	
	public Point getUpperLeft() {
		return this.upperLeft;
	}
	
	public Point getUpperRight() {
		return this.upperRight;
	}
	
	public Point getLowerLeft() {
		return this.lowerLeft;
	}
	
	public Point getLowerRight() {
		return this.lowerRight;
	}
	
	public String getId() {
		return this.id;
	}
	
	// https://stackoverflow.com/questions/5254838/calculating-distance-between-a-point-and-a-rectangular-box-nearest-point
	public boolean isInDistance(double lat, double lon, double radius) {
		double dx = Math.max(Math.max(this.lowerLeft.getX() - lat, 0), lat - this.upperRight.getX());
		double dy = Math.max(Math.max(this.lowerLeft.getY() - lon, 0), lon- this.upperRight.getY());
		
		return Math.sqrt(dx * dx + dy * dy) <= radius;
	}
	
	public boolean contains(double lat, double lon) {
		if (this.lowerLeft.getX() <= lat && this.lowerLeft.getY() <= lon
				&& this.upperRight.getX() >= lat && this.upperRight.getY() >= lon) {
			return true;
		}
		
		return false;
	}
	
	public boolean areEqual(GridCell gc) {
		if (this.upperLeft.getX() != gc.upperLeft.getX() || this.upperLeft.getY() != gc.upperLeft.getY()) {
			return false;
		}
		
		if (this.lowerLeft.getX() != gc.lowerLeft.getX() || this.lowerLeft.getY() != gc.lowerLeft.getY()) {
			return false;
		}
		
		if (this.upperRight.getX() != gc.upperRight.getX() || this.upperRight.getY() != gc.upperRight.getY()) {
			return false;
		}
		
		if (this.lowerRight.getX() != gc.lowerRight.getX() || this.lowerRight.getY() != gc.lowerRight.getY()) {
			return false;
		}
		
		return true;
	}
}
