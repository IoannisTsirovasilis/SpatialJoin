package gr.unipi.spatialJoin;

public class GridCell {
	private Point upperRight;
	private Point lowerLeft;
	private String id;
	private GridCell[] adjacentCells; 
	
	public GridCell(Point lowerLeft, Point upperRight, String id) {
		this.upperRight = upperRight;
		this.lowerLeft = lowerLeft;
		this.id = id;
	}
	
	public Point getUpperRight() {
		return this.upperRight;
	}
	
	public Point getLowerLeft() {
		return this.lowerLeft;
	}
	
	public String getId() {
		return this.id;
	}
	
	// https://stackoverflow.com/questions/5254838/calculating-distance-between-a-point-and-a-rectangular-box-nearest-point
	public boolean isInDistance(double lat, double lon, double radius) {
		return MathUtil.pointToRectangleDistance(new Point(lon, lat), this.lowerLeft, this.upperRight) <= radius;
	}
	
	public boolean contains(double lat, double lon) {
		if (this.lowerLeft.getY() <= lat && this.lowerLeft.getX() <= lon
				&& this.upperRight.getY() >= lat && this.upperRight.getX() >= lon) {
			return true;
		}
		
		return false;
	}
	
	public boolean areEqual(GridCell gc) {		
		if (this.lowerLeft.getX() != gc.lowerLeft.getX() || this.lowerLeft.getY() != gc.lowerLeft.getY()) {
			return false;
		}
		
		if (this.upperRight.getX() != gc.upperRight.getX() || this.upperRight.getY() != gc.upperRight.getY()) {
			return false;
		}
		
		return true;
	}
}
