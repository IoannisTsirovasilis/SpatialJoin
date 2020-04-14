package gr.unipi.spatialJoin;

public class GridCell {
	private Point upperLeft;
	private Point upperRight;
	private Point lowerLeft;
	private Point lowerRight;
	
	public GridCell(Point lowerLeft, Point lowerRight, Point upperLeft, Point upperRight) {
		this.upperLeft = upperLeft;
		this.upperRight = upperRight;
		this.lowerLeft = lowerLeft;
		this.lowerRight = lowerRight;
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
