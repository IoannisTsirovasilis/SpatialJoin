package gr.unipi.spatialJoin;

public class GridBuilder {
	private double radius;
	private double minLat;
	private double maxLat;
	private double minLon;
	private double maxLon;	
	private GridCell[][] gridCells;
	
	public GridBuilder(double radius, double minLat, double maxLat, double minLon, double maxLon) {
		this.radius = radius;
		this.minLat = minLat;
		this.maxLat = maxLat;
		this.minLon = minLon;
		this.maxLon = maxLon;
	}
	
	public void buildGrid() {
		double vDistance = MathUtil.haversineDistance(this.minLat, this.maxLat, this.minLon, this.minLon);
		double hDistance = MathUtil.haversineDistance(this.minLat, this.minLat, this.minLon, this.maxLon);
		
		// The data won't always reside in a square area. So we may have to 
		// draw different number of sectors at each dimension
		int hSectors = (int)(Math.floor(hDistance / (2 * this.radius))); 
		int vSectors = (int)(Math.floor(vDistance / (2 * this.radius)));
		
		double vStep = (this.maxLat - this.minLat) / vSectors;
		double hStep = (this.maxLon - this.minLon) / hSectors;
		
		Point lowerLeft;
		Point upperRight;
		
		gridCells = new GridCell[vSectors][hSectors];
		short id = 0;
		for (int i = 0; i < vSectors; i++) {
			for (int j = 0; j < hSectors; j++) {	
				// the three following cases ensure that there won't be any
				// calculation errors (caused by floating-point precision) that will
				// result in creating a grid slightly smaller than intended
				
				// case when building top-right cell
				if (j == hSectors - 1 && i == vSectors - 1) {
					lowerLeft = new Point(this.minLon + j * hStep, this.minLat + i * vStep);
					upperRight = new Point(this.maxLon, this.maxLat);
					gridCells[i][j] = new GridCell(lowerLeft, upperRight, Short.toString(id++));
					continue;
				} 
				
				// case when building cells on the right side of the grid
				if (j == hSectors -1) {
					lowerLeft = new Point(this.minLon + j * hStep, this.minLat + i * vStep);
					upperRight = new Point(this.maxLon, this.minLat + (i + 1) * vStep);
					gridCells[i][j] = new GridCell(lowerLeft, upperRight, Short.toString(id++));
					continue;
				}
				
				
				// case when building cells on the top side of the grid
				if (i == vSectors - 1) {
					lowerLeft = new Point(this.minLon + j * hStep, this.minLat + i * vStep);
					upperRight = new Point(this.minLon + (j + 1) * hStep, this.maxLat);
					gridCells[i][j] = new GridCell(lowerLeft, upperRight, Short.toString(id++));
					continue;
				}
				
				// case when building the remaining cells
				lowerLeft = new Point(this.minLon + j * hStep, this.minLat + i * vStep);
				upperRight = new Point(this.minLon + (j + 1) * hStep, this.minLat + (i + 1) * vStep);
				gridCells[i][j] = new GridCell(lowerLeft, upperRight, Short.toString(id++));
			}
		}
	}
	
	public GridCell[][] getGridCells() {
		return this.gridCells;
	}	
}
