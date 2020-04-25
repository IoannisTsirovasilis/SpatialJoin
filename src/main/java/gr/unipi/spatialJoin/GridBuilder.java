package gr.unipi.spatialJoin;

public class GridBuilder {
	private double radius;
	private double minLat;
	private double maxLat;
	private double minLon;
	private double maxLon;	
	private GridCell[][] gridCells;
	private int hSectors;
	private int vSectors;
	
	public GridBuilder(double radius, double minLat, double maxLat, double minLon, double maxLon, int hSectors, int vSectors) {
		this.radius = radius;
		this.minLat = minLat;
		this.maxLat = maxLat;
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.hSectors = hSectors;
		this.vSectors = vSectors;
	}
	
	public void buildGrid() {		
		
		double vStep = (this.maxLat - this.minLat) / this.vSectors;
		double hStep = (this.maxLon - this.minLon) / this.hSectors;
		
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
	
	public int getVSectors() {
		return this.vSectors;
	}
	
	public int getHSectors() {
		return this.hSectors;
	}
	
	public GridCell[][] getGridCells() {
		return this.gridCells;
	}	
}
