package gr.unipi.spatialJoin;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class GridBuilderTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public GridBuilderTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( GridBuilderTest.class );
    }
    
    public void testBuildGridCreatesProperGrid() {
    	int hSectors = 3;
    	int vSectors = 2;
    	GridBuilder gb = new GridBuilder(1.4, 0, 6, 0, 10, hSectors, vSectors);
    	double hStep = 10.0 / hSectors;
    	double vStep = 6.0 / vSectors;
    	Point point00 = new Point(0, 0);
    	Point point01 = new Point(0 + hStep, 0);
    	Point point02 = new Point(0 + 2 * hStep, 0);
    	Point point10 = new Point(0, 0 + vStep);
    	Point point11 = new Point(0 + hStep, 0 + vStep);
    	Point point12 = new Point(0 + 2 * hStep, 0 + vStep);
    	Point point13 = new Point(0 + 3 * hStep, 0 + vStep);
    	Point point21 = new Point(0 + hStep, 0 + 2 * vStep);
    	Point point22 = new Point(0 + 2 * hStep, 0 + 2 * vStep);
    	Point point23 = new Point(0 + 3 * hStep, 0 + 2 * vStep);
    	
    	gb.buildGrid();
    	
    	assertEquals(2, gb.getGridCells().length);
    	assertEquals(3, gb.getGridCells()[0].length);
    	
    	GridCell[][] actualGridCells = new GridCell[2][3];
    	actualGridCells[0][0] = new GridCell(point00, point11, "0");
    	actualGridCells[0][1] = new GridCell(point01, point12, "1");
    	actualGridCells[0][2] = new GridCell(point02, point13, "2");
    	actualGridCells[1][0] = new GridCell(point10, point21, "3");
    	actualGridCells[1][1] = new GridCell(point11, point22, "4");
    	actualGridCells[1][2] = new GridCell(point12, point23, "5");
    	
    	for (int i = 0; i < gb.getGridCells().length; i++) {
    		for (int j = 0; j < gb.getGridCells()[0].length; j ++) {
    			assertEquals(true, actualGridCells[i][j].areEqual(gb.getGridCells()[i][j]));
    		}
    	}
    }
}