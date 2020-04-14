package gr.unipi.spatialJoin;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class GridCellTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public GridCellTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( GridCellTest.class );
    }
    
    public void testContainsPointInRectangle() {
    	Point lf = new Point(0, 0);
    	Point ur = new Point(1, 1);
    	GridCell cell = new GridCell(lf, ur);
    	Point sample = new Point(0, 0.5);
    	assertEquals(true, cell.contains(sample.getX(), sample.getY()));
    }
    
    public void testContainsPointOutsideRectangle() {
    	Point lf = new Point(0, 0);
    	Point ur = new Point(1, 1);
    	GridCell cell = new GridCell(lf, ur);
    	Point sample = new Point(1.1, 0.5);
    	assertEquals(false, cell.contains(sample.getX(), sample.getY()));
    }
}
