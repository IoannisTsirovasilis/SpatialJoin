package gr.unipi.spatialJoin;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MathUtilTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MathUtilTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( MathUtilTest.class );
    }
    
    public void testHaversineDistance() {
    	double lat1 = 0;
    	double lat2 = 2;
    	double lon1 = 1;
    	double lon2 = -1;
    	double d = MathUtil.haversineDistance(lat1, lat2, lon1, lon2);
    	assertEquals(314.5, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase0() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(-1, 2);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(157.2, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase1() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(0.5, 3);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(222.4, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase2() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(3, 3);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(314.4, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase3() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(5, 0.7);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(444.7, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase4() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(5, -5);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(711.6, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase5() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(0.2, -3);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(333.6, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase6() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(-3, -3);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(471.7, d, 0.1);
    }
    
    public void testPointToRectangleDistanceCase7() {
    	Point lowerLeft = new Point(0, 0);
    	Point upperRight = new Point(1, 1);
    	Point point = new Point(-3, 1);
    	double d = MathUtil.pointToRectangleDistance(point, lowerLeft, upperRight);
    	assertEquals(333.5, d, 0.1);
    }
}
