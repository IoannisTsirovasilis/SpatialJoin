package gr.unipi.spatialJoin;

import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MinMaxCoordsParserTest 
    extends TestCase
{
    public MinMaxCoordsParserTest( String testName )
    {
        super( testName );
    }
    
    public static Test suite()
    {
        return new TestSuite( MinMaxCoordsParserTest.class );
    }

    
    public void testParserHotelRecordReturnsCorrectCoords() {
		Text record = new Text("3|Alameda County Fairgrounds|0|28|37.66169|-121.887367|laundry_service,meeting_rooms,event_catering,complimentary_breakfast,internet,internet_wireless,pets,fitness_facilities");
		int latitudeIndice = 4;
		int longitudeIndice = 5;
		String separator = "|";
		MinMaxCoordsParser parser = new MinMaxCoordsParser();
		parser.parse(record, latitudeIndice, longitudeIndice, separator);
		assertEquals(37.66169, parser.getLatitude());
		assertEquals(-121.887367, parser.getLongitude());
	}
}
