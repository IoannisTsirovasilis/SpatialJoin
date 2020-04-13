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

    
    public void testParserCoordsInBetweenRecordReturnsCorrectCoords() {
		Text record = new Text("3|Alameda County Fairgrounds|0|28|37.66169|-121.887367|laundry_service,meeting_rooms,event_catering,complimentary_breakfast,internet,internet_wireless,pets,fitness_facilities");
		int latitudeIndice = 4;
		int longitudeIndice = 5;
		String separator = "|";
		MinMaxCoordsParser parser = new MinMaxCoordsParser();
		parser.parse(record, latitudeIndice, longitudeIndice, separator);
		assertEquals(37.66169, parser.getLatitude());
		assertEquals(-121.887367, parser.getLongitude());
	}
    
    public void testParserCoordsAtBeginningOfRecordReturnsCorrectCoords() {
		Text record = new Text("34.016384|12|Starbucks Coffee|4|-118.49698|American, Coffee");
		int latitudeIndice = 0;
		int longitudeIndice = 4;
		String separator = "|";
		MinMaxCoordsParser parser = new MinMaxCoordsParser();
		parser.parse(record, latitudeIndice, longitudeIndice, separator);
		assertEquals(34.016384, parser.getLatitude());
		assertEquals(-118.49698, parser.getLongitude());
	}
    
    public void testParserCoordsAtEndOfRecordReturnsCorrectCoords() {
		Text record = new Text("12|Starbucks Coffee|4|-118.49698|American, Coffee|34.016384");
		int latitudeIndice = 5;
		int longitudeIndice = 3;
		String separator = "|";
		MinMaxCoordsParser parser = new MinMaxCoordsParser();
		parser.parse(record, latitudeIndice, longitudeIndice, separator);
		assertEquals(34.016384, parser.getLatitude());
		assertEquals(-118.49698, parser.getLongitude());
	}
}
