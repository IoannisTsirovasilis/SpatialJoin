package gr.unipi.spatialJoin;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;



public class MinMaxCoordsMappersTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MinMaxCoordsMappersTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( MinMaxCoordsMappersTest.class );
    }
    
    @SuppressWarnings("deprecation")
	public void testHotelsMapOutputsCorrectKeyValue() throws IOException, InterruptedException {
    	Text record = new Text("3|Alameda County Fairgrounds|0|28|37.66169|-121.887367|laundry_service,meeting_rooms,event_catering,complimentary_breakfast,internet,internet_wireless,pets,fitness_facilities");
		int latitudeIndice = 4;
		int longitudeIndice = 5;
		String separator = "|";
		
		Configuration conf = new Configuration();		
		conf.setInt("hotelLatitudeIndice", latitudeIndice);
		conf.setInt("hotelLongitudeIndice", longitudeIndice);
		conf.set("hotelSeparator", "|");
		new MapDriver<LongWritable, Text, Text, DoubleWritable>()
			.withMapper(new MinMaxCoordsHotelsMapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new Text("minLat"), new DoubleWritable(37.66169))
			.withOutput(new Text("maxLat"), new DoubleWritable(37.66169))
			.withOutput(new Text("minLon"), new DoubleWritable(-121.887367))
			.withOutput(new Text("maxLon"), new DoubleWritable(-121.887367))
			.withConfiguration(conf)
			.runTest();;
    }
    
    @SuppressWarnings("deprecation")
	public void testRestaurantsMapOutputsCorrectKeyValue() throws IOException, InterruptedException {
    	Text record = new Text("3|Laguna Pizza|4.5|38.423436|-121.41361|Pizza");
		int latitudeIndice = 3;
		int longitudeIndice = 4;
		String separator = "|";
		
		Configuration conf = new Configuration();		
		conf.setInt("restaurantLatitudeIndice", latitudeIndice);
		conf.setInt("restaurantLongitudeIndice", longitudeIndice);
		conf.set("restaurantSeparator", "|");
		new MapDriver<LongWritable, Text, Text, DoubleWritable>()
			.withMapper(new MinMaxCoordsRestaurantsMapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new Text("minLat"), new DoubleWritable(38.423436))
			.withOutput(new Text("maxLat"), new DoubleWritable(38.423436))
			.withOutput(new Text("minLon"), new DoubleWritable(-121.41361))
			.withOutput(new Text("maxLon"), new DoubleWritable(-121.41361))
			.withConfiguration(conf)
			.runTest();;
    }
}
