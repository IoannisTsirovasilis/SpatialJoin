package gr.unipi.spatialJoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class JoinFileMapperTest extends TestCase {
	public JoinFileMapperTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( JoinFileMapperTest.class );
    }
    
    public void testBuildGridCreatesProperGrid() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Alameda County Fairgrounds|0|28|-40|-123|laundry_service,meeting_rooms,event_catering,complimentary_breakfast,internet,internet_wireless,pets,fitness_facilities");
		
    	int nameIndice = 1;
    	int latitudeIndice = 4;
		int longitudeIndice = 5;
		String separator = "|";
		
		Configuration conf = new Configuration();	
		conf.setInt("file1NameIndice", nameIndice);
		conf.setInt("file1LatitudeIndice", latitudeIndice);
		conf.setInt("file1LongitudeIndice", longitudeIndice);
		conf.set("file1Separator", separator);
		conf.setDouble("radius", 500);
		conf.setDouble("minLat", -42.118331);
		conf.setDouble("maxLat", 49.00106);
		conf.setDouble("minLon", -124.586307);
		conf.setDouble("maxLon", 47.669728);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("0", "1"), new TextPair("Alameda County Fairgrounds", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase00() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.15|0.15");
    	Text record2 = new Text("3|Test|0|28|0.15|0.15");
		
    	int nameIndice = 1;
    	int latitudeIndice = 4;
		int longitudeIndice = 5;
		String separator = "|";
		
		Configuration conf = new Configuration();	
		conf.setInt("file1NameIndice", nameIndice);
		conf.setInt("file1LatitudeIndice", latitudeIndice);
		conf.setInt("file1LongitudeIndice", longitudeIndice);
		conf.set("file1Separator", separator);
		conf.setDouble("radius", 15.72);
		conf.setDouble("minLat", 0);
		conf.setDouble("maxLat", 1);
		conf.setDouble("minLon", 0);
		conf.setDouble("maxLon", 1);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("0", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    
}
