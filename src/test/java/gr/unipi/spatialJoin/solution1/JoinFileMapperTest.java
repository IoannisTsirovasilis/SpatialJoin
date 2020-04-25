package gr.unipi.spatialJoin.solution1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import gr.unipi.spatialJoin.TextPair;
import gr.unipi.spatialJoin.solution1.JoinFile1Mapper;
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
		conf.setInt("hSectors", 10);
		conf.setInt("vSectors", 10);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("0", "1"), new TextPair("Alameda County Fairgrounds", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
 // Tests on 3x3 Grid
    
    public void testEdgeCaseRightBound() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.15|1");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("2", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase0UpperRight() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.31|0.31");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("0", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("3", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("1", "1"), new TextPair("Test", "1"))			
			.withOutput(new TextPair("4", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    
    
    public void testEdgeCase0Top() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.31|0.15");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("0", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("3", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase0Right() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.15|0.31");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("0", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("1", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase2UpperLeft() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.31|0.68");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("2", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("5", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("1", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("4", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase2Top() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.31|0.90");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("2", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("5", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase2Left() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.15|0.70");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("2", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("1", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase6Right() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.90|0.31");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("6", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("7", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase6Bottom() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.68|0.15");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("6", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("3", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase6BottomRight() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.68|0.31");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("6", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("3", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("7", "1"), new TextPair("Test", "1"))			
			.withOutput(new TextPair("4", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase8Bottom() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.68|0.90");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("8", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("5", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase8Left() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.90|0.68");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("8", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("7", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    public void testEdgeCase8BottomLeft() throws IOException, InterruptedException {    	
    	Text record = new Text("3|Test|0|28|0.68|0.68");
		
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
		conf.setInt("hSectors", 3);
		conf.setInt("vSectors", 3);
		
		new MapDriver<LongWritable, Text, TextPair, TextPair>()
			.withMapper(new JoinFile1Mapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new TextPair("8", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("5", "1"), new TextPair("Test", "1"))
			.withOutput(new TextPair("7", "1"), new TextPair("Test", "1"))			
			.withOutput(new TextPair("4", "1"), new TextPair("Test", "1"))
			.withConfiguration(conf)
			.runTest();
    }
    
    
}
