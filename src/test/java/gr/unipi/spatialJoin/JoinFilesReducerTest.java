package gr.unipi.spatialJoin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import gr.unipi.spatialJoin.Tuple4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import gr.unipi.spatialJoin.JoinFilesReducer;
import gr.unipi.spatialJoin.TextPair;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class JoinFilesReducerTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public JoinFilesReducerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( JoinFilesReducerTest.class );
    }
    
    public void testReducerOutputsCorrectKeyValue() throws IOException, InterruptedException {
    	TextPair key = new TextPair("1", "1");
    	
    	Tuple4[] samples = {
    			new Tuple4("Name11", "1", 0.05, 0.1),
    			new Tuple4("Name12", "1", 10, 10),
    			new Tuple4("Name13", "1", 5, 5),
    			new Tuple4("Name21", "2", 0, 0),
    			new Tuple4("Name22", "2", 0, 0.1),
    			new Tuple4("Name23", "2", 5.05, 5.05),
    	};
    	List<Tuple4> iter = Arrays.asList(samples);
    	
    	Configuration conf = new Configuration();	
		conf.setDouble("radius", 15.72);
    	
		new ReduceDriver<TextPair, Tuple4, Text, Text>()
			.withReducer(new JoinFilesReducer())
			.withInput(key, iter)
			.withOutput(new Text("Name11"), new Text("Name21"))
			.withOutput(new Text("Name11"), new Text("Name22"))
			.withOutput(new Text("Name13"), new Text("Name23"))
			.withConfiguration(conf)
			.runTest();
    }
}
