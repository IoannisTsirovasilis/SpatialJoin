package gr.unipi.spatialJoin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

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
    	
    	TextPair[] samples = {
    			new TextPair("Name11", "1"),
    			new TextPair("Name12", "1"),
    			new TextPair("Name13", "1"),
    			new TextPair("Name21", "2"),
    			new TextPair("Name22", "2"),
    			new TextPair("Name23", "2"),
    	};
    	List<TextPair> iter = Arrays.asList(samples);
    	
		new ReduceDriver<TextPair, TextPair, Text, Text>()
			.withReducer(new JoinFilesReducer())
			.withInput(key, iter)
			.withOutput(new Text("Name11"), new Text("Name21"))
			.withOutput(new Text("Name12"), new Text("Name21"))
			.withOutput(new Text("Name13"), new Text("Name21"))
			.withOutput(new Text("Name11"), new Text("Name22"))
			.withOutput(new Text("Name12"), new Text("Name22"))
			.withOutput(new Text("Name13"), new Text("Name22"))
			.withOutput(new Text("Name11"), new Text("Name23"))
			.withOutput(new Text("Name12"), new Text("Name23"))
			.withOutput(new Text("Name13"), new Text("Name23"))
			.runTest();
    }
}
