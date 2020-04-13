package gr.unipi.spatialJoin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MinMaxCoordsReducerTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MinMaxCoordsReducerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( MinMaxCoordsReducerTest.class );
    }

	public void testReducerOutputsCorrectMinKeyValue() throws IOException, InterruptedException {
    	Text key = new Text("minLat");
    	DoubleWritable[] samples = {
    			new DoubleWritable(37.776181),
    			new DoubleWritable(38.959176),
    			new DoubleWritable(37.66069),
    			new DoubleWritable(34.19198813),
    			new DoubleWritable(37.755557),
    			new DoubleWritable(38.423436)
    	};
    	List<DoubleWritable> iter = Arrays.asList(samples);
    	
		new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>()
			.withReducer(new MinMaxCoordsReducer())
			.withInput(key, iter)
			.withOutput(new Text("minLat"), new DoubleWritable(34.19198813))
			.runTest();
    }
	
	public void testReducerOutputsCorrectMaxKeyValue() throws IOException, InterruptedException {
    	Text key = new Text("maxLat");
    	DoubleWritable[] samples = {
    			new DoubleWritable(37.776181),
    			new DoubleWritable(38.959176),
    			new DoubleWritable(37.66069),
    			new DoubleWritable(34.19198813),
    			new DoubleWritable(37.755557),
    			new DoubleWritable(38.423436)
    	};
    	List<DoubleWritable> iter = Arrays.asList(samples);
    	
		new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>()
			.withReducer(new MinMaxCoordsReducer())
			.withInput(key, iter)
			.withOutput(new Text("maxLat"), new DoubleWritable(38.959176))
			.runTest();
    }
}
