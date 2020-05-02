package gr.unipi.spatialJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Tuple4 implements WritableComparable<Tuple4> {
	private Text name;
	private Text fileTag;
	private DoubleWritable longitude;
	private DoubleWritable latitude;
	
	public Tuple4() {
		set(new Text(), new Text(), 0, 0);
	}
	
	public Tuple4(String name, String fileTag, double longitude, double latitude) {
		set(new Text(name), new Text(fileTag), longitude, latitude);
	}

	public Tuple4(Text name, Text fileTag, double longitude, double latitude) {
		set(name, fileTag, longitude, latitude);
	}
	
	public Tuple4(Text name, Text fileTag, DoubleWritable longitude, DoubleWritable latitude) {
		set(name, fileTag, longitude.get(), latitude.get());
	}

	public void set(Text name, Text fileTag, double longitude, double latitude) {
		this.name = name;
		this.fileTag = fileTag;
		this.longitude = new DoubleWritable(longitude);
		this.latitude = new DoubleWritable(latitude);
	}

	public Text getName() {
		return this.name;
	}

	public Text getFileTag() {
		return this.fileTag;
	}
	
	public DoubleWritable getLongitude() {
		return this.longitude;
	}
	
	public DoubleWritable getLatitude() {
		return this.latitude;
	}
	
	public void write(DataOutput out) throws IOException {
		this.name.write(out);
		this.fileTag.write(out);
		this.longitude.write(out);
		this.latitude.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.name.readFields(in);
		this.fileTag.readFields(in);
		this.longitude.readFields(in);
		this.latitude.readFields(in);
	}

	public int compareTo(Tuple4 t4) {
		int cmp = this.name.compareTo(t4.name);
		if (cmp != 0) {
			return cmp;
		}
		return this.fileTag.compareTo(t4.fileTag);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Tuple4) {
			Tuple4 t4 = (Tuple4) o;
			return this.name.equals(t4.name) && this.fileTag.equals(t4.fileTag) 
					&& this.longitude.get() == t4.longitude.get() && this.latitude.get() == t4.latitude.get();
		}
		return false;
	}

	@Override
	public String toString() {
		return this.name + "\t" + this.fileTag + "\t" + this.longitude + "\t" + this.latitude;
	}
}
