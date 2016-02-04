import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * This is the class which implements Writable interface. 
 * This class is responsible for holding the data which is 
 * passed from the Mapper to the Combiner and then to the Reducer.
 */

class CompositeWritable implements Writable{

	private int count;
	private double min;
	private double max;
	private double pSum;
	private double pSqSum;
	private double avg;
	private double stdDev;
	private String type;
	
	/**
	 * Default Constructor. Initializes all the data members to default values.
	 */
	public CompositeWritable() {
		super();
		this.count = 0;
		this.min = 0.0;
		this.max = 0.0;
		this.pSum = 0.0;
		this.pSqSum = 0.0;
		this.avg = 0.0;
		this.stdDev = 0.0;
		this.type = new String("");
	}

	/**
	 * Parameterized Constructor. Initializing values to the values passed in as parameters.
	 * @param min
	 * @param max
	 * @param pSum
	 * @param pSqSum
	 * @param count
	 */
	public CompositeWritable(double min, double max,
			double pSum, double pSqSum, int count) {
		super();
		this.min = min;
		this.max = max;
		this.pSum = pSum;
		this.pSqSum = pSqSum;
		this.count = count;
		this.avg = 0.0;
		this.stdDev = 0.0;
		this.type = new String("");
	}
	
	/**
	 * Copy Constructor. Copies the value from the passed object to the newly created object.
	 * @param obj
	 */
	
	public CompositeWritable(CompositeWritable obj){
		super();
		this.min = obj.min;
		this.max = obj.max;
		this.pSum = obj.pSum;
		this.pSqSum = obj.pSqSum;
		this.count = obj.count;
		this.avg = obj.avg;
		this.stdDev = obj.stdDev;
		this.type = obj.type;
	}
	
	@Override
	/**
	 * Overridden readFields() method. 
	 * Used to read values from DataInput to the CompositeWritable object.
	 * @param in
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		if(type.equalsIgnoreCase("min")){
			in.readLine();
			this.setMin(in.readDouble());
		}else if(type.equalsIgnoreCase("max")){
			in.readLine();
			this.setMax(in.readDouble());
		}else if(type.equalsIgnoreCase("avg")){
			in.readLine();
			this.setAvg(in.readDouble());
			in.readLine();
			this.setStdDev(in.readDouble());
		}else{
			this.min = in.readDouble();
			this.max = in.readDouble();
			this.pSqSum = in.readDouble();
			this.pSum = in.readDouble();
			this.count = in.readInt();
		}
	}

	@Override
	/**
	 * Overridden write() method.
	 * Used to write the values of the CompositeWritable object to DataOutput.
	 * @param out
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		if(type.equalsIgnoreCase("min")){
			WritableUtils.writeString(out, "Minimum: ");
			out.writeDouble(this.getMin());
		}else if(type.equalsIgnoreCase("max")){
			WritableUtils.writeString(out, "Maximum: ");
			out.writeDouble(this.getMax());
		}else if(type.equalsIgnoreCase("avg")){
			WritableUtils.writeString(out, "Average: ");
			out.writeDouble(this.getAvg());
			WritableUtils.writeString(out, "Standard Deviation: ");
			out.writeDouble(this.getStdDev());
		}else{
			out.writeDouble(this.min);
			out.writeDouble(this.max);
			out.writeDouble(this.pSqSum);
			out.writeDouble(this.pSum);
			out.writeInt(this.count);
		}
		
	}


	/**
	 * Returns the contents of the CompositeWritable object in form of String.
	 */
	public String toString(){
		String outString = "";
		if(this.getType().equalsIgnoreCase("min")){
			outString = " Minimum: "+this.getMin();
		}else if(type.equalsIgnoreCase("max")){
			outString = " Maximum: "+this.getMax();
		}else if(type.equalsIgnoreCase("avg")){
			outString = " Average: "+ this.getAvg() +
			"\n\t Standard Deviation: " + this.getStdDev();
		}
		return outString;
	}
	
	/**
	 * Getters and Setters.
	 * @return
	 */
	
	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	public double getStdDev() {
		return stdDev;
	}

	public void setStdDev(double stdDev) {
		this.stdDev = stdDev;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getpSum() {
		return pSum;
	}

	public void setpSum(double pSum) {
		this.pSum = pSum;
	}

	public double getpSqSum() {
		return pSqSum;
	}

	public void setpSqSum(double pSqSum) {
		this.pSqSum = pSqSum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}

/**
 * StatsComputer2: Encapsulates the following classes:
 * 1. Map
 * 2. Combine
 * 3. Reduce
 */
public class StatsComputer2 {

	/**
	 * Map class which implements Mapper interface.
	 * Accepts input in the form of <LongWritable, Text>.
	 * Emits output in the form of <Text, CompositeWritable>.
	 * Map class gets input as line number (offset in the file) and the line (which has the number)
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, CompositeWritable>{
		/* Receives the input from the file and computes the partial values :  */
		
		int _count=0;
		double _pSum=0.0, _pSqSum=0.0;
		double _pMin = Double.MAX_VALUE;
		double _pMax = Double.MIN_VALUE;
		
		/**
		 * map() method : Calculates the minimum, maximum, sum and sum of squares.
		 * However this computation is the partial computation, and it is passed to the Combiner, which does the further computation.
		 * @param key - min, max, avg
		 * @param partialValues - Iterable value of CompositeWritable
		 * @param context - Byte Stream between Mapper, Combiner and Reducer and the HDFS
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(),"/n");
			while(itr.hasMoreTokens()){
				_count++;
				double _num = Double.parseDouble(itr.nextToken());
				_pSum += _num;
				_pSqSum += Math.pow(_num, 2);
				_pMin = Math.min(_pMin, _num);
				_pMax = Math.max(_pMax, _num);
				
			}
			if(200==_count){
				CompositeWritable _outputWritable = new CompositeWritable(_pMin, _pMax, _pSum, _pSqSum, _count);
				context.write(new Text("min"), _outputWritable);
				context.write(new Text("max"), _outputWritable);
				context.write(new Text("avg"), _outputWritable);
				//System.out.println("Count "+_count);
				_count=0;
				_pSum = 0; _pSqSum = 0; _pMin = Double.MAX_VALUE; _pMax = Double.MIN_VALUE;
			}
			
		}
		
	}
	
	/**
	 * Combine class which implements Reducer interface.
	 * Accepts input in the form of <Text, CompositeWritable>.
	 * Emits output in the form of <Text, CompositeWritable>.
	 */
	public static class Combine extends Reducer<Text, CompositeWritable, Text, CompositeWritable >{
		
		/**
		 * reduce() method : Responsible for computing total minimum value, total maximum value.
		 * Also it computes the total sum, and total square sum.
		 * These values are passed on to the Reducer.
		 * @param key - min, max, avg
		 * @param partialValues - Iterable value of CompositeWritable
		 * @param context - Byte Stream between Mapper, Combiner and Reducer and the HDFS
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<CompositeWritable> partialValues, Context context)
				throws IOException, InterruptedException {
			double _pSum=0.0, _pSqSum=0.0;
			double _pMin = Double.MAX_VALUE;
			double _pMax = Double.MIN_VALUE;
			double _pAvg = 0.0;
			int _count=0;
			String _key;			
			for(CompositeWritable _cW : partialValues){
				_key = key.toString();
				if(_key.equalsIgnoreCase("min")){
					_pMin = Math.min(_pMin, _cW.getMin());
				}else if(_key.equalsIgnoreCase("max")){
					_pMax = Math.max(_pMax, _cW.getMax());
				}else if(_key.equalsIgnoreCase("avg")){
					_pSum += _cW.getpSum();
					_pSqSum += _cW.getpSqSum();
					_count += _cW.getCount();
				}
			}
			CompositeWritable _outputWritable = new CompositeWritable(Math.round(_pMin * 1000.0) / 1000.0,
						Math.round(_pMax * 1000.0) / 1000.0, _pSum, _pSqSum, _count);
			context.write(key, _outputWritable);
		}
		
	}
	
	/**
	 * Reduce class which implements Reducer interface.
	 * Accepts input in the form of <Text, CompositeWritable>
	 * Emits output in the form of <Text, CompositeWritable>
	 */
	public static class Reduce extends Reducer<Text,CompositeWritable, Text, CompositeWritable>{
		/**
		 * reduce() method : Gets input from Combine class which provides Reducer with the final minimum and maximum value.
		 * Also, the combiner provides with the complete sum and complete square sum and the total count of numbers.
		 * The reduce() method will compute the average and the standard deviation, and then write to the output stream (context).
		 * @param key - min, max, avg
		 * @param partialValues - Iterable value of CompositeWritable
		 * @param context - Byte Stream between Mapper, Combiner and Reducer and the HDFS
		 * @throws IOException 
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<CompositeWritable> partialValues, Context context) throws IOException, InterruptedException {
			double _pSum = 0.0, _pSqSum = 0.0;
			double _pAvg = 0.0;
			int _count=0;
			CompositeWritable _outputWritable = new CompositeWritable();
			String _key = key.toString();
			for(CompositeWritable _cW : partialValues){
				_outputWritable = new CompositeWritable(_cW);
				if(_key.equalsIgnoreCase("min")){
					System.out.println("Minimum: "+_outputWritable.getMin());
					_outputWritable.setType("min");
				}else if(_key.equalsIgnoreCase("max")){
					System.out.println("Maximum: "+_outputWritable.getMax());
					_outputWritable.setType("max");
				}else if(_key.equalsIgnoreCase("avg")){
					_count = _cW.getCount();
					_pSum = _cW.getpSum();
					_pSqSum = _cW.getpSqSum();
					_pAvg = _pSum / _count;
					System.out.println("Average: "+Math.round(_pAvg * 100.0) / 100.0);
					_outputWritable.setType("avg");
					_outputWritable.setAvg(Math.round(_pAvg * 100.0) / 100.0);
					double _stdDev = ((_pSqSum)  
					- (2 * _pAvg * _pSum)
					+	(_count * Math.pow(_pAvg, 2))) / _count;
					_stdDev = Math.sqrt(_stdDev);
					_outputWritable.setStdDev(Math.round(_stdDev * 1000.0) / 1000.0);
					System.out.println("Standard Deviation: "+_stdDev);
				}
			}
			context.write(key, _outputWritable);
		}
	}
	
	/**
	 * main() method
	 * @param args <input_file_path> <output_file_path>
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: StatsComputer2 <in> <out>");
	      System.exit(2);
	    }

	    // create a job with name "wordcount"
	    Job job = new Job(conf, "StatsComputer2");
	    job.setJarByClass(StatsComputer2.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Combine.class);
	    job.setReducerClass(Reduce.class);
	    job.setNumReduceTasks(3);

	    // set output key type   
	    job.setOutputKeyClass(Text.class);
	    // set output value type
	    job.setOutputValueClass(CompositeWritable.class);
	    //set the HDFS path of the input data
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    // set the HDFS path for the output
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    //Wait till job completion
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
