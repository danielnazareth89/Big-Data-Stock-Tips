import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;

public class StockVolatilityChecker {

	public static class StockRORMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	LinkedList<Double> adjCloseList =new LinkedList<Double>();
	int start_month=12;
	
	FileSplit split;
	String filename;
	Text file=new Text();
	DoubleWritable adjClose = new DoubleWritable();
	double monthlyROR;
	

	
	public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
	System.out.println("Reached here");
	
	split =(FileSplit)context.getInputSplit();
	filename = split.getPath().getName();
	file.set(filename);
	


	String fields[]=value.toString().split(",");
	System.out.println(fields[0]);
	if(fields[0].equals("Date")==true)
			{
			;
			}
	
	else if((fields[0].equals("Date")==false) && (Integer.parseInt(fields[0].substring(5,7))==start_month))
			{
			adjCloseList.addLast(Double.parseDouble(fields[6]));
			}
	else if((Integer.parseInt(fields[0].substring(5,7))<start_month) || (start_month==1 && Integer.parseInt(fields[0].substring(5,7))>1))
			{
			if(adjCloseList.size()>0)
			{
			System.out.println("last day adj close is " + adjCloseList.getFirst() + " and first day adjClose is " +adjCloseList.getLast() );
			monthlyROR=(adjCloseList.getFirst() - adjCloseList.getLast())/adjCloseList.getLast();
			System.out.println("Monthly ROR for month" +start_month + "is " +monthlyROR);
			adjClose.set(monthlyROR);
			context.write(file,adjClose);
			adjCloseList.clear();
			adjCloseList.addLast(Double.parseDouble(fields[6]));
			start_month=Integer.parseInt(fields[0].substring(5,7));
			}
			}

	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
	if(adjCloseList.size()>0)
	{
	System.out.println("last day adj close is " + adjCloseList.getFirst() + " and first day adjClose is " +adjCloseList.getLast() );
	monthlyROR=(adjCloseList.getFirst() - adjCloseList.getLast())/adjCloseList.getLast();
	System.out.println("Monthly ROR for month" +start_month + "is " +monthlyROR);
	adjClose.set(monthlyROR);
	context.write(file,adjClose);
	adjCloseList.clear();
	}
	}
	
	}
	
	
	public static class VolatilityCalculator
    extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	
	DoubleWritable volatility = new DoubleWritable();
	Text keyText = new Text();
	ArrayList<Double> RORlist = new ArrayList<Double>();
	ArrayList<Double> allVolatilities = new ArrayList<Double>();
	ArrayList<String> all_Keys = new ArrayList<String>();
	
	public void reduce(Text key, Iterable<DoubleWritable> values,
            Context context
            ) throws IOException, InterruptedException {
	double sum=0.0;
	int month_count=0;
	double vlty=0.0;
	double averageROR=0.0;
	for(DoubleWritable val : values)
		{
		RORlist.add(val.get());
		sum+=val.get();
		month_count++;		
		}
	System.out.println("Sum of ROR's is equal to "+sum+ " and no of months is equal to" +month_count);
	averageROR=sum/month_count;
	System.out.println("average ROR is " +averageROR);
	sum=0.0;
	for(double ror : RORlist)
		{
		sum+=Math.pow((ror-averageROR),2);
		}
	
	System.out.println("weighted difference squared is " +sum	);
	if(month_count != 1)
		{
		System.out.println("About to calculate volatility with sum " +sum);
		vlty=sum * (1.0/(month_count-1));
		System.out.println("Volatility is " +vlty + " before sqrt");
		vlty=Math.sqrt(vlty);
		System.out.println("Volatility is " +vlty);
		if(vlty != 0.0)
		{
		allVolatilities.add(vlty);
		all_Keys.add(key.toString().substring(0, (key.toString().length()-4)));
		}
		//context.write(key,volatility);
		RORlist.clear();
		}
	
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
	String string_temp;
	double double_temp;
	int i,j;
	
	for(i=0;i<allVolatilities.size();i++)
		{
		for(j=i+1;j<allVolatilities.size();j++)
			{
			if(allVolatilities.get(j)<allVolatilities.get(i))
				{
				double_temp=allVolatilities.get(i);
				allVolatilities.set(i,allVolatilities.get(j));
				allVolatilities.set(j,double_temp);
				
				string_temp=all_Keys.get(i);
				all_Keys.set(i,all_Keys.get(j));
				all_Keys.set(j,string_temp);
				}
			}
		}
	if(allVolatilities.size()>10 && all_Keys.size()>10)
	{
	for(i=0;i<10;i++)
		{
		System.out.println(allVolatilities.get(i));
		volatility.set(allVolatilities.get(i));
		keyText.set(all_Keys.get(i));
		context.write(keyText,volatility);
		}
	for(i=allVolatilities.size()-10;i<allVolatilities.size();i++)
		{
		System.out.println(allVolatilities.get(i));
		volatility.set(allVolatilities.get(i));
		keyText.set(all_Keys.get(i));
		context.write(keyText,volatility);
		}
	
	}
	}
	}
	
	public static void main(String[] args) throws Exception{
	
		Configuration myconf = new Configuration();
		Job job = Job.getInstance(myconf, "Volatility Calculator");
		job.setJarByClass(StockVolatilityChecker.class);
		job.setMapperClass(StockRORMapper.class);
	    job.setReducerClass(VolatilityCalculator.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
