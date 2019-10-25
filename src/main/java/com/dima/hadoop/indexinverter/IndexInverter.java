package com.dima.hadoop.indexinverter;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IndexInverter {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println(String.format("Invalid number of arguments. Got %d, expected %d.", args.length, 2));
            System.exit(1);
        }

        String inputPath = args[0];
        int numReduceTasks = 1;
        try {
            numReduceTasks = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Got invalid number of reduce tasks.");
            System.exit(1);
        }
        System.out.println("Set number of reduce tasks to: " + numReduceTasks);

        Configuration conf = new Configuration();
        // set begin and end tags of Wikipedia page
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");
        Job job = Job.getInstance(conf);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd-HHmmss");
        FileOutputFormat.setOutputPath(job, new Path("output-" + sf.format(new Date()).toString()));

        
        job.setJarByClass(IndexInverter.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(IndexInverterMapper.class);
        job.setCombinerClass(IndexInverterCombiner.class);
        job.setReducerClass(IndexInverterReducer.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(numReduceTasks);

        job.waitForCompletion(true);
    }

}