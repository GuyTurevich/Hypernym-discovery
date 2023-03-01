package org.example;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import static java.lang.System.exit;

import org.apache.commons.lang3.tuple.MutablePair;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, NounPair> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            DependencyGraph dependencyTree = new DependencyGraph(line.toString());

            for (MutablePair<String,NounPair> pattern : dependencyTree.getPatterns()) {
                context.write(new Text(pattern.getKey()), pattern.getValue());
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, NounPair, NounPair, IntWritable> {
        private int dpMin = 4;
        private int index;

        @Override
        public void reduce(Text key, Iterable<NounPair> values, Context context) throws IOException, InterruptedException {
            LinkedList<NounPair> pairs = new LinkedList<NounPair>();
            HashSet<String> uniquePairs = new HashSet<>();

            for (NounPair nounPair : values) {
                pairs.add(new NounPair(new Text(nounPair.getWord1().toString()), new Text(nounPair.getWord2().toString()), nounPair.getCount().get()));
                uniquePairs.add(nounPair.getWord1().toString() + "~~~" + nounPair.getWord2().toString());
            }

            if (uniquePairs.size() >= dpMin) {
                for (NounPair nounPair : pairs) {
                    context.write(nounPair, new IntWritable(index));
                }
                index += 1;
            }
        }

         @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            NounPair vectorSizeNP = new NounPair(new Text("*vectorSize*"), new Text("~"), index);
             System.out.println("started cleanup and writing: " + vectorSizeNP + ", -2");
            context.write(vectorSizeNP, new IntWritable(-2));
        }
    }

    public static void main(String[] args) throws Exception {
        String dpMin = "5";
        String input = "", output = "";
        boolean local = false;
        if(local){
            input = "/home/adler/Desktop/demo/biarcs";
            output = "/home/adler/Desktop/dist3/src/step1output";
        }
        else{
            input = "s3://bucketurevich/biarcs";
            output = "s3://bucketurevich/step1output";
        }

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Configuration conf = new Configuration();
        conf.set("dpMin", dpMin);
        Job job = Job.getInstance(conf);

        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NounPair.class);

        job.setOutputKeyClass(NounPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        TextInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
