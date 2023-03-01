package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static java.lang.System.exit;

public class Step2 {

    public static class MapperClassAnnotated extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            String word1 = splitted[0];
            String word2 = splitted[1];
            if(word1.equals("") || word2.equals(""))
                return;
            boolean isHypernym = Boolean.valueOf(splitted[2]);
            NounPair nounPair = new NounPair(new Text(word1), new Text(word2), isHypernym);
            Text np = nounPair2Text(nounPair, "~#@");
            context.write(np, new Text("-100")); // value will not be used
        }
    }

    private static Text nounPair2Text(NounPair nounPair, String delimiter) {
        return new Text(nounPair.getWord1() + delimiter + nounPair.getWord2() + delimiter + nounPair.getCount() + delimiter + nounPair.isHypernym());
    }

    public static class MapperClass extends Mapper<NounPair, IntWritable, Text, Text> {
        @Override
        public void map(NounPair key, IntWritable value, Context context) throws IOException, InterruptedException {
            if(key.getWord1().toString().equals("*vectorSize*")) {
                System.out.println("writing to context vectorSize: " + key.getCount().get());
                Text vectorSize = new Text(Integer.toString(key.getCount().get())); // this is the index (size of the vector)
                context.write(new Text("*vectorSize*"), vectorSize);
            }
            else {
                NounPair nounPair = new NounPair(key.getWord1(), key.getWord2(), key.getCount().get());
                if(key.getWord1().toString().equals("") || key.getWord2().toString().equals(""))
                    return;
                Text np = nounPair2Text(nounPair, "~#@");
                String index = Integer.toString(value.get());
                int count = key.getCount().get();
                context.write(np, new Text(count + "," + index));
            }
        }
    }


private static class Comparison extends WritableComparator {
    protected Comparison() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable pair1, WritableComparable pair2) {
        try {
            if (pair1.toString().equals("*vectorSize*"))
                return -1;

            if (pair2.toString().equals("*vectorSize*"))
                return 1;

            String[] np1 = pair1.toString().split("~#@");
            String[] np2 = pair2.toString().split("~#@");

            int w1 = np1[0].compareTo(np2[0]);
            int w2 = np1[1].compareTo(np2[1]);

            if (w1 == 0) {
                if (w2 == 0) {
                    if (Integer.valueOf(np1[2]) == -1)
                        return -1;
                    else if (Integer.valueOf(np2[2]) == -1)
                        return 1;
                }
                return w2;
            }
            return w1;
        } catch (Exception e) {
            System.out.println("pair1: " + pair1.toString());
            System.out.println("pair2: " + pair2.toString());
            System.out.println(e);
            return 1;
        }
    }
}


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private NounPair curPair = null;
        int totalFeatures=1;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            try {
                if (key.toString().equals("*vectorSize*")) {
                    System.out.println("found vectorSize");
                    this.totalFeatures = Integer.parseInt(values.iterator().next().toString());
                    return;
                }

                String[] splitted = key.toString().split("~#@");
                String w1 = splitted[0];
                String w2 = splitted[1];
                int count = Integer.parseInt(splitted[2]);
                boolean hyper = Boolean.valueOf(splitted[3]);

                if (count == -1) {  //  annotated pair
                    this.curPair = new NounPair(new Text(w1), new Text(w2), hyper);
                    return;
                }

                if (this.curPair == null || !w1.equals(this.curPair.getWord1().toString()) || !w2.equals(this.curPair.getWord2().toString())) {
                    return;
                }

                Long[] featuresVector = new Long[this.totalFeatures];
                for (int i = 0; i < this.totalFeatures; i++) {
                    featuresVector[i] = Long.valueOf(0);
                }

                for (Text pattern : values) {
                    String[] parsed = pattern.toString().split(",");
                    int k = Integer.parseInt(parsed[0]); // count
                    int v = Integer.parseInt(parsed[1]); // index
                    featuresVector[v] += k;
                }

                StringBuilder ans = new StringBuilder();
                for (Long occ : featuresVector) {
                    ans.append(occ + ",");
                }

                context.write(new Text(curPair.toString()), new Text(ans.toString()));
            }
            catch (Throwable e) {
                System.out.println(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String input = "", output = "", hypernymFile = "";
        boolean local = false;
        if(local){
            input = "/home/adler/Desktop/dist3/src/step1output";
            output = "/home/adler/Desktop/dist3/src/step2output";
            hypernymFile = "/home/adler/Desktop/demo/hypernym.txt";
        }
        else{
            input = "s3://bucketurevich/step1output";
            output = "s3://bucketurevich/step2output";
            hypernymFile = "s3://bucketurevich/hypernym.txt";
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Step2.class);
        MultipleInputs.addInputPath(job, new Path(input), SequenceFileInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, new Path(hypernymFile), TextInputFormat.class, MapperClassAnnotated.class);
        job.setReducerClass(ReducerClass.class);

        job.setSortComparatorClass(Comparison.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        exit(job.waitForCompletion(true) ? 0 : 1);
    }
}