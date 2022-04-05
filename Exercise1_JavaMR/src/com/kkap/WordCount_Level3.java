package com.kkap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class WordCount_Level3 {

    private static HashSet<String> blacklist = new HashSet<>();
    private static String          fileName;
    public static void initBlacklist() {
        try {
            BufferedReader in = new BufferedReader(new FileReader(fileName));
            String         str;

            blacklist = new HashSet<>();        // init blacklist using HashSet

            while ((str = in.readLine()) != null) {
                blacklist.addAll(List.of(str.split("\n|\\s+")));        // add all words in each line, separated by any number of 'space'
            }
        } catch (Exception ignored) {
        }
    }

    public static class Map extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one  = new IntWritable(1);
        private final        Text        word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            ArrayList<String> tokens = new ArrayList<>(Arrays.asList(value
                    .toString()
                    .replaceAll("\\p{P}|[$+<=>^`]", " ")  // preprocess step 1: replace every punctuation with a 'space'
                    .toLowerCase()                        // preprocess step 2: lower all cases
                    .split("\\s+")));                     // split the string by any number of 'space'

            tokens.removeAll(blacklist);                  // remove all word existed in blacklist

            for (String token : tokens) {
                word.set(token);
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void execute(String[] args) throws Exception {

        initBlacklist();
        fileName = args[args.length - 2];

        Job job = Job.getInstance(new Configuration(), "WordCount_Level3");
        job.setJarByClass(WordCount_Level3.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}