package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FullInvertedIndex {

    public static class IndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        
        private Text word = new Text();
        private Text location = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String line = value.toString();
            int lineNumber = (int) key.get();

            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                word.set(token.toLowerCase());
                location.set(fileName + ":" + lineNumber);
                output.collect(word, location);
            }
        }
    }

    public static class IndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Map<String, List<Integer>> map = new HashMap<>();

            while (values.hasNext()) {
                String[] fileAndLine = values.next().toString().split(":");
                String fileName = fileAndLine[0];
                int lineNumber = Integer.parseInt(fileAndLine[1]);

                map.putIfAbsent(fileName, new ArrayList<>());
                map.get(fileName).add(lineNumber);
            }

            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
                if (result.length() > 0) {
                    result.append("; ");
                }
                result.append(entry.getKey()).append(":").append(entry.getValue().toString());
            }

            output.collect(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(FullInvertedIndex.class);
        conf.setJobName("fullinvertedindex");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(IndexMapper.class);
        conf.setCombinerClass(IndexReducer.class);
        conf.setReducerClass(IndexReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
