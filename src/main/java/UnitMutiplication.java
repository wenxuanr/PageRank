import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class UnitMutiplication {
    public static class TransitionMapper extends Mapper<Object,Text,Text,Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            if(fromTo.length == 1 || fromTo[1].trim().equals("")) {
                return;
            }
            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to: tos) {
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        //k1   //v1   //k2   // v2
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String pr[] = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
            }
    }
    public static class MultiplicationReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce (Text key, Iterable<Text>values, Context context)
                throws IOException,InterruptedException {
            // key = page 1 (front page )value = < got to 2 =1/4, got to 7=1/4..... 1/6012 (初始化的pr)>
            List<String> transitionCell = new ArrayList<String>();
            double prCell =  0;
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionCell.add(value.toString().trim());
                }
                else {
                    // represent current page rank
                    prCell = Double.parseDouble(value.toString().trim());
                }
            }
            for (String cell : transitionCell) {
                // seperate transition from pr cell
                // then mutiply
                String outputKey = cell.split("=")[0]; // to page
                double relation = Double.parseDouble(cell.split("=")[1]); // 1/4 * PRO
                String OutputValue = String.valueOf(relation * prCell);
                context.write(new Text(outputKey), new Text(OutputValue));
            }
        }
    }
    public static void main (String []args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(UnitMutiplication.class);

        ChainMapper.addMapper(job,TransitionMapper.class,Object.class,Text.class,Text.class,Text.class,conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        //args[0] = relation.txt
        //args[1] = pr.txt
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
