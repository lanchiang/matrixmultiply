import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Fuga on 15/11/20.
 */
public class MatrixMultiply {

    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;//m1 or m2

        private static int rownum;// row number of A
        private static int colnum;// column number of B

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            rownum = Integer.parseInt(context.getConfiguration().get("rownum"));
            colnum = Integer.parseInt(context.getConfiguration().get("colnum"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit)context.getInputSplit();
            flag = split.getPath().getName();
            System.out.println(flag);

            String[] tokens = MainRun.DELIMITER.split(value.toString());
            if (flag.equals("m1.csv")) {
                Text v = new Text("A:" + tokens[1] + "," + tokens[2]);
                for (int i = 1;i<=colnum;i++) {
                    Text k = new Text(tokens[0] + "," + i);
                    context.write(k, v);
                    System.out.println(k.toString() + " " + v.toString());
                }
            }
            else if (flag.equals("m2.csv")) {
                Text v = new Text("B:" + tokens[0] + "," + tokens[2]);
                for (int i = 1;i<=rownum;i++) {
                    Text k = new Text(i + "," + tokens[1]);
                    context.write(k, v);
                    System.out.println(k.toString() + " " + v.toString());
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,String> mapA = new HashMap<>();
            Map<String,String> mapB = new HashMap<>();
            for (Text line: values) {
                String val = line.toString();
                String[] kv = MainRun.DELIMITER.split(val.substring(2));
                if (val.startsWith("A:")) {
                    mapA.put(kv[0], kv[1]);
                }
                else if (val.startsWith("B:")) {
                    mapB.put(kv[0], kv[1]);
                }
            }
            int result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();
                result += Integer.parseInt(mapA.get(mapk)) * Integer.parseInt(mapB.get(mapk));
            }
            System.out.println(key.toString()+": " + result);
            context.write(key, new Text(String.valueOf(result)));
        }

    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = MainRun.config();

        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String output = path.get("output");

        Job job = new Job(conf);
        job.setJarByClass(MatrixMultiply.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.setMinInputSplitSize(job, 0);
        FileInputFormat.setMaxInputSplitSize(job, 1024*1L);

        job.waitForCompletion(true);
    }
}
