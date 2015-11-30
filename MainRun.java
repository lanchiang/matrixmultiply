import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Fuga on 15/11/20.
 */
public class MainRun {
    public static final String HDFS = "hdfs://localhost:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    static Map<String, String> path;

    public static void main(String[] args) {
        path = new HashMap<>();
        matrixMultiply();
    }

    public static void matrixMultiply() {
        path.put("m1", "/user/matrixmultiply/m1.csv"); // 本地数据文件
        path.put("m2", "/user/matrixmultiply/m2.csv");
        path.put("input1", HDFS+"/user/matrixmultiply/matrix/m1.csv");
        path.put("input2", HDFS+"/user/matrixmultiply/matrix/m2.csv");
        path.put("output", HDFS+"/user/matrixmultiply/matrix/output");

        try {
            MatrixMultiply.run(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static JobConf config() throws IOException {
        JobConf conf = new JobConf(MainRun.class);
        conf.set("rownum", String.valueOf(getMatrixArownum(path.get("input1"))));
        conf.set("colnum", String.valueOf(getMatrixBcolnum(path.get("input2"))));
        Path output_path = new Path(HDFS+"/user/matrixmultiply/matrix/output");
        FileSystem fs = output_path.getFileSystem(conf);
        if (fs.delete(output_path, true)) {
            System.out.println("删除上次output目录成功");
        }
        else {
            System.out.println("删除上次output目录失败");
            System.exit(1);
        }

//        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");
//        Long l = (long)(10240000);
//        conf.set("mapreduce.input.fileinputformat.split.maxsize", l.toString());

        conf.setJobName("MatrixMultiply");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/core-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/hdfs-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/mapred-site.xml");
        return conf;
    }

    public static int getMatrixArownum(String uri) {
        JobConf conf = new JobConf(MainRun.class);
        int num = 0;
        try {
            FileSystem filesystem = FileSystem.get(URI.create(uri), conf);
            InputStream inputStream = filesystem.open(new Path(uri));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line=br.readLine())!=null) {
                num = Integer.parseInt(line.split(",")[0]);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return num;
        }
    }

    public static int getMatrixBcolnum(String uri) {
        JobConf conf = new JobConf(MainRun.class);
        int num = 0;
        try {
            FileSystem filesystem = FileSystem.get(URI.create(uri), conf);
            InputStream inputStream = filesystem.open(new Path(uri));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line=br.readLine())!=null) {
                if (!(line.split(",")[0]).equals("1")) {
                    break;
                }
                num = Integer.parseInt(line.split(",")[1]);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return num;
        }
    }
}
