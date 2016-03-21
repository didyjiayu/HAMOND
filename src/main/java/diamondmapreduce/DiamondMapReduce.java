package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sidefunctions.makeDB;
import sidefunctions.removeDB;

public class DiamondMapReduce extends Configured implements Tool {

    public static String QUERY = "query_sequence";
    public static String DATABASE = "database";
    public static String OUTPUT = "output_path";

    void launch(String diamond, String query, String outPut) throws Exception {

        //set Hadoop configuration
        Job job = Job.getInstance(getConf(), "DIAMOND");
        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.task.timeout", "36000000");
        conf.set("mapreduce.map.memory.mb", "8192");
        conf.set("mapreduce.reduce.memory.mb", "8192");

        //get user name
        String hadoopUser = UserGroupInformation.getCurrentUser().getUserName();

        //make DIAMOND database on local then copy to HDFS with query and delete local database
        makeDB.makeDB(diamond, query);
        String[] deleteFiles = {"hadoop", "fs", "-rm", "-r", hadoopUser + "/*"};
        Process delete = Runtime.getRuntime().exec(deleteFiles);
        delete.waitFor();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("output"), true);
        fs.copyFromLocalFile(new Path(query + ".dmnd"), new Path(hadoopUser));
        fs.copyFromLocalFile(new Path(query), new Path(hadoopUser));
        fs.copyFromLocalFile(new Path(diamond), new Path(hadoopUser));
        removeDB.removeDB(query + ".dmnd");

        //pass query name and database name to mappers
        conf.set(QUERY, query);
        conf.set(DATABASE, query + ".dmnd");
        conf.set(OUTPUT, outPut);

        //add DIAMOND bin and database into cached file
        job.addCacheFile(new URI(hadoopUser + "/diamond"));
        job.addCacheFile(new URI(hadoopUser + "/" + query + ".dmnd"));

        //start Hadoop MapReduce
        FileInputFormat.addInputPath(job, new Path(hadoopUser + "/" + query));
        FileOutputFormat.setOutputPath(job, new Path(outPut));

        job.setJarByClass(DiamondMapReduce.class);
        job.setMapperClass(DiamondMapper.class);
//        job.setReducerClass(DiamondReducer.class);

        job.setInputFormatClass(CustomNLineFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

    @Override
    public int run(String[] args) throws Exception {
        String dmd = args[0];
        String q = args[1];
        String op = args[2];
        launch(dmd, q, op);
        return 0;
    }

    public static void main(String[] argv) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DiamondMapReduce(), argv);
        System.exit(res);
    }
}
