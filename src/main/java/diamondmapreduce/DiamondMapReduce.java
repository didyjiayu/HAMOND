package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DiamondMapReduce extends Configured implements Tool {

    public static String QUERY = "query_sequence";
    public static String DATABASE = "database";
    public static String OUTPUT = "output_path";

    void launch(String query, String dataBase, String outPut) throws Exception {

        Job job = Job.getInstance(getConf(), "DIAMOND");
        Configuration conf = job.getConfiguration();
//        conf.set("mapreduce.input.fileinputformat.split.maxsize", "102400");
//        conf.set("textinputformat.record.delimiter",">");
        conf.set("mapreduce.task.timeout", "36000000");
        conf.set("mapreduce.map.memory.mb", "8192");
        conf.set("mapreduce.reduce.memory.mb", "8192");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outPut), true);

        // Starting the data analysis.
//        Configuration jc = job.getConfiguration();
        conf.set(QUERY, query);
        conf.set(DATABASE, dataBase);
        conf.set(OUTPUT, outPut);

//        job.addCacheFile(new URI(dataBase));
        FileInputFormat.addInputPath(job, new Path(query));
        FileOutputFormat.setOutputPath(job, new Path(outPut));

        job.setJarByClass(DiamondMapReduce.class);
        job.setMapperClass(DiamondMapper.class);
        job.setReducerClass(DiamondReducer.class);

        job.setInputFormatClass(CustomNLineFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    @Override
    public int run(String[] args) throws Exception {
        String q = args[0];
        String db = args[1];
        String op = args[2];
        launch(q, db, op);
        return 0;
    }

    public static void main(String[] argv) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DiamondMapReduce(), argv);
        System.exit(res);
    }
}
