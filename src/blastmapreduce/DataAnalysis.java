
package blastmapreduce;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataAnalysis extends Configured implements Tool {

    public static String QUERY = "query_sequence";
    public static String DATABASE = "database";
    public static String OUTPUT = "output_path";

    void launch(String query, String dataBase, String outPut) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BLAST");

        // First get the file system handler, delete any previous files, add the
        // files and write the data to it, then pass its name as a parameter to
        // job
        Path hdMainDir = new Path(outPut);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(hdMainDir, true);

        Path hdOutDir = new Path(hdMainDir, "out");

        // Starting the data analysis.
        Configuration jc = job.getConfiguration();

        jc.set(QUERY, query);
        jc.set(DATABASE, dataBase);
        jc.set(OUTPUT, outPut);

        job.addCacheFile(new URI(dataBase));

        FileInputFormat.setInputPaths(job, new Path(query));
        FileOutputFormat.setOutputPath(job, hdOutDir);

        job.setJarByClass(DataAnalysis.class);
        job.setMapperClass(RunnerMap.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(DataFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(0);

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
        int res = ToolRunner.run(new Configuration(), new DataAnalysis(), argv);
        System.exit(res);
    }
}
