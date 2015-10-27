
package blastmapreduce;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BlastMapReduce extends Configured implements Tool {

    public static String QUERY = "query_sequence";
    public static String DATABASE = "database";
    public static String OUTPUT = "output_path";

    void launch(String query, String dataBase, String outPut) throws Exception {

        Job job = Job.getInstance(new Configuration(), "BLAST");
        Configuration conf = job.getConfiguration();
        
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outPut), true);

        // Starting the data analysis.
        Configuration jc = job.getConfiguration();

        jc.set(QUERY, query);
        jc.set(DATABASE, dataBase);
        jc.set(OUTPUT, outPut);

        job.addCacheArchive(new URI(dataBase));

        FileInputFormat.setInputPaths(job, new Path(query));
        FileOutputFormat.setOutputPath(job, new Path(outPut));

        job.setJarByClass(BlastMapReduce.class);
        job.setMapperClass(BlastMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
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
        int res = ToolRunner.run(new Configuration(), new BlastMapReduce(), argv);
        System.exit(res);
    }
}
