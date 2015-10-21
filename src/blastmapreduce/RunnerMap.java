package blastmapreduce;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RunnerMap extends Mapper<String, String, IntWritable, Text> {

    private String localDB = "";

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        URI[] local = context.getCacheArchives();
        this.localDB = local[0].getPath();
    }

    @Override
    public void map(String key, String value, Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();
        String query = conf.get(DataAnalysis.QUERY);
        String output = conf.get(DataAnalysis.OUTPUT);

        String execCommand = "/usr/bin/blastp"+ " -db " + this.localDB + " -num_alignments 20000 -comp_based_stats 0 -seg no -outfmt 6 -evalue 0.00001";
		//Create the external process

        Process p = Runtime.getRuntime().exec(execCommand);

        OutputHandler inputStream = new OutputHandler(p.getInputStream(), "INPUT", output);

        // start the stream threads.
        inputStream.start();

        p.waitFor();

        //Upload the results to HDFS

        Path outputDirPath = new Path(outputDir);
        Path outputFileName = new Path(outputDirPath, fileNameOnly);
        fs.copyFromLocalFile(new Path(outFile), outputFileName);

    }
}
