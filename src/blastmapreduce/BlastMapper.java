package blastmapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BlastMapper extends Mapper<String, String, IntWritable, Text> {

    private String localDB = "";

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        URI[] local = context.getCacheFiles();
        this.localDB = local[0].getPath();
    }

    public void map(IntWritable key, String value, Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String query = conf.get(BlastMapReduce.QUERY);
        String output = conf.get(BlastMapReduce.OUTPUT);
        
        String execCommand = "/vol/biotools/bin/diamond blastp" + " -d " + this.localDB + " -q " + query + "-a " + output + ".daa" + " -k 20000 -evalue 0.00001";
        //Create the external process

        Process p = Runtime.getRuntime().exec(execCommand);

        if (fs.exists(new Path(output + "\test.out"))) {
            OutputStream out = fs.append(new Path(output + ".daa"));
            IOUtils.copyBytes(p.getInputStream(), out, 4096, true);
        } else {
            OutputStream out = fs.create(new Path(output + ".daa"));
            IOUtils.copyBytes(p.getInputStream(), out, 4096, true);
        }

        p.waitFor();

    }
}
