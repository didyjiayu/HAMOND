package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sidefunctions.DiamondAlignment;
import sidefunctions.DeleteIntermediateFiles;
import sidefunctions.DiamondView;
import sidefunctions.WriteKeyValueToTemp;

public class DiamondMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String localDB;
    private String diamond;

    //get local path of DIAMOND and database
    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path[] local = context.getLocalCacheFiles();
        this.diamond = local[0].toString();
        this.localDB = local[1].toString();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        //get query and database name from mapreduce driver
        Configuration conf = context.getConfiguration();
        String query = conf.get(DiamondMapReduce.QUERY);
        String dataBase = conf.get(DiamondMapReduce.DATABASE);
        String[] args = conf.getStrings("DIAMOND-arguments");

        //write key-value pair to local tmp
        WriteKeyValueToTemp.write(key.toString(), value.toString());

        //use runtime to execute alignment, intermediate binary files are stored in local tmp
        DiamondAlignment.align(this.diamond, this.localDB, key.toString(), args);

        //view the binary files to tabular output file, view output will be streammized into HDFS
        DiamondView.view(this.diamond, key.toString(), conf);
        
        //delete all intermediate files
        DeleteIntermediateFiles.deleteFiles(key.toString());

    }
}
