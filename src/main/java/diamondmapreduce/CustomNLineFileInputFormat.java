/*
 * Copyright 2016 YU Jia

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author yujia1986
 */
public class CustomNLineFileInputFormat extends FileInputFormat<LongWritable, Text> {

//    private static final long MAX_SPLIT_SIZE = 16777216;  //16MB SPLIT
    private static final long MAX_SPLIT_SIZE = 2097152;  //2MB SPLIT
//    private static final long MAX_SPLIT_SIZE = 8388608;  //8MB SPLIT
//    private static final long MAX_SPLIT_SIZE = 33554432; //32MB SPLIT
//    private static final long MAX_SPLIT_SIZE = 67108864; //64MB SPLIT

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NLineRecordReader();
    }

    @Override
    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return super.computeSplitSize(blockSize, minSize, MAX_SPLIT_SIZE);
    }
}
