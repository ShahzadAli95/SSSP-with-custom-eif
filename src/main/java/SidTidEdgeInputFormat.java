import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SidTidEdgeInputFormat extends org.apache.giraph.io.formats.TextEdgeInputFormat<LongWritable,
        FloatWritable>{
    public SidTidEdgeInputFormat(){
        super();
    }

    protected class SidTidWithCostEdgeReader extends TextEdgeReaderFromEachLine{
        @Override
        protected LongWritable getSourceVertexId(org.apache.hadoop.io.Text line) throws IOException {
            return new LongWritable(Long.parseLong(line.toString().split("\t")[0]));
        }

        @Override
        protected LongWritable getTargetVertexId(org.apache.hadoop.io.Text line) throws IOException {
            return new LongWritable(Long.parseLong(line.toString().split("\t")[1]));
        }

        @Override
        protected FloatWritable getValue(org.apache.hadoop.io.Text line) throws IOException {
            return new FloatWritable(Long.parseLong(line.toString().split("\t")[2]));
        }

    }

    public EdgeReader<LongWritable, FloatWritable> createEdgeReader(InputSplit split,
                                                                    TaskAttemptContext context) throws
            IOException{
        return new SidTidWithCostEdgeReader();
    }
}


