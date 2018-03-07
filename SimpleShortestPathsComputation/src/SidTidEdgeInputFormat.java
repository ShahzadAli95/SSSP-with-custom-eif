import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.StringWriter;

public class SidTidEdgeInputFormat extends org.apache.giraph.io.formats.TextEdgeInputFormat<LongWritable,CustomEdgeValueWritable>{
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

        //CONSIDERING THE NEW CONSTRUCTOR, PLEASE REVIEW THIS:
        @Override
        protected CustomEdgeValueWritable getValue(org.apache.hadoop.io.Text line) throws IOException {
            return new CustomEdgeValueWritable(Float.parseFloat(line.toString().split("\t")[2]), Character.valueOf(char c));
        }

/*        protected String getType(org.apache.hadoop.io.Text line) throws IOException {
            return new String(String.valueOf(line.toString().split("\t")[3]));
        }

        protected FloatWritable startTime(org.apache.hadoop.io.Text line) throws IOException {
            return new FloatWritable(Long.parseLong(line.toString().split("\t")[4]));
        }

        protected FloatWritable endTime(org.apache.hadoop.io.Text line) throws IOException {
            return new FloatWritable(Long.parseLong(line.toString().split("\t")[5]));
*/        }

    }

    public EdgeReader<LongWritable, FloatWritable> createEdgeReader(InputSplit split,
                                                                   TaskAttemptContext context) throws
            IOException{
        return new SidTidWithCostEdgeReader();
    }
}

