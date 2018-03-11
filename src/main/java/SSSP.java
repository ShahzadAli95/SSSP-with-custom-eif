import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.*;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;


import java.io.IOException;


public class SSSP extends
        BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    public static final LongConfOption SOURCE_ID = new
            LongConfOption("SimpleShortestPathsVertex.sourceId",1L,
            "Source Vertex of ID 1");

    private boolean isSource(Vertex<LongWritable, ?, ?> vertex){
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable,
            FloatWritable> vertex, Iterable<DoubleWritable>
                                messages) throws IOException {
        if (getSuperstep() == 0) {
            vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
        }

        double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }
        if (minDist < vertex.getValue().get()) {
            vertex.setValue(new DoubleWritable(minDist));

            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()){
                double distance = minDist + edge.getValue().get();
                sendMessage(edge.getTargetVertexId(),new DoubleWritable(distance));
            }

        }
        vertex.voteToHalt();
    }
    public static void main(String[] args) throws Exception{
        String inputPath = "/home/hadoopUser/eif.txt";
        String outputPath = "/tmp/Nust_eif_sssp_out";
        //GiraphConfiguration Object
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        //Computation Class
        giraphConf.setComputationClass(SimpleShortestPathsComputation.class);
        //Input Format
        giraphConf.setEdgeInputFormatClass(SidTidEdgeInputFormat.class);
        //Input Path
        GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(inputPath));
        //Other properties
        giraphConf.setLocalTestMode(true);
        giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
        giraphConf.setWorkerConfiguration(1,1,100);
        //Output Format
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        //creation of Giraph Job object
        GiraphJob giraphJob = new GiraphJob(giraphConf, "SimpleShortestPathsComputation");
        //Output Path
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(outputPath));
        //run Giraph job
        giraphJob.run(true);
    }
}