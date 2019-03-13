import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class HadoopDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        System.exit(ret);
    }

    private Job createSimpleJob(String input, String output, String name) throws IOException{
        //Init job
        Job job = Job.getInstance(getConf(), name);
        job.setJarByClass(HadoopDriver.class);

        //Define input format
        TextInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class); //works without this line (if not FileInputFormat.class, because its an abstract)

        //Define output format
        TextOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class); //works without this line also

        return job;
    }

    private Job counterJob(String input, String output) throws IOException {
        Job job = createSimpleJob(input,output, "wordCount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TupleWritable.class);

        //Define mapper/combiner/reducer
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(Summer.class);

        //If types for map and reduce (outputs) are different, then:
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job;
    }

    private Job sorterJob(String input, String output) throws IOException {
        Job job = createSimpleJob(input, output, "DESC sorter");

        //Define mapper/combiner/reducer
        job.setMapperClass(SortMapper.class);
//        job.setNumReduceTasks(0); //turn off reducer
        job.setReducerClass(SortReducer.class);
//        job.setSortComparatorClass(TupleWritable.Comparator.class);

        //If types for map and reduce (outputs) are different, then:
        //MAP
        job.setMapOutputKeyClass(TupleWritable.class);
        job.setMapOutputValueClass(Text.class);

        //REDUCE
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TupleWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
			ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }

        String tempDir = args[1] + File.separator + "tmp";
        String finalDir = args[1] + File.separator + "final";
        Job counterJob = counterJob(args[0], tempDir);
        Job sorterJob = sorterJob(tempDir, finalDir);

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(counterJob)));
        System.out.println("Temp dir: " + tempDir);
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(sorterJob));

        boolean countRes = counterJob.waitForCompletion(true);
        if (countRes) {
            return sorterJob.waitForCompletion(true) ? 0 : 1;
        }
        return 1; //something went wrong
    }

}
