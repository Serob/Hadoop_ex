import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class HadoopDriver extends Configured implements Tool {

    final static String PARAM_LENGTH = "length";
    final static String PARAM_IS_SET = "isSet";

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
			ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }

        Configuration conf = getConf();

        try {
            Integer.parseInt(args[2]);
            conf.set(PARAM_LENGTH, args[2]);
            conf.set(PARAM_IS_SET, Boolean.toString(true));
            System.out.println("-- Will search words of " + args[2] + " length.");
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException ex){
            conf.set(PARAM_IS_SET, Boolean.toString(false));
            System.out.println("-- Will search words of ANY length.");
        }

        //Init job
		Job job = Job.getInstance(conf, "WordCounter");
        job.setJarByClass(HadoopDriver.class);

        //Define input format
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class); //works without this line (if not FileInputFormat.class, because its an abstract)

        //Define output format
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class); //works without this line also
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //Define mapper/combiner/reducer
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(Summer.class);

/*		//If types for map and reduce (outputs) are different, then:
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);*/

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
