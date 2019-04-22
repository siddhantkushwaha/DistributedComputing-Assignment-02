import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Search extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("query", args[2]);

        Job job = Job.getInstance(configuration, "job_search");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            String query = configuration.get("query");

            String line = lineText.toString();
            String sentence = line.split(",")[2].replaceAll("^\"|\"$", "");

            if (sentence.contains(query)) {
                Text sentenceText = new Text(sentence);
                context.write(NullWritable.get(), sentenceText);
            }
        }
    }

    public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text text : values) {
                context.write(NullWritable.get(), text);
            }
        }
    }
}
