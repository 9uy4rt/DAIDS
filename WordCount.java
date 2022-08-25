import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	private static String sc;
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String wordString = new String();	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String valueString=value.toString();
			if(valueString.contains(sc)) {
			StringTokenizer itr = new StringTokenizer(valueString);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				wordString = word.toString();
				if (wordString.equals("결핵병")) {
					context.write(word, one);
				} else if (wordString.equals("가금티푸스")) {
					context.write(word, one);
				} else if (wordString.equals("브루셀라병")) {
					context.write(word, one);
				} else if (wordString.equals("돼지생식기호흡기증후군")) {
					context.write(word, one);
				} else if (wordString.equals("낭충봉아부패병")) {
					context.write(word, one);
				} else if (wordString.equals("사슴만성소모성질병")) {
					context.write(word, one);
				} else if (wordString.equals("고병원성조류인플루엔자")) {
					context.write(word, one);
				} else {

				}

			}

			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Scanner scanner=new Scanner(System.in);
		System.out.println("날짜 혹은 지역을 입력하세요.");
		System.out.println("ex.2022, 2022-07, 충청남도, 제주, 대전");
		sc=scanner.next();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
