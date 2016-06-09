package sos2016;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FacturacionReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private Text llamante = new Text();
	private IntWritable coste = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable val : values) {
			sum += val.get();
		}
		llamante.set(key);
		coste.set(sum);
		context.write(llamante, coste);
	}

}
