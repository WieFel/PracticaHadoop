package sos2016;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FacturacionMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text llamante = new Text();
	private IntWritable duracion = new IntWritable();

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer linesTokenizer = new StringTokenizer(value.toString());

		while (linesTokenizer.hasMoreTokens()) {
			// get line
			String linea = new String(linesTokenizer.nextToken());
			// split line by ","
			String[] llamada = linea.split(",");
			llamante.set(llamada[0]);
			int d = Integer.parseInt(llamada[3]);

			// 50 por establecimiento de llamada, 15 por cada segundo de llamada
			duracion.set(50 + d * 15);
			context.write(llamante, duracion);
		}
	}

}
