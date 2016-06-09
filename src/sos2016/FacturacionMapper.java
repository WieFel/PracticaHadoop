package sos2016;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input: (K,V)=(Object,Text)=(File identificator,File content)
 * Output: (K,V)=(Text,IntWritable)=(Llamante,Coste partial)
 */
public class FacturacionMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text llamante = new Text();
	private IntWritable coste = new IntWritable();

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
			int duracion = Integer.parseInt(llamada[3]);

			// 50 por establecimiento de llamada, 15 por cada segundo de llamada
			coste.set(50 + duracion * 15);
			context.write(llamante, coste);
		}
	}

}
