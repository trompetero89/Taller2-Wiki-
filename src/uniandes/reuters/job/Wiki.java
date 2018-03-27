package uniandes.reuters.job;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import uniandes.mapRed.MapperWiki;
import uniandes.mapRed.PNReducer;
import util.*;




public class Wiki {
	
	public static void main(String[] args)  {
		
		if(args.length<2){
			System.out.println("Falta alguno de los parametros para realizar la busqueda");
			System.exit(-1);
		}
		String entrada = args[0]; //carpeta de entrada
		String salida = args[1];//La carpeta de salida no puede existir
		
		try {

			ejecutarJob(entrada, salida);
		} catch (Exception e) { //Puede ser IOException, ClassNotFoundException o InterruptedException
			e.printStackTrace();
		}
		
	}
	
	public static void ejecutarJob(String entrada, String salida) 
                throws IOException,ClassNotFoundException, InterruptedException
	{
		

		
		Configuration conf = new Configuration(true);
		
		conf.set("START_TAG_KEY","<page>");		
		conf.set("END_TAG_KEY","</page>");		
        
		
		Job wcJob=Job.getInstance(conf, "Wiki Job");		
		wcJob.setJarByClass(Wiki.class);	
		
		//Mapper	
		wcJob.setMapperClass(MapperWiki.class);		
		wcJob.setNumReduceTasks(0);
		//wcJob.setInputFormatClass(XmlInputFormat.class);
		wcJob.setInputFormatClass(XmlFormatCompress.class); 
		wcJob.setMapOutputKeyClass(Text.class);		
		wcJob.setMapOutputValueClass(LongWritable.class);	
		
		//Reducer
		wcJob.setReducerClass(PNReducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(LongWritable.class);		
		
		///Input Format
		//XmlFormat.setInputPaths(wcJob, new Path(entrada));
		XmlFormatCompress.setInputPaths(wcJob, new Path(entrada));
		///Output Format
		TextOutputFormat.setOutputPath(wcJob, new Path(salida));
		wcJob.setOutputFormatClass(TextOutputFormat.class);
		wcJob.waitForCompletion(true);		
		System.out.println(wcJob.toString());                   
                        
		
	}
}