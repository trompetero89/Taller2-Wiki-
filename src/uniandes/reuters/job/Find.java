package uniandes.reuters.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uniandes.mapRed.FindMapper;
import uniandes.mapRed.WCReducer;


public class Find { 
	public static void main(String[] args)  {
		if(args.length<2){
			System.out.println("Se necesitan las carpetas de entrada y salida");
			System.exit(-1);
		}
		String entrada = args[0]; //carpeta de entrada
		String salida = args[1];//La carpeta de salida no puede existir
		String art_work = args[2];
		String artist = args[3];	
                String place = args[4];
                String start_date = args[5];
                String end_date = args[6];
		
		try {
			ejecutarJob(entrada, salida , art_work, artist, place, start_date, end_date);
		} catch (Exception e) { //Puede ser IOException, ClassNotFoundException o InterruptedException
			e.printStackTrace();
		} 
		
	}
	public static void ejecutarJob(String entrada, String salida, String art_work, String artist, String place, String start_date, String end_date) throws IOException,ClassNotFoundException, InterruptedException
	{
		
		/**
		 * Objeto de configuraci�n, dependiendo de la versi�n de Hadoop 
		 * uno u otro es requerido. 
		 * 
		 * 
		 * */
		String Artwork = art_work;
		String Artist = artist;
                String Place = place;
                String Startdate = start_date;
                String Enddate = end_date;
	
        
		Configuration conf = new Configuration();
			
		conf.set("Obra", Artwork);
		conf.set("Artista", Artist);
                conf.set("Place", Place);
                conf.set("Fecha", Startdate);
                conf.set("Fecha2", Enddate);
        
		Job wcJob=Job.getInstance(conf, "Find");
		wcJob.setJarByClass(Find.class);
		//////////////////////
		//Mapper
		//////////////////////
		
		wcJob.setMapperClass(FindMapper.class);
		
		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(IntWritable.class);
		///////////////////////////
		//Reducer
		///////////////////////////
		wcJob.setReducerClass(WCReducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		
		///////////////////////////
		//Input Format
		///////////////////////////
		//Advertencia: Hay dos clases con el mismo nombre, 
		//pero no son equivalentes. 
		//Se usa, en este caso, org.apache.hadoop.mapreduce.lib.input.TextInputFormat
		TextInputFormat.setInputPaths(wcJob, new Path(entrada));
		wcJob.setInputFormatClass(TextInputFormat.class); 
		
		////////////////////
		///Output Format
		//////////////////////
		TextOutputFormat.setOutputPath(wcJob, new Path(salida));
		wcJob.setOutputFormatClass(TextOutputFormat.class);
		wcJob.waitForCompletion(true);
		System.out.println(wcJob.toString());
	}
}