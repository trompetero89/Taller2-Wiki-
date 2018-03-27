/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.reuters.job;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import uniandes.mapRed.NCMapper;
import uniandes.mapRed.NCReducer;
import uniandes.mapRed.WCMapper;
import uniandes.mapRed.WCReducer;

/**
 *
 * @author cesar
 */
public class NewsCounter {
    
    public static void main(String[] args)  {
		if(args.length<2){
			System.out.println("Se necesitan las carpetas de entrada y salida");
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
	public static void ejecutarJob(String entrada, String salida) throws IOException,ClassNotFoundException, InterruptedException
	{
		/**
		 * Objeto de configuraci�n, dependiendo de la versi�n de Hadoop 
		 * uno u otro es requerido. 
		 * */
		Configuration conf = new Configuration();		
		Job ncJob=Job.getInstance(conf, "NewsCounter Job");
		ncJob.setJarByClass(NewsCounter.class);
		//////////////////////
		//Mapper
		//////////////////////
		
		ncJob.setMapperClass(NCMapper.class);
		
		ncJob.setMapOutputKeyClass(Text.class);
		ncJob.setMapOutputValueClass(IntWritable.class);
		///////////////////////////
		//Reducer
		///////////////////////////
		ncJob.setReducerClass(NCReducer.class);
		ncJob.setOutputKeyClass(Text.class);
		ncJob.setOutputValueClass(IntWritable.class);
		
		///////////////////////////
		//Input Format
		///////////////////////////
		//Advertencia: Hay dos clases con el mismo nombre, 
		//pero no son equivalentes. 
		//Se usa, en este caso, org.apache.hadoop.mapreduce.lib.input.TextInputFormat
		TextInputFormat.setInputPaths(ncJob, new Path(entrada));
		ncJob.setInputFormatClass(TextInputFormat.class); 
		
		////////////////////
		///Output Format
		//////////////////////
		TextOutputFormat.setOutputPath(ncJob, new Path(salida));
		ncJob.setOutputFormatClass(TextOutputFormat.class);
		ncJob.waitForCompletion(true);
		System.out.println(ncJob.toString());
	}
    
}
