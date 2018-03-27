package uniandes.reuters.job;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import uniandes.mapRed.SearchMapper;
import util.*;




public class Search {
	public static void main(String[] args)  {
		
		if(args.length<5){
			System.out.println("Falta alguno de los parametros para realizar la busqueda");
			System.exit(-1);
		}
		String entrada = args[0]; //carpeta de entrada
		String salida = args[1];//La carpeta de salida no puede existir
		String Work = args[2];
		String Artist = args[3];	
        String Place = args[4];
        String startDate = args[5];
        String endDate = args[6];
		
		try {

			ejecutarJob(entrada, salida , Work, Artist, Place, startDate, endDate);
		} catch (Exception e) { //Puede ser IOException, ClassNotFoundException o InterruptedException
			e.printStackTrace();
		}
		
	}
	
	public static void ejecutarJob(String entrada, String salida, String Work, String Artist, String Place, String startDate, String endDate) 
                throws IOException,ClassNotFoundException, InterruptedException
	{
		
		String work = Work;
		String artist = Artist;
                String place = Place;
                String startdate = startDate;
                String enddate = endDate;
		
		Configuration conf = new Configuration(true);
		
		conf.set("START_TAG_KEY","<page>");		
		conf.set("END_TAG_KEY","</page>");		
		conf.set("Obra", work);
		conf.set("Artista", artist);
                conf.set("Lugar", place);
                conf.set("Fecha", startdate);
                conf.set("Fecha2", enddate);
        
        
		
		Job wcJob=Job.getInstance(conf, "SearchWiki");		
		wcJob.setJarByClass(Search.class);	
		
		//Mapper	
		wcJob.setMapperClass(SearchMapper.class);		
		wcJob.setNumReduceTasks(0);
		//wcJob.setInputFormatClass(XmlInputFormat.class); //original_funciona
		wcJob.setInputFormatClass(XmlFormatCompress.class); 
		wcJob.setMapOutputKeyClass(Text.class);		
		wcJob.setMapOutputValueClass(LongWritable.class);	
		
		//Reducer
		/*wcJob.setReducerClass(Records_Reducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(LongWritable.class);		*/
		
		///Input Format
		//XmlInputFormat.setInputPaths(wcJob, new Path(entrada)); 
		XmlFormatCompress.setInputPaths(wcJob, new Path(entrada));
		///Output Format
		TextOutputFormat.setOutputPath(wcJob, new Path(salida));
		wcJob.setOutputFormatClass(TextOutputFormat.class);
		wcJob.waitForCompletion(true);		
		System.out.println(wcJob.toString());                   
                        
		
	}
}