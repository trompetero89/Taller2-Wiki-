package uniandes.mapRed;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class FindMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		HashMap<String, Integer> palabrasLinea=new HashMap<String, Integer>();
		
		
        String art_work = context.getConfiguration().get("Obra");
        String artist = context.getConfiguration().get("Artista");
        String place = context.getConfiguration().get("Place");
        String start_date = context.getConfiguration().get("Fecha");
        String end_date = context.getConfiguration().get("Fecha2");   
		
        
		String[] palabras = value.toString().split("<end_article>");
		for(String palabra:palabras){
			if(palabra.toLowerCase().contains(art_work.toLowerCase()) ||
					palabra.toLowerCase().contains(artist.toLowerCase()) ||
					palabra.toLowerCase().contains(place.toLowerCase()) ||
					palabra.toLowerCase().contains(start_date.toLowerCase()) ||
					palabra.toLowerCase().contains(end_date.toLowerCase()) 				
					){
				String lw=palabra.toLowerCase().trim();
				if(lw.equals("")){continue;} //No queremos contar espacios
				//Si la palabra existe en el hashmap incrementa en 1 su valor,
				//en caso contrario la agrega y le asigna 1.
				palabrasLinea.put(lw,
						palabrasLinea.containsKey(lw)?
								(palabrasLinea.get(lw)+1)
								:1);
			}
		}
		for(String k:palabrasLinea.keySet()){
			context.write(new Text("||||"+k), new IntWritable(palabrasLinea.get(k)));
		}
		
	}
}