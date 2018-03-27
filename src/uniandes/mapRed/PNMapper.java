/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.mapRed;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author cesar
 */
public class PNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
         @Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		HashMap<String, Integer> palabrasLinea=new HashMap<String, Integer>();
		
		boolean linea_valida = value.toString().contains("<PLACES>");
		if(linea_valida) {
			String[] palabras = value.toString().split("<P[A-Z]{4}S><D>|..D...P[A-Z]{4}S>|(<.D><D>)");
			
			if(palabras.length>1) {
				for(String palabra:palabras){
					String lw = palabra.toLowerCase().trim();
					if(lw.equals("")){continue;} //No queremos contar espacios
					//Si la palabra existe en el hashmap incrementa en 1 su valor,
					//en caso contrario la agrega y le asigna 1.
					palabrasLinea.put(lw,
							palabrasLinea.containsKey(lw)?
									(palabrasLinea.get(lw)+1)
									:1);
				}
			}
}
                	
		for(String k:palabrasLinea.keySet()){
			context.write(new Text(k), new IntWritable(palabrasLinea.get(k)));
		}
		
	
        }
    
}
