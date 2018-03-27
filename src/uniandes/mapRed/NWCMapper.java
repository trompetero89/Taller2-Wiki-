/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.mapRed;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jorgehernandez
 */
public class NWCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    boolean start = false;
    boolean fecha_rango = false;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> wordlInLine = new HashMap<String, Integer>();
        String str = value.toString();
        boolean dateLine = str.contains("<DATE>");

        if (dateLine && !fecha_rango) {
            try {
                fecha_rango = rangeDate(value.toString());
            } catch (java.text.ParseException e) {
                e.printStackTrace();
            }
        }

        if (str.contains("<BODY>")) {
            start = true;
        } else if (str.contains("</BODY>") && start) {
            start = false;
            fecha_rango = false;
        }

        if (start) {
            String[] words = str.replaceAll("<BODY>", "").replaceAll("</DATELINE>", "").split("([().,!?:;'\"-]|\\s)+");
            for (String word : words) {
                if (fecha_rango && word.equals("year")) {
                    word.toLowerCase().trim();
                    wordlInLine.put(word, wordlInLine.containsKey(word) ? (wordlInLine.get(word) + 1) : 1);
                }
            }
            for (String k : wordlInLine.keySet()) {
                context.write(new Text(k), new IntWritable(wordlInLine.get(k)));
            }
        }
    }

    public boolean rangeDate(String lineRead) throws ParseException, java.text.ParseException {

        //boolean linea_aceptada = Linea_leida.toString().contains("<DATE>");
        if (lineRead.toString().contains("<DATE>")) {
            String[] palabras = lineRead.toString().split(" ");
            //System.out.println(palabras.length);						
            if (palabras.length > 1) {
                for (String palabra : palabras) {
                    String lw = palabra.toLowerCase().trim().replace("<date>", "");
                    if (lw.matches("[0-9]{2}-[a-z]{3}.[1-9]{4}")) {
                        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
                        String start_date = "01-mar-1987";
                        String end_date = "15-jul-1987";
                        //String comparar = "16-jun-1987";		
                        Date StartDate = sdf.parse(start_date);
                        Date EndDate = sdf.parse(end_date);
                        Date Fecha_Comparar = sdf.parse(lw.toString());
                        if (Fecha_Comparar.after(StartDate) && Fecha_Comparar.before(EndDate)) {
                            return true;
                        }

                    }
                }
            }
        }
        return false;
    }

}