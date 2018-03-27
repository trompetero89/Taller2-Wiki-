package uniandes.mapRed;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;




public class SearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
        	
            
            String Work = context.getConfiguration().get("Obra");
            String Artist = context.getConfiguration().get("Artista");
            String Place = context.getConfiguration().get("Lugar");
            String startDate = context.getConfiguration().get("Fecha");
            String endDate = context.getConfiguration().get("Fecha2");        
                    
        	
            InputStream	 is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is); 
            doc.getDocumentElement().normalize(); 
            NodeList nList = doc.getElementsByTagName("page"); 
            
            for (int temp = 0; temp < nList.getLength(); temp++) { 
                Node nNode = nList.item(temp); 
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {  
                	

                	Element eElement = (Element) nNode;  
                	String text = eElement.getElementsByTagName("text").item(0).getTextContent();
                        String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                	String info_box;
                   
                    
                	
                            if(text.contains("<page>")){                            	
                            	int start_block = text.indexOf("<page>");
                            	int end_block = text.indexOf("</page>");
                            	 
                            	if(end_block>start_block) {
                            		
	                            	info_box = text.substring(start_block, end_block); 
	                            	context.write(new Text(info_box), NullWritable.get());
	                            	
	                            	if(info_box.toLowerCase().contains(Work.toLowerCase()) ||
	                            	   info_box.toLowerCase().contains(Artist.toLowerCase()) ||
	                            	   info_box.toLowerCase().contains(Place.toLowerCase()) ||
	                            	   info_box.toLowerCase().contains(startDate.toLowerCase()) ||
	                            	   info_box.toLowerCase().contains(endDate.toLowerCase())
	                            	   ) {
	                            		context.write(new Text(info_box), NullWritable.get());
			                            		                          
			                            	
			                            	
	                            	     }
                            	}
                           }
                    
                    
                }
            }
                    
                    
        } catch (Exception e) {
        }
}   
}