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




public class MapperWiki extends Mapper<LongWritable, Text, Text, NullWritable> {
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
        	
                  
            InputStream	 is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is); 
            doc.getDocumentElement().normalize(); 
            NodeList nList = doc.getElementsByTagName("page");
            String article;
            
            
            for (int temp = 0; temp < nList.getLength(); temp++) { 
                Node nNode = nList.item(temp); 
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {  
                	

                	Element eElement = (Element) nNode;  
                	String text = eElement.getElementsByTagName("text").item(0).getTextContent();
                	String title = eElement.getElementsByTagName("title").item(0).getTextContent();
                        String info_box;
                    
                    if(text.contains("{{Infobox")){                            	
                        int start_block = text.indexOf("{{Infobox");
                        
                        int end_block = text.indexOf("\n}}\n");
                        
                        if(end_block>start_block) {
	                         info_box = text.substring(start_block, end_block); 
	                         String[] atributos = info_box.split("[|]{1}");
	                         
	                          
	                         String atributo_titulo = "||||"+"{"+"\"id\""+": "+"\""+title.trim()+"\""+","+"\"group\""+": "+"1"+"}," ; 
	                         article = atributo_titulo.replace("\n", " ");
	                         
	                         //nodes
	                         for(int j=0;j<atributos.length;j++) { 
			                       if(atributos[j].toLowerCase().contains("=")) {
					                //crea atrbuto para los nodos relacionados	
					                String atributo_atributo = "{"+"\"id\""+": "+"\""+atributos[j].trim()+"\""+","+"\"group\""+": "+"2"+"}," ;
					                article =article.replace("\n", " ")+"||||"+atributo_atributo.replace("\n", " ");
					                					                            		
			                       }
			                  }
	                         //relations
	                         for(int k=0;k<atributos.length;k++) { 			                            			
			                        if(atributos[k].toLowerCase().contains("=")) {
			                        
			                        	String atributo_relacion = "{"+"\"source\""+": "+"\""+title.trim()+"\""+","+"\"target\""+": "+
				                            									"\""+atributos[k].trim()+"\""+","+"\"value\""+": "+"1"+"}," ; 
			                        	 article =article.replace("\n", " ")+"||||"+atributo_relacion.replace("\n", " ");
				                        
			 
			                        }
	                         }
	                         article= article+"<end_article>";
	                         context.write(new Text(article),null);
	                       
	                         
	                         
	                         
			              }
                    }
                }
            }
        } catch (Exception e) {
        }
}   
}