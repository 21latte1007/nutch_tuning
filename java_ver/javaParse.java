import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.json.simple.JSONObject;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.time.LocalDateTime;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.ByteBuffer;

public class javaParse {
    public static void main(String[] args){
        String htmlFilePath = "./dump/dump";
        String keyFilePath = "./key.json";
        String jsonFilePath = "./news.json";
        String projectId = "<<PROJECT_ID>>";
        String datasetId = "<<DATASET_ID>>";
        String tableId = "<<TABLE_ID>>";
        
        try{
            String html = readFile(htmlFilePath);
            Document soup = Jsoup.parse(html);
            String[] segments = soup.html().split("Recno::");
            List<String> segmentList = Arrays.asList(segments).subList(1, segments.length);
            
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(keyFilePath));
            BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();
            TableId table = TableId.of(projectId, datasetId, tableId);
                
            for(String segment : segmentList) {
                Document seg = Jsoup.parse(segment);
                Elements tags = seg.select("meta[property=og:title], meta[property=og:regDate], meta[property=og:author], meta[property=og:url], meta[property=og:image], meta[property=og:description], p[dmcf-ptype=general], div[dmcf-ptype=general]");
                List<String> contents = new ArrayList<>();
                for(Element tag : tags)
                    contents.add(tag.toString() + "\n");
                
                if(contents.size() < 7)
                    continue;
                
                String title = contents.get(0);
                String regDate = contents.get(1);
                String author = contents.get(2);
                String url = contents.get(3);
                String image = contents.get(4);
                String description = contents.get(5);
                String content = "";
                
                for(int i = 6; i < contents.size(); i++)
                    content += contents.get(i);
                    
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("title", contents.get(0));
                jsonObject.put("regDate", contents.get(1));
                jsonObject.put("author", contents.get(2));
                jsonObject.put("url", contents.get(3));
                jsonObject.put("image", contents.get(4));
                jsonObject.put("description", contents.get(5));
                jsonObject.put("content", contents.get(6));
                jsonObject.put("timestamp", LocalDateTime.now().plusHours(9).toString());
                jsonObject.put("content", content);
                    
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String json = gson.toJson(jsonObject);
                
                try(FileWriter writer = new FileWriter("./news.json")){
                    writer.write(json);
                } catch(IOException e){
                    e.printStackTrace();
                }
                
                if(title.contains("og:title") && regDate.contains("og:regDate") && author.contains("og:article:author") && url.contains("og:url") && image.contains("og:description") && content.contains("general")){
                    System.out.println("Upload Pass");
                    
                    WriteChannelConfiguration writeConfig = WriteChannelConfiguration.newBuilder(table)
                        .setFormatOptions(FormatOptions.json())
                        .build();
                    
                    try(TableDataWriteChannel writer = bigquery.writer(writeConfig)){
                        byte[] jsonData = Files.readAllBytes(Paths.get(jsonFilePath));
                        writer.write(ByteBuffer.wrap(jsonData));
                    }catch(IOException | BigQueryException e){
                        e.printStackTrace();
                    }
                    System.out.println("Upload Pass");
                }
            }
        } catch(IOException e){
            e.printStackTrace();
        }
    }
    
    private static String readFile(String filePath) throws IOException {
        StringBuilder content = new StringBuilder();
        try(java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(filePath))){
            String line;
            while((line = reader.readLine()) != null){
                content.append(line).append(System.lineSeparator());
            }
        }
        return content.toString();
    }
    
    private static void writeFile(String filePath, List<String> lines) throws IOException {
        try(java.io.FileWriter writer = new java.io.FileWriter(filePath)){
            for(String line : lines){
                writer.write(line);
            }
        }
    }
}
