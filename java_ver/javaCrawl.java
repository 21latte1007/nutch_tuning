import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import java.time.LocalDate;

class javaCrawl {
    public static void main(String[] args){
        //** Action 1. Latest News URL ParseData **//
        String filePath = "./urls/freegen.txt";
        String userBaseUrl = "https://news.daum.net/breakingnews?page=";
        int numUrls = 1;
        generateURLs(userBaseUrl, numUrls, filePath);
        
        try {
            System.out.println("FreeGenerator Start");
            Process process = Runtime.getRuntime().exec("bin/nutch freegen urls/freegen.txt crawl-test/segments/");
            log(process);
            process.waitFor();

            System.out.println("Path memory");
            String folderPath = "./crawl-test/segments/";
            File folder = new File(folderPath);
            File[] files = folder.listFiles();
            Arrays.sort(files, Collections.reverseOrder());
            String recent = "./crawl-test/segments/" + files[0].getName();
            
            System.out.println("FetchList Fetch Start");
            process = Runtime.getRuntime().exec("./bin/nutch fetch " + recent);
            log(process);
            process.waitFor();
            
            System.out.println("FetchList Parse Start");
            process = Runtime.getRuntime().exec("./bin/nutch parse " + recent);
            log(process);
            process.waitFor();
            
            System.out.println("ParseData Getting");
            process = Runtime.getRuntime().exec("./bin/nutch readseg -dump ./crawl-test/segments/" + files[0].getName() + " dump -nofetch -nogenerate -noparse -noparsetext -nocontent");
            log(process);
            process.waitFor();
        } catch (IOException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        
        //** Action 2. Parse Outlink and Anchor **//
        String dump = "./dump/dump";
        String parse = "./dump/output.txt";
        String pattern = "toUrl: (.*?) anchor: (.*?)$";
            
        try(BufferedReader reader = new BufferedReader(new FileReader(dump));
            BufferedWriter writer = new BufferedWriter(new FileWriter(parse))){
            Pattern regexPattern = Pattern.compile(pattern);
            
            String line;
            while((line = reader.readLine()) != null){
                Matcher matcher = regexPattern.matcher(line);
                if(matcher.find()){
                    String outlink = matcher.group(1);
                    String anchor = matcher.group(2);
                    
                    writer.write(outlink + ", " + anchor + "\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
            
        //** Action 3. Delete Duplication Link **//
        String inputPath = "./dump/output.txt";
        String outputPath = "./dump/result.txt";
        
        String[] lines = readData(inputPath);
        String[] filteredLines = removeDuplicates(lines);
        
        writeData(outputPath, filteredLines);
        
        //** Action 4. Lastest News Crawling **//
        String outputFilePath = "./dump/result.txt";
        
        LocalDate today = LocalDate.now();
        String seedDirectoryPath = String.format("./urls/%s", today);
        String seedFilePath = String.format("%s/seed.txt", seedDirectoryPath);
        String completedFilePath = String.format("./urls/complete_%s.txt", today);
        
        File seedDirectory = new File(seedDirectoryPath);
        if(!seedDirectory.exists()){
            seedDirectory.mkdirs();
            try{
                new File(seedFilePath).createNewFile();
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        
        File completedFile = new File(completedFilePath);
        if(!completedFile.exists()){
            try{
                Files.copy(Paths.get("./urls/complete.txt"), completedFile.toPath());
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        
        List<String> newUrls = readLinesFromFile(outputFilePath);
        List<String> completeUrls = readLinesFromFile(completedFilePath);
        List<String> uniqueUrls = new ArrayList<>();
        List<String> combinedUrls = new ArrayList<>();
        
        for(String url : newUrls){
            String link = url.split(",")[0].trim();
            if(!completeUrls.contains(link)){
                uniqueUrls.add(link);
            }
        }
        
        combinedUrls.addAll(completeUrls);
        combinedUrls.addAll(uniqueUrls);
        
        writeLinesToFile(seedFilePath, uniqueUrls);
        writeLinesToFile(completedFilePath, combinedUrls);
        
        String crawlCommand = String.format("bin/crawl -i -s urls/%s/ crawl-test/ 1", today);
        executeCommand(crawlCommand);
        
        String folderPath = "./crawl-test/segments/";
        File[] subdirectories = new File(folderPath).listFiles(File::isDirectory);
        if(subdirectories != null && subdirectories.length > 0){
            File recentDirectory = subdirectories[0];
            String recentSegmentPath = recentDirectory.getPath();
            String readSegCommand = String.format("bin/nutch readseg -dump %s dump -nofetch -nogenerate -noparse -noparsetext -noparsedata", recentSegmentPath);
            executeCommand(readSegCommand);
        }
        
        deleteDirectory("./crawl-test/crawldb");
        deleteDirectory("./crawl-test/linkdb");
    }
    public static void generateURLs(String baseUrl, int numUrls, String filePath) {
        try(FileWriter fileWriter = new FileWriter(filePath)){
            for(int i = 1; i <= numUrls; i++){
                String url = baseUrl + i;
                fileWriter.write(url + "\n");
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    public static void log(Process process) throws IOException{
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while((line = reader.readLine()) != null){
            System.out.println(line);
        }
    }
    public static String[] removeDuplicates(String[] lines){
        Set<String> uniqueOutlinks = new HashSet<>();
        Set<String> uniqueAnchors = new HashSet<>();
        StringBuilder filteredDataBuilder = new StringBuilder();
        
        for(String line : lines){
            String[] lineParts = line.split(", ");
            String outlink = lineParts[0];
            String anchor = String.join(", ", lineParts[1]);
            
            if(!uniqueOutlinks.contains(outlink) && !uniqueAnchors.contains(anchor)){
                filteredDataBuilder.append(line).append("\n");
                uniqueOutlinks.add(outlink);
                uniqueAnchors.add(anchor);
            }
        }
        return filteredDataBuilder.toString().split("\n");
    }
    public static String[] readData(String filePath){
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))){
            return reader.lines().toArray(String[]::new);
        }catch(IOException e){
            e.printStackTrace();
            return new String[0];
        }
    }
    public static void writeData(String filePath, String[] lines){
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))){
            for(String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    private static List<String> readLinesFromFile(String filePath){
        List<String> lines = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))){
            String line;
            while((line = reader.readLine()) != null){
                lines.add(line);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
        return lines;
    }
    private static void writeLinesToFile(String filePath, List<String> lines){
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))){
            for(String line : lines){
                writer.write(line);
                writer.newLine();
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    private static void executeCommand(String command){
        try{
            Process process = Runtime.getRuntime().exec(command);
            log(process);
            process.waitFor();
        }catch(IOException | InterruptedException e){
            e.printStackTrace();
        }
    }
    private static void deleteDirectory(String directoryPath){
        try{
            Path path = Paths.get(directoryPath);
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
