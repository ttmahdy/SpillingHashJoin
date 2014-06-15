package HashToDisk;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.*;

public class SpillFileHandler {

	public String getFileName() {
		return fileName;
	}

	public Path getFilePath() {
		return filePath;
	}

	public SpillFileHandler(String fileName) throws IOException
	{
		this.fileName = fileName;
		fileToWriteTo = new File(fileName);
		filePath = fileToWriteTo.toPath();
		
		if (fileToWriteTo.exists())
		{
			fileToWriteTo.delete();
		}
		else
		{
			try {
				fileToWriteTo.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.format("createFile error: %s%n", e);
			}
		}
		
		fileWriter = new FileWriter(fileName);
		bufferedWriter = new BufferedWriter(fileWriter,bufferSize);
	}
	
	public void writeToFile(String s) throws IOException
	{
		bufferedWriter.write(s+"\n");
	}
	
	public void CloseFile() throws IOException
	{
			bufferedWriter.flush();
			bufferedWriter.close();
	}
	
	String fileName;
	File fileToWriteTo;
	FileWriter fileWriter;
	BufferedWriter bufferedWriter;
	Path filePath;
	Charset charset = Charset.forName("US-ASCII");
	final int bufferSize = 262144;
}
