import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class SourceTarget_Reader
{
	
	private Properties property=new Properties();
	private InputStream input=null;
	String sourceDB="",targetDB="";
	
	public SourceTarget_Reader()
	{
			try {
				
				input=new FileInputStream("./sourceTargetInfo/sourceTargetInfo.properties");
				property.load(input);
				  
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	//end try-catch()
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 	
	 		sourceDB=property.getProperty("SOURCE");
	 		targetDB=property.getProperty("TARGET");
	 
	}//end SourceTarget_Reader()
	
	public String getSourceDB()
	{
		return sourceDB;
	}
	
	public String getTargetDB()
	{
		return targetDB;
	}
}
