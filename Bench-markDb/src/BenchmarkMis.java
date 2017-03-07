import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BenchmarkMis {	
	public static void main(String...a) throws IOException{
		String inFilePath = "/home/rohan/mis_queries_sqream.sql";
		String outFilePath = "/home/rohan/sqream_output10.xls";
		File ExelFile = new File(outFilePath);
		BufferedReader in = null;
		BufferedWriter out = null;
		try {
			in = new BufferedReader(new FileReader(inFilePath));
			if (!ExelFile.exists()) ExelFile.createNewFile();
	        FileWriter filewriter = new FileWriter(ExelFile.getAbsoluteFile());
	        out= new BufferedWriter(filewriter);
			String str=null;
			StringBuilder buildQuery = new StringBuilder();
			System.out.println("Getting connection...");
			Connection conn = getSqreamConnection();
			System.out.println("Connection Esatablished");
			System.out.println("Processing...");
			while((str = in.readLine()) != null){
				if(!str.isEmpty()) buildQuery.append(str);
				else{
					String[] arr = buildQuery.toString().split("~");
					buildQuery = new StringBuilder();
					ResultSet rs = null;
					PreparedStatement preparedStatement = null;
					if(arr.length > 1){
						preparedStatement = conn.prepareStatement(arr[1]);
						long sTime = System.currentTimeMillis();
						rs = preparedStatement.executeQuery();
						long eTime = System.currentTimeMillis();
						long timeTaken = eTime-sTime;
						out.write(arr[0]+","+timeTaken+"\n");
						out.flush();
						preparedStatement.close();
						rs.close();
					}
				}
			}
			System.out.println("\nProcess Completed");
		} catch (IOException | SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}finally {
			in.close();
			out.close();
		}
	}
	public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException{
		Connection conn = null;
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection("jdbc:mysql://10.0.6.174:3306/eaplive","root","Root@174");
		return conn;
		
	}
	public static Connection getSqreamConnection() throws ClassNotFoundException, SQLException{
		Connection conn = null;
		Class.forName("sql.SQDriver");
		conn = DriverManager.getConnection("jdbc:Sqream://10.0.6.110:5000/casper;user=sqream;password=sqream");
		return conn;
	}
}