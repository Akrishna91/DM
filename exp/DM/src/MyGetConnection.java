
import java.sql.Connection;
import java.sql.DriverManager;

import javax.swing.JOptionPane;

public class MyGetConnection {
	public static Connection con;
	
	static
	{

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			JOptionPane.showMessageDialog(null, "JDBC Driver has not found");
		}

		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost/SQL_mongo", "root", "");
		} catch (Exception e) {
			JOptionPane.showMessageDialog(null, "Error in Opening the Connection");
		}
	}
}
