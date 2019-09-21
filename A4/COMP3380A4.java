import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.Scanner;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class COMP3380A4 {
    static Connection connection;

    public static void main(String[] args) throws Exception {
	
		// startup sequence
		MyDatabase db = new MyDatabase();		
		runConsole(db);

		System.out.println("Exiting...");
	}
	
	public static void runConsole(MyDatabase db){

		Scanner console = new Scanner(System.in);

		System.out.print("db > ");
		String line = console.nextLine();
        String [] parts;
        String arg = "";

		while(line != null && !line.equals("q")){
            parts = line.split("\\s+");
            if (line.indexOf(" ") > 0)
                arg = line.substring(line.indexOf(" ")).trim();
			if (parts[0].equals("h"))
				printHelp();
			else if (parts[0].equals("w")){
				db.allWards();
			}
			else if (parts[0].equals("c")){
				db.allCouncilors();
			}
			else if (parts[0].equals("e")){
				db.allExpenses();
			}
			else if (parts[0].equals("wt")){
				try{
					if (parts.length >= 2)
						db.singleWard(arg);
					else
						System.out.println("Require an argument for this command");
				} catch(Exception e){
					System.out.println("id must be an integer");
				}
			}
			else if (parts[0].equals("ct")){
				if (parts.length >= 2)
					db.singleCouncilor(arg);
				else
					System.out.println("Require an argument for this command");
			}
			else if (parts[0].equals("de")){
				try {
					if (parts.length >= 2 )
						db.deleteExpense(Integer.parseInt(arg));
					else
						System.out.println("Require an argument for this command");
				} catch(Exception e){
					System.out.println("id must be an integer");
				}
			}
			else if (parts[0].equals("dc")){
				if (parts.length >= 2 )
					db.deleteCouncilor(arg);
				else
					System.out.println("Require an argument for this command");
			}
			else if (parts[0].equals("m")){
				db.highestExpense();
			}
			else
				System.out.println("Read the help with h, or find help somewhere else.");

			System.out.print("db > ");
			line = console.nextLine();
		}

		console.close();
	}

	private static void printHelp(){
		System.out.println("Winnipeg Council Member Expenses console");
		System.out.println("Commands:");
		System.out.println("h - Get help");
		System.out.println("w - Print all wards");
		System.out.println("e - Print all expenses with associated ward and councilor");
		System.out.println("c - Print all coucillors");
		System.out.println("ct name - Print total expenses for cNames 'name'");
		System.out.println("wt name - Print total expenses for ward 'name'");
		System.out.println("dc name - Delete cNames named 'name'");
		System.out.println("de id - delete expense 'id'");
		System.out.println("m - Show the highest single-time expense for each cNames");
		System.out.println("---- end help ----- ");
	}
}

class MyDatabase{
	private Connection connection;
	private final String filename = "Council_Member_Expenses.csv";
	public MyDatabase(){
		try {
			Class.forName("org.hsqldb.jdbcDriver");
			// creates an in-memory database
			connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");

			createTables();
			readInData();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace(System.out);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	private void createTables(){
		// TODO
		// To be completed
				
		// Create Wards table
		String Wards = "CREATE TABLE Wards ( "+
			" wID INTEGER PRIMARY KEY IDENTITY,"+
			" wOffice VARCHAR(60)"+
			");";
		try {
			connection.createStatement().executeUpdate(Wards);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
		
		//Create cNames table - FK reference to Wards
		String Councilors = "CREATE TABLE Councilors ( "+
			" cID INTEGER PRIMARY KEY IDENTITY,"+
			" wID INTEGER,"+
			" cName VARCHAR(60),"+
			" FOREIGN KEY (wID) REFERENCES Wards ON DELETE CASCADE"+
			");";
		try {
			connection.createStatement().executeUpdate(Councilors);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
		
		// Create Expenses Table - FK reference to cNames
		String Expenses = "CREATE TABLE Expenses ( "+
			" eID INTEGER PRIMARY KEY IDENTITY,"+
			" cID INTEGER,"+
			" eDate DATE,"+
			" eVendor VARCHAR(60),"+
			" eType VARCHAR(60),"+
			" eDescription VARCHAR(200),"+
			" eAcctName VARCHAR(100),"+
			" eAmount DOUBLE,"+
			" FOREIGN KEY (cID) REFERENCES Councilors ON DELETE CASCADE"+
			");";
		try {
			connection.createStatement().executeUpdate(Expenses);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}
	
	public void allWards() {
		// Works, but the table must be corrected!
		try {
			String sql = "SELECT * FROM Wards;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {
				System.out.println("wID: " + resultSet.getInt("wID") + "\t| Ward Office: " + resultSet.getString("wOffice"));
				
			}

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}
	
	public void allExpenses(){
		// TODO
		try {
			String sql = "SELECT * FROM Expenses E, Councilors C, Wards W WHERE E.cID=C.cID AND c.wID=W.wID;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {
				System.out.println("eID: " + resultSet.getInt("eID") + "  | Date: " + resultSet.getDate("eDate") + "  | Amount: $" + resultSet.getDouble("eAmount") + "  | Type: " + resultSet.getString("eType") + "  | Vendor: " + resultSet.getString("eVendor") + "  | Description: " + resultSet.getString("eDescription") + "  | Account: " + resultSet.getString("eAcctName") + "  | Ward Office: " + resultSet.getString("wOffice") + "  | Councilor: " + resultSet.getString("cName"));
				
			}

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}
	
	public void allCouncilors() {
		// TODO
		try {
			String sql = "SELECT * FROM Councilors;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {
				System.out.println("cID: " + resultSet.getInt("cID") + "\t| Councilor: " + resultSet.getString("cName"));
				
			}

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}

	public void singleCouncilor(String name) {
		// TODO
		String sqlName = "'" + name + "'";
		
		try {
			String sql = "SELECT SUM(E.eAmount) AS sum_eAmount FROM Expenses E, Councilors C WHERE E.cID=C.cID AND C.cName = " + sqlName + ";";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {
				if(resultSet.getDouble("sum_eAmount") == 0)
					System.out.println("Sum = 0 --- probably entered wrong name or command...");
				else				
					System.out.printf("Councilor: %s  | Expense Total: $%.2f\n", name, resultSet.getDouble("sum_eAmount"));
			}

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}

	public void singleWard(String wardID) {
		// TODO
		String sqlName = "'" + wardID + "'";
		
		try {
			String sql = "SELECT SUM(E.eAmount) AS sum_eAmount FROM Expenses E, Councilors C, Wards W WHERE E.cID=C.cID AND C.wID=W.wID AND W.wOffice = " + sqlName + ";";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {
				if(resultSet.getDouble("sum_eAmount") == 0)
					System.out.println("Sum = 0 --- probably entered wrong name or command...");
				else	
					System.out.printf("Ward: %s  | Expense Total: $%.2f\n", wardID, resultSet.getDouble("sum_eAmount"));
				
			}

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}

	public void deleteCouncilor(String name){
		// TODO
		String sqlName = "'" + name + "'";
		
		try {
			String sql = "DELETE FROM Councilors WHERE cName = " + sqlName + ";";
			System.out.println("Deleting " + sqlName + " from Councilors table (and associated expenses)...");
			connection.createStatement().executeUpdate(sql);
		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}		
	}

	public void deleteExpense(int expenseID){
		// TODO
		try {
			String sql = "DELETE FROM Expenses WHERE eID = " + expenseID + ";";
			System.out.println("Deleting eID: " + expenseID + " from Expenses table...");
			connection.createStatement().executeUpdate(sql);
		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}	
	}
	
	public void highestExpense() {
		// TODO
		try {
			String sql = "SELECT C.cName, MAX(eAmount) AS max_eAmount FROM Expenses E, Councilors C WHERE E.cID=C.cID GROUP BY C.cID;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {
				System.out.printf("Councilor: %s  | Max Expense: $%.2f\n", resultSet.getString("cName"), resultSet.getDouble("max_eAmount"));
				
			}

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}
	
	private java.sql.Date getSQLDate(String date) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MM/yyyy");
		java.util.Date dateJava = new java.util.Date();
		try {
			dateJava = sdf1.parse(date);
		}catch(Exception e){
			System.out.println("Cannot convert date");
		}
		java.sql.Date sqlDate = new java.sql.Date(dateJava.getTime());
		return sqlDate;
	}
	
	private void readInData(){

		// TODO
		// to be corrected and completed
		
		// Used to check if entry already exists
		boolean exists;
		
		// Wards Table Declarations
		int[] wIDs = new int[100];
		int wardCount = -1;
		int newWardID = -1;
		String newWardOffice = null;
		
		// cNames Table Declarations
		String[] cNames = new String[100];
		String newCName = null;
		int cID = -1; // Will use this as autoincrement ID
		
		// Expenses Table Declarations
		int eID = -1; // Will use this as autoincrement ID
		int expenseCID = -1;
		java.sql.Date eDate = null;
		String eVendor = null;
		String eType = null;
		String eDescription = null;
		String eAcctName = null;
		double eAmount = -1;
		
		
		// Buffered Input Reader
		BufferedReader in = null;

		try {
			in = new BufferedReader((new FileReader(filename)));
		
			// throw away the first line - the header
			in.readLine();

			// Read each relevant line w/ data
			String line = in.readLine();
			while (line != null) {
				// split naively on commas
				// good enough for this dataset!
				String[] parts = line.split(",");
				
				
				//-------------------------
				// Acquire UNIQUE Ward Info
				// and store in Wards table
				//-------------------------
				
				newWardID = Integer.parseInt(parts[0]);
				newWardOffice = parts[1];
				
				// Check if ward already exists
				exists = false;
				for (int i = 0; i <= wardCount; i++){
					if (newWardID == wIDs[i])
						exists = true;
				}
					
				// Insert new ward if not yet inserted
				if (!exists){
					wardCount++;
					wIDs[wardCount] = newWardID;
					
					//System.out.println(newWardID + " " + newWardOffice);
					
					PreparedStatement pstmt = connection.prepareStatement(
						"INSERT INTO Wards (wID, wOffice) VALUES (?, ?);");
					
					pstmt.setInt(1, newWardID);
					pstmt.setString(2, newWardOffice);

					pstmt.executeUpdate();
					pstmt.close();
				}
				
				
				//------------------------------
				// Acquire UNIQUE Councilor Info
				// and store in cNames table
				//------------------------------
					
				newCName = parts[2];
				
				// Check if councilor already exists
				exists = false;
				for (int i = 0; i <= cID; i++){
					if (newCName.equals(cNames[i]))
						exists = true;
				}
					
				// Insert new councilor if not yet inserted
				if (!exists){
					cID++;
					cNames[cID] = newCName;
					
					//System.out.println(cIDs + " " + newWardID + " " + newCName);
					
					PreparedStatement pstmt = connection.prepareStatement(
						"INSERT INTO Councilors (cID, wID, cName) VALUES (?, ?, ?);");
					
					pstmt.setInt(1, cID);
					pstmt.setInt(2, newWardID);
					pstmt.setString(3, newCName);

					pstmt.executeUpdate();
					pstmt.close();
				}
				
				
				//------------------------------
				// Acquire ALL Expense Info
				// and store in Expenses table
				//------------------------------
				
				// Acquire all values				
				eID++;
				
				// Find the councilor who made the expense
				for (int i = 0; i <= cID; i++){
					if(newCName.equals(cNames[i]))
						expenseCID = i;
				}
				
				eDate = getSQLDate(parts[3]);
				eVendor = parts[4];
				eType = parts[5];
				
				// Number of columns vary - eDescription and eAcctName may not 
				// be correct
				eDescription = parts[parts.length-3];
				eAcctName = parts[parts.length-2];
				eAmount = Double.parseDouble(parts[parts.length-1]);										
				
				// if(eID == 0){
					// System.out.println(eID);
					// System.out.println(expenseCID);
					// System.out.println(eDate);
					// System.out.println(eVendor);
					// System.out.println(eType);
					// System.out.println(eDescription);
					// System.out.println(eAcctName);
					// System.out.println(eAmount);
				// }
				
				// Insert new expense
				PreparedStatement pstmt = connection.prepareStatement(
					"INSERT INTO Expenses (eID, eDate, cID, eVendor, eType, eDescription, eAcctName, eAmount) VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

				pstmt.setInt(1, eID);
				pstmt.setDate(2, eDate);
				pstmt.setInt(3, expenseCID);
				pstmt.setString(4, eVendor);
				pstmt.setString(5, eType);
				pstmt.setString(6, eDescription);
				pstmt.setString(7, eAcctName);
				pstmt.setDouble(8, eAmount);

				pstmt.executeUpdate();
				pstmt.close();
				
				
				// prepared statement:
				// see http://hsqldb.org/doc/2.0/apidocs/org/hsqldb/jdbc/JDBCPreparedStatement.html
				
				// PreparedStatement pstmt = connection.prepareStatement(
					// "INSERT INTO Wards (wID, wOffice) VALUES (?, ?);"
				// );
				// pstmt.setInt(1, Integer.parseInt(parts[0]));
				// pstmt.setString(2, parts[1]);

				// pstmt.executeUpdate();
				
				// get next line
				line = in.readLine();
				//System.out.println(line);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}
}