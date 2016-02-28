import com.mongodb.BasicDBObject;
import org.json.simple.JSONObject;
import org.json.*;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.*;

//import javax.swing.text.html.AccessibleHTML.TableElementInfo.TableAccessibleContext;

public class MongoConnect {
	// MongoClient client;

	static DB db;

	// Array
	// static HashMap<String, ArrayList<String>> table = new HashMap<String,
	// ArrayList<String>>();

	// ArrayList<ArrayList<String>> table = new ArrayList<ArrayList<String>>();

	public static int array_search(String array[], String temp) {
		for (int k = 0; k < array.length; k++) {
			if (array[k].equalsIgnoreCase(temp)) {
				return k;

			}
		}

		return -1;
	}

	// static DB db;
	public static DB getConnection(String localhost, int port, String database) {
		MongoClient client = null;

		db = null;
		try {
			client = new MongoClient(localhost, port);
			db = client.getDB(database);
			return db;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return db;
	}

	public static void getArray(Object object2) throws ParseException,
			SQLException {

		JSONArray jsonArr = (JSONArray) object2;

		for (int k = 0; k < jsonArr.size(); k++) {

			if (jsonArr.get(k) instanceof JSONObject) {
				parseJson((JSONObject) jsonArr.get(k), "");
			} else {
				System.out.println(jsonArr.get(k));
			}

		}
	}

	static int count_rows = 0;
	static Connection conn = MyGetConnection.con;
	static PreparedStatement stmt = null;
	static boolean flag = true;
	
	public static void parseJson(JSONObject jsonObject, String temp)
			throws ParseException, SQLException {
		conn.getMetaData();
		
		Set<Object> set = jsonObject.keySet();
		// System.out.println("hey"+set.size());
		HashMap<String, ArrayList<String>> tempTable = new HashMap<String, ArrayList<String>>();
		Iterator<Object> iterator = set.iterator();
		if (flag) {
			String createTableSQL = "Create Table " + temp
					+ "("+temp+"ID INTEGER PRIMARY KEY AUTO_INCREMENT)";
			stmt = conn.prepareStatement(createTableSQL);
			stmt.execute();
			flag = false;
		}
		while (iterator.hasNext()) {
			Object obj = iterator.next();
			if (jsonObject.get(obj) instanceof JSONArray) {
				System.out.println("first" + obj.toString());
				getArray(jsonObject.get(obj));
			} else {
				System.out.println(obj.toString());
				if (jsonObject.get(obj) instanceof JSONObject) {
					flag = true;
					try{
					String addCol = "ALTER TABLE " + temp + " ADD "
							+ obj.toString() + "ID" +" VARCHAR(100)";
					System.out.println("table " + obj.toString());
					stmt = conn.prepareStatement(addCol);
					stmt.execute();
					}catch(Exception e){
						parseJson((JSONObject) jsonObject.get(obj), obj.toString());
					}
					parseJson((JSONObject) jsonObject.get(obj), obj.toString());
					// count_rows--;
				} else {

					String col_name;
					/*if (!temp.equals("")) {
						col_name = temp + "." + obj.toString();
					} else {
						col_name = obj.toString();
					}*/
					col_name = obj.toString();
					
					ArrayList<String> key_data;
					if (tempTable.containsKey(col_name)) {
						
						key_data = tempTable.get(col_name);
					} else {
						key_data = new ArrayList<String>();
						try{
						String addCol = "ALTER TABLE " + temp + " ADD "
								+ col_name +" VARCHAR(100)";
						stmt = conn.prepareStatement(addCol);
						stmt.execute();
						
						}catch(Exception e){
							continue;
						}
					}

					// System.out.println(jsonObject.get(obj).toString() + "" +
					// "here");
					key_data.add(jsonObject.get(obj).toString());
					tempTable.put(col_name, key_data);
					
										// System.out.println(count_rows);
					if (key_data.size() < count_rows) {
						// int temp_count = key_data.size();
						int temp_size = key_data.size();

						for (int i = temp_size; i < count_rows; i++) {
							key_data.add("null");

						}
					}

					// count_rows++;

					// System.out.println(col_name + "\t"
					// + jsonObject.get(obj));

				}
			}

		}
		// count_rows++;

		for (String s : tempTable.keySet()) {
			ArrayList<String> temp_array = tempTable.get(s);
			if (temp_array.size() < count_rows) {
				// int temp_count = key_data.size();
				int temp_size = temp_array.size();

				for (int i = temp_size; i < count_rows; i++) {
					temp_array.add("null");

				}
			}
		}

		for (String s : tempTable.keySet()) {

			System.out.print(s + "\t\t\t");
		}
		System.out.println();
		for (int i = 0; i < tempTable.keySet().size() * 24; i++)
			System.out.print("-");
		System.out.println();
		for (int i = 0; i < count_rows; i++) {
			for (String s : tempTable.keySet()) {
				if (s.equalsIgnoreCase("_id.$oid"))
					System.out.print(tempTable.get(s).get(i) + "\t");
				else
					System.out.print(tempTable.get(s).get(i) + "\t\t\t");

			}
			System.out.println();
		}

	}

	public static MongoPreparedStatement mongopreparedstatement(String query)
			throws Exception {
		MongoPreparedStatement mps = null;
		String array[] = query.split("\\s+");
		if (array[0].equalsIgnoreCase("select")) {
			int from_index = array_search(array, "from");
			from_index++;
			String collection = array[from_index];
			DBCollection coll = db.getCollection(collection);
			// System.out.println(coll.find());
			DBCursor temp = coll.find();
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = null;

			System.out.println();
			while (temp.hasNext()) {
				// System.out.println(temp.next());
				jsonObject = (JSONObject) jsonParser.parse(temp.next()
						.toString());
				// jsonObject = (JSONObject) object;
				// Set<Object> set = jsonObject.keySet();
				// for(Object temp1 : set){
				// System.out.println(temp1);
				//
				// }
				count_rows++;
				parseJson(jsonObject, collection);

			}
			/*
			 * for (String s : table.keySet()) { ArrayList<String> temp_array =
			 * table.get(s); if (temp_array.size() < count_rows) { // int
			 * temp_count = key_data.size(); int temp_size = temp_array.size();
			 * 
			 * for (int i = temp_size; i < count_rows; i++) {
			 * temp_array.add("null");
			 * 
			 * } } }
			 */

		}

		return mps;
	}

	public static void main(String args[]) throws Exception {
		/*
		 * try {
		 * 
		 * // MongoClient mj = new MongoClient("localhost"); MongoClient mj =
		 * new MongoClient("localhost", 27017); DB db = mj.getDB("test_db"); //
		 * MongoCredential credential = //
		 * MongoCredential.createMongoCRCredential(userName, database, //
		 * password); MongoClient mongoClient = new MongoClient(new
		 * ServerAddress()); DBCollection coll = db.getCollection("movie");
		 * 
		 * BasicDBObject doc = new BasicDBObject("name", "MongoDB")
		 * .append("type", "database") .append("count", 1) .append("info", new
		 * BasicDBObject("x", 203).append("y", 102)); coll.insert(doc);
		 * 
		 * } catch (Exception e) { }
		 */

		getConnection("localhost", 27017, "test");

		mongopreparedstatement("select * from mycollection");
		// System.out.println(table);
		/*
		 * for(String s:table.keySet()){
		 * 
		 * System.out.print(s+"\t\t\t"); } System.out.println(); for(int
		 * i=0;i<table.keySet().size()*24;i++) System.out.print("-");
		 * System.out.println(); for(int i = 0;i<count_rows;i++){ for(String
		 * s:table.keySet()){ if(s.equalsIgnoreCase("_id.$oid"))
		 * System.out.print(table.get(s).get(i)+"\t"); else
		 * System.out.print(table.get(s).get(i)+"\t\t\t");
		 * 
		 * } System.out.println(); }
		 */
	}
}
