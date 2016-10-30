import java.io.*;
import java.util.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.*;

/**
 * 
 * @author Shiva
 * @description This class has main methods that creates releaseMonth and releaseYear properties on Movie nodes
 */
public class UpdateTimeFields {

	public static void main(String[] args) {
		
		GraphDatabaseService graphDB = null;
		
		// Path to the database location
		String dbPath = "D:/561/Project/Datasets/Original/cineasts_12k_movies_50k_actors_2.1.6/cineasts_12k_movies_50k_actors.db";
		
		//Connect to the graph database
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(dbPath)).newGraphDatabase();
		
		// Fetch all movies from the Movies database
		String cypherQuery = "match (n:Movie) return ID(n) as nodeId, n.releaseDate as releaseDate";
		String updateQuery = "";
		
		Calendar calendar = Calendar.getInstance();
		
		Result result = graphDB.execute(cypherQuery);
		int i=0;
		while(result.hasNext()) {
			Map<String,Object> row = result.next();
			System.out.println(row);
			
			if(row.get("releaseDate")!=null) {
				// Get the calendar instance corresponding to the milliseconds from the releaseDate
				calendar.setTimeInMillis(Long.parseLong((String)row.get("releaseDate")));
	
				int year = calendar.get(Calendar.YEAR); // Get the year
				int month = calendar.get(Calendar.MONTH); // Get the month
				int day = calendar.get(Calendar.DAY_OF_MONTH); // Get the day
	
				// Query to create the attributes releaseYear and releaseMonth on the Movie nodes
				updateQuery = "match (n:Movie) where ID(n)="+row.get("nodeId")+" set n.releaseYear="+year+", n.releaseMonth="+ (month+1) +", n.releaseDay="+day+" return n";
				//System.out.println(updateQuery);
				
				// Execute the query
				graphDB.execute(updateQuery);
			}
			
			i++;
		}
		System.out.println("Total rows:"+i);
	
		//Shut down the graph database at the end once all queries are executed
		graphDB.shutdown();
	}

}
