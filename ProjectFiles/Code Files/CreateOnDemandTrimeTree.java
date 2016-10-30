import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * 
 */

/**
 * @author Shiva
 * @description This class contains a main method that crates an on-demand time tree for the Movies database
 *
 */
public class CreateOnDemandTrimeTree {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		GraphDatabaseService graphDB = null;
		
		// Path to the database location
		String dbPath = "D:/561/Project/Datasets/OnDemandTimeTree/Month/cineasts_12k_movies_50k_actors.db";
		
		//Create a map to maintain year and its associated months details
		Map<Integer, ArrayList<Integer>> yearMonthMap = new HashMap<Integer, ArrayList<Integer>>(); 
		
		//Connect to the graph database
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(dbPath)).newGraphDatabase();
		
		// Fetch all movies from the Movies database
		String cypherQuery = "match (n:Movie) return distinct n.releaseYear as releaseYear, n.releaseMonth as releaseMonth";
		Result result = graphDB.execute(cypherQuery);
		
		// Update the map with the available years and months
		while(result.hasNext()) {
			Map<String,Object> row = result.next();
			
			if(row.get("releaseYear")!=null) {
				
				Integer year = Integer.valueOf(String.valueOf(row.get("releaseYear")));
				Integer month = Integer.valueOf(String.valueOf(row.get("releaseMonth")));
				
				ArrayList<Integer> monthsList = yearMonthMap.get(year);
				if(monthsList==null) {
					monthsList = new ArrayList<Integer>();					
				}
				if(!monthsList.contains(month)) {
					monthsList.add(month);
				}
				yearMonthMap.put(year, monthsList);
			}
		}
		for(Integer eachYear : yearMonthMap.keySet()) {
			ArrayList<Integer> monthsList = yearMonthMap.get(eachYear);
			Collections.sort(monthsList);
		}
		System.out.println(yearMonthMap);
		
		// create a root node
		String timeQuery = "create (root:Root{root:\"Root\"}) ";
		graphDB.execute(timeQuery);

		//create year, month nodes and their relationships
		for(Integer eachYear : yearMonthMap.keySet()) {
			ArrayList<Integer> monthsList = yearMonthMap.get(eachYear);
			timeQuery = "merge (y:Year{year:"+eachYear+"}) ";
			timeQuery += "merge (root:Root{root:\"Root\"}) merge (y)<-[:HAS_Year]-(root) ";
			
			for(int i=0; i<monthsList.size();i++) {
				timeQuery+="create (m"+i+":Month{month:"+monthsList.get(i)+"}) ";
				timeQuery+="merge (y)-[:HAS_MONTH]->(m"+i+") ";
				if(i==0) {
					timeQuery+="merge (y)-[:FIRST]->(m"+i+") ";					
				}
				if(i==monthsList.size()-1) {
					timeQuery+="merge (y)-[:LAST]->(m"+i+") ";
				}
			}
			System.out.println("Query:"+timeQuery);
			graphDB.execute(timeQuery);
			
		}
		
		//Connect month nodes with "NEXT" relationship
		timeQuery = "MATCH (year:Year)-[:HAS_MONTH]->(month) WITH year,month ORDER BY year.year, month.month WITH collect(month) as months FOREACH(i in RANGE(0, size(months)-2) | FOREACH(month1 in [months[i]] | FOREACH(month2 in [months[i+1]] | CREATE UNIQUE (month1)-[:NEXT]->(month2))))";
		System.out.println("Query:"+timeQuery);
		graphDB.execute(timeQuery);
		
		// Connect all movies to their corresponding month nodes
		String linkMoviesQuery = "MATCH allMovies = (eachMovie:Movie) where eachMovie.releaseMonth is not null foreach(eachMovie in nodes(allMovies)| merge (month:Month{month:eachMovie.releaseMonth})<-[:HAS_MONTH]-(year:Year{year:eachMovie.releaseYear})<-[:HAS_Year]-(root:Root{root:\"Root\"}) create (month)<-[:released]-(eachMovie))";
		graphDB.execute(linkMoviesQuery);
		
		//Shut down the graph database at the end once all queries are executed
		graphDB.shutdown();

	}

}
