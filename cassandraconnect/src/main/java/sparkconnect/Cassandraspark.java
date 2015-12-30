package sparkconnect;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;

import scala.annotation.serializable;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class Cassandraspark {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// if (args.length != 2) {
		// System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark
		// Master URL> <Cassandra contact point>");
		// System.exit(1);
		// }
		SparkConf conf = new SparkConf().setAppName("Cassandra spark").setMaster("spark://quickstart.cloudera:7077");
		// .set("spark.cassandra.connection.host", "127.0.0.1");
		// conf.set("spark.cassandra.connection.host",args[1]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		try (Session session = connector.openSession()) //connecting to cluster and creating tables
		{
			session.execute("DROP KEYSPACE IF EXISTS Indrajit");
			session.execute("CREATE KEYSPACE Indrajit WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE Indrajit.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
			session.execute("CREATE TABLE Indrajit.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
			session.execute("CREATE TABLE Indrajit.summaries (product INT PRIMARY KEY, summary DECIMAL)");

		}
		//select statement
		JavaRDD<CassandraRow> prodtcs=javaFunctions(sc).cassandraTable("Indrajit","Product").select("coulemn1","col-3").where("col5=");
		      
		
		sc.stop();
		sc.close();

	}

}

