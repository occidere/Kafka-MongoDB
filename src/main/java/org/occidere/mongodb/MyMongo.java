package org.occidere.mongodb;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.occidere.common.MyLogger;
import org.occidere.dao.Student;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;

public class MyMongo {
	private MongoClient mongoClient;
	private MongoDatabase mongoDb;
	
	public MyMongo(final String SERVER, final int PORT) {
		this.mongoClient = new MongoClient(SERVER, PORT);
		
		setDatabase("test"); //default db
	}
	
	public void setDatabase(String database) {
		this.mongoDb = mongoClient.getDatabase(database);
	}
	
	
	public void insert(final String collection, final Student student) {
		Document doc = new Document();
		doc.put("name", student.getName());
		doc.put("age", student.getAge());
		doc.put("school", student.getSchool());

		try {
			mongoDb.getCollection(collection).insertOne(doc);
			MyLogger.logger.info("[INSERT] " + student);
		}
		catch(MongoException me) {
			me.printStackTrace();
		}
	}
	
	public List<Student> findStudentByName(final String collection, String name) {
		BasicDBObject query = new BasicDBObject();
		query.put("name", name);
		
		Document doc;
		List<Student> list = new LinkedList<>();
		Iterator<Document> iter = mongoDb.getCollection(collection).find(query).iterator();
		while(iter.hasNext()) {
			doc = iter.next();
			
			Student student = new Student(
					doc.getString("name"),
					doc.getInteger("age"),
					doc.getString("school"));
			
			list.add(student);
		}
		
		return list;
	}
	
	public void dropCollection(final String collection) {
		mongoDb.getCollection(collection).drop();
	}
	
	public void close() {
		mongoClient.close();
	}
}
