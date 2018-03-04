package org.occidere.kafka_mongodb;

import java.util.List;

import org.occidere.dao.Student;
import org.occidere.kafka.MyConsumer;
import org.occidere.kafka.MyProducer;
import org.occidere.mongodb.MyMongo;

public class Main {
	public static void main(String[] args) throws Exception {
		
		/* MongoDB Connection */
		MyMongo mongo = new MyMongo("ncloud-centos-sj9401h", 27017);
		mongo.setDatabase("mydb");
		mongo.dropCollection("myCollection");
		
		mongo.insert("myCollection", new Student("LSJ", 26, "KookminUniversity"));
		mongo.insert("myCollection", new Student("LSJ", 20, "KoreaUniversity"));
		mongo.insert("myCollection", new Student("David", 25, "SoongsilUniversity"));
		mongo.insert("myCollection", new Student("David", 25, "SejongUniversity"));
		List<Student> list = mongo.findStudentByName("myCollection", "LSJ");
		mongo.close();
		
		System.out.println("== DB ==");
		list.forEach(System.out::println);
		
		/* MongoDB -> Kafka */
		System.out.println("== Producer ==");
		MyProducer producer = new MyProducer("ncloud-centos-occidere000:9092");
		list.forEach(student -> producer.send("test", student.toString()));
		producer.close();

		/* Kafka Result */
		System.out.println("== Consumer ==");
		MyConsumer consumer = new MyConsumer("ncloud-centos-occidere000:9092");
		consumer.run("test");
		consumer.close();
	}
}