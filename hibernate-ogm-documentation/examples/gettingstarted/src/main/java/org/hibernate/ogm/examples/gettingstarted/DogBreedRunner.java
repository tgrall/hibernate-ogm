package org.hibernate.ogm.examples.gettingstarted;

import org.hibernate.ogm.examples.gettingstarted.domain.Cloud;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.transaction.TransactionManager;
import java.lang.reflect.InvocationTargetException;

public class DogBreedRunner {

	private static final String JBOSS_TM_CLASS_NAME = "com.arjuna.ats.jta.TransactionManager";
	private static final Log logger = LoggerFactory.make();

	public static void main(String[] args) {

		TransactionManager tm = getTransactionManager();

		//build the EntityManagerFactory as you would build in in Hibernate Core

		EntityManagerFactory emf = null;

		String id = null;
		int x = 1;
		if (x==1) {
			emf = Persistence.createEntityManagerFactory( "ogm-jpa-tutorial-cb" );
			System.out.println("\n\t===== COUCHBASE ======");
			id = "5c37513c-17a4-4ea6-bb24-a33298b0741c";

		} else {
			emf = Persistence.createEntityManagerFactory( "ogm-jpa-tutorial-mongo" );
			System.out.println("\n\t===== MONGO ======");
			id = "963b2800-5956-49f6-ac9b-351fdcbcbc92";

		}



		try {
			EntityManager em =null;

//			{
//
//			tm.begin();
//			 em = emf.createEntityManager();
//
//			SnowFlake sf = new SnowFlake();
//			sf.setDescription("Snowflake 1");
//
//			em.persist( sf );
//
//			SnowFlake sf2 = new SnowFlake();
//			sf2.setDescription( "Snowflake 2" );
//			em.persist( sf2 );
//
//			Cloud cloud = new Cloud();
//			cloud.setLength(23);
//			cloud.setType("Cumulus");
//			cloud.getProducedSnowFlakes().add(sf);
//			cloud.getProducedSnowFlakes().add(sf2);
//			em.persist( cloud );
//
//			id = cloud.getId();
//
//			em.flush();
//			em.close();
//			tm.commit();
//
//			}



			{
			tm.begin();
			em = emf.createEntityManager();
			System.out.println("=============");


			Cloud c = em.find(Cloud.class, id);

			System.out.println( c.getType() );
			System.out.println( c.getId() );
			System.out.println(  c.getProducedSnowFlakes() );

			em.flush();
			em.close();
			tm.commit();
			}



		}   catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(0);


//		//Persist entities the way you are used to in plain JPA
//		try {
//			tm.begin();
//			logger.infof( "About to store dog and breed" );
//			EntityManager em = emf.createEntityManager();
//			Breed collie = new Breed();
//			collie.setName( "Collie" );
//			em.persist( collie );
//			Dog dina = new Dog();
//			dina.setName( "Dina" );
//			dina.setBreed( collie );
//			em.persist( dina );
//			Long dinaId = dina.getId();
//			em.flush();
//			em.close();
//			tm.commit();
//
//			//Retrieve your entities the way you are used to in plain JPA
//			logger.infof( "About to retrieve dog and breed" );
//			tm.begin();
//			em = emf.createEntityManager();
//			dina = em.find( Dog.class, dinaId );
//			logger.infof( "Found dog %s of breed %s", dina.getName(), dina.getBreed().getName() );
//			logger.infof( "Found Breed %s",  dina.getBreed().getId() +":"+ dina.getBreed().getName() );
//			em.flush();
//			em.close();
//			tm.commit();
//
//			emf.close();
//		} catch ( Exception e ) {
//			e.printStackTrace();
//		}

	}

	public static TransactionManager getTransactionManager() {
		try {
			Class<?> tmClass = DogBreedRunner.class.getClassLoader().loadClass( JBOSS_TM_CLASS_NAME );
			return (TransactionManager) tmClass.getMethod( "transactionManager" ).invoke( null );
		} catch ( ClassNotFoundException e ) {
			e.printStackTrace();
		} catch ( InvocationTargetException e ) {
			e.printStackTrace();
		} catch ( NoSuchMethodException e ) {
			e.printStackTrace();
		} catch ( IllegalAccessException e ) {
			e.printStackTrace();
		}
		return null;
	}
}
