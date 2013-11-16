scala-cassandra
===============

Implementation of a Scala wrapper over the DataStax Java Driver for Cassandra

Test cases are a good entry point so lets start here https://github.com/stealthly/scala-cassandra/blob/master/src/test/scala/ScalaCassandraSpec.scala

	class ScalaCassandraSpec extends Specification {

	   CQL.init()
	   CQL.startup("MetaStore")
	   Meta.createTable()

	   "Meta objects" should {
	      "be able to store their binary state starting from a random state and then retrieving it" in {

	      	 //we use a Thrift object here for portability of the data stored
	         val tMeta = new TMeta() 

	         //setting up some randomness so we can confirm what we are writing is what we get back
	         val metaUUID = UUID.randomUUID() 
	         val dataum = Random.alphanumeric.take(1000000).mkString
	         
	         tMeta.setId(metaUUID.toString)
	         tMeta.setDatum(dataum)

	         Meta.save(tMeta) //Saved to C*

	         val someNewTMeta = Meta(metaUUID)
	         someNewTMeta.getId() must_== metaUUID.toString

	         someNewTMeta.getDatum() must_== dataum
	      }
	   }
	}

The Meta class (contained in the https://github.com/stealthly/scala-cassandra/blob/master/src/main/scala/MetaDAO.scala file) is like a Widget... Whats a Widget? For us lets consider its like "sample code".

When you want to add something different than a widget create a new thrift file in the `thrift/interface` directory

Then in that directory (lets say you created an IDL called Music.thrift) then run `thrift -gen java Music.thrift` which we did for you already and copy the jar outputted from a `mvn package` to the `lib` folder so you don't have to worry about it.  You can even just keep using the Meta implementaiotn and just shove JSON or XML or whatever you want into it; however, it makes more sense to partition the objects some so you can create wider rows with a collection key in your table along with your partition key.  You can also flatten the things by storing a HashMap on the table and retrieving it.

http://stealth.ly