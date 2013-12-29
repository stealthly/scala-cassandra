scala-cassandra
===============

Quick up and running using Scala for Apache Cassandra

Use Vagrant to get up and running.

1) Install Vagrant [http://www.vagrantup.com/](http://www.vagrantup.com/)  
2) Install Virtual Box [https://www.virtualbox.org/](https://www.virtualbox.org/)  

Then once that is done (or if done already)

0) git clone https://github.com/stealthly/scala-cassandra
1) cd scala-cassandra
2) vagrant up  
3) ./sbt test  

Cassandra will be running in the virtual machine on 172.16.7.2 and accesible to your host machine.

Test cases are a good entry point so lets start here https://github.com/stealthly/scala-cassandra/blob/master/src/test/scala/ScalaCassandraSpec.scala

	class ScalaCassandraSpec extends Specification {

	   CQL.init()
	   CQL.startup("MetaStore")
	   Meta.createTable()

	   "Meta objects" should {
	      "be able to store & retrieve their binary state" in {

          //we use a Thrift object here for portability of the data stored
         val tMeta = new TMeta() 

         //setting up some randomness so we can confirm what we are writing is what we get back
         val metaUUID = UUID.randomUUID() 
         val dataum = Random.alphanumeric.take(1000000).mkString
         
         tMeta.setId(metaUUID.toString)
         tMeta.setDatum(dataum)

         val newMetaObj = new MetaTableName(metaUUID, tMeta)
         newMetaObj.save() //Saved to C*

         val savedMetaObj = new MetaTableName(metaUUID)
         val someNewTMeta = savedMetaObj.get()
         someNewTMeta.getId() must_== metaUUID.toString

         someNewTMeta.getDatum() must_== dataum
	      }
	   }
	}

The example implementation is contained in the https://github.com/stealthly/scala-cassandra/blob/master/src/main/scala/MetaTableNameDAO.scala and you should mirror your implementation around this.

	case class MetaTableName(id: UUID, blobMeta: TMeta) extends Table with Instrument {
	  def this() = this(null, null)
	  def this(id: UUID) = this(id, null)

	  tableName = "MetaTableName"
	  tableColumnNames = List("id uuid",blobColumnName + " blob")
	  tablePrimaryKey = "id"

	  def bindIdAndBlob(exec:(List[String])=>BoundStatement) = {
	    val blobBytes: ByteBuffer = blobMeta 
	    exec(List("id", blobColumnName)).bind(id,blobBytes)
	  }

	  //bind just the chainId to some CQL call in the DAL
	  def bindId(exec: (String)=>BoundStatement) = {
	    exec("id").bind(id)
	  }

	  //save the object we have, upsert
	  def save() = {
	    val context = time(MetaTableName,BlobMetric.insert)
	    try {
			  execute(bindIdAndBlob(insertBound))
	    } finally {
	      context.stop()
	    }
		}

	  //just get back the stored serialized object just based on the one primary key
	  def get(): TMeta = {     
	    getBlob(new TMeta(), bindId(getSavedBlob)).asInstanceOf[TMeta]
	    
	  }  
	}

This patern is what you need to-do moving forward.  You need to setup the local binding functions so the Table trait knows nothing about your object but still can bind to your values you need passing in.  You also need to implement the saving and get with a minimal of boilter plate for the access layer.  And that is it.

Big Data Open Source Security LLC
http://www.stealth.ly