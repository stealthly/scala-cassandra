/*
   Copyright 2013 Big Data Open Source Security LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ly.stealth.cassandra

import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TIOStreamTransport
import org.apache.thrift.{TSerializer, TDeserializer, TBase}
import com.datastax.driver.core.{Cluster, Session, BoundStatement, Row}
import com.datastax.driver.core.exceptions.InvalidQueryException
import kafka.utils.{Logging => AppLogging}
import java.nio.ByteBuffer
import com.codahale.metrics.MetricRegistry
import scala.collection.JavaConverters._
import ly.stealth.thrift.{Meta => TMeta, Tag => TTag}
import java.util.concurrent.atomic._

object Metrics {

  lazy val registry: MetricRegistry = new MetricRegistry()
}

trait Instrument {
   def time[T <: AnyRef](x : T, s: String) = {
    Metrics.registry.timer(MetricRegistry.name(x.getClass.asInstanceOf[Class[T]], s)).time()
  }  
}

object CQL extends AppLogging {
  private val clusterCreated = new AtomicBoolean(false)

  private var cluster: Cluster = null
	var session: Session = null

	def init(hosts: String): Unit = {
	  info("init for Cassandra hosts = %s".format(hosts))
  	if (!clusterCreated.getAndSet(true)) { //only do this once
          cluster = Cluster.builder()
              .addContactPoints(hosts)
              .build()
    }
  }

    def startup(ks: String, hosts: String = "localhost") = { 
    	info("** starting cassandra ring connection for keyspace = %s".format(ks))
    	init(hosts) 
    	
    	try {
          session = cluster.connect(ks) 
        }
        catch {
          case iqe: InvalidQueryException => { //the keyspace probably doesn't exist
            session = cluster.connect()
            createKeyspace(ks)
            session = cluster.connect(ks)
          }
          case _: Throwable => System.exit(-1) // danger will robinson, danger!
        }
    }

  	def shutdown() = cluster.shutdown()

    def createKeyspace(keyspace: String, withOptions: String = "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}") = {
      session.execute("CREATE KEYSPACE IF NOT EXISTS %s %s".format(keyspace,withOptions));
    }

  	def table(tableName: String, columns: List[String], primaryKey: String) = {
  		"CREATE TABLE IF NOT EXISTS %s \n(%s, \nPRIMARY KEY (%s));".format(tableName,columns.mkString(",\n"),primaryKey)
  	}

  	def insert(tableName: String, columns: List[String]) = {
  		"INSERT INTO  %s (%s) VALUES (%s);".format(tableName,columns.mkString(","),(columns.map(c=>"?").mkString(",")))
  	}    	
}


trait Table extends AppLogging{

  var tableName = ""
  var tableColumnNames = List("")
  var tablePrimaryKey = ""
  
  object BlobMetric {
    val insert = "object-inserted"
    val update = "object-updated"
    val select = "object-selected"
  }

  val blobColumnName = "objectStored" // column name for blob objects representing the object the table is flattened for

	//They take a thrift object and return it in byte[] or ByteBuffer format
	implicit def bytesToByteBuffer(bytes: Array[Byte]): ByteBuffer = {
		ByteBuffer.wrap(bytes)
	}	

	implicit def thriftAsBytes(t: TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]): ByteBuffer = {
		val binarySerializer   = new TSerializer(new TBinaryProtocol.Factory())
        val bytes: Array[Byte] = binarySerializer.serialize(t)
        bytes
	}		
	
	//default settings for async future query
	def defaultFutureNum() = 1
	def defaultFutureUnit() = TimeUnit.SECONDS
	
	//return the bytes stored within a cassandra row column
	import com.datastax.driver.core.Row
	def getRowBytes(row: Row, name: String) = {
		var bytes: Array[Byte] = null

		if (row.getBytes(name) != null) {
			val bf = row.getBytes(name)
			bytes = new Array[Byte](bf.remaining())
			bf.get(bytes)		
		}

		bytes
	}

  def createTable() = {
    CQL.session.execute(table())
  }

  def table() = {
    CQL.table(tableName, tableColumnNames, tablePrimaryKey)
  }  

  //generic query creation for an insert 
  def insert(columns: List[String]) = {
    "INSERT INTO %s (%s) VALUES (%s);".format(tableName,columns.mkString(","),(columns.map(c=>"?").mkString(",")))
  }

  //most often we are going to be getting from single columne
  def insertBound(condition: String): BoundStatement = {
    insertBound(List(condition))
  }

  //bind an insert to a list of columns
  def insertBound(columns: List[String]) = {
    debug(insert(columns))
    val boundStatement = new BoundStatement(CQL.session.prepare(insert(columns)))
    boundStatement
  }

  //where we actually write to cassandra
  def execute(boundStatement: BoundStatement) = {
    CQL.session.execute(boundStatement)
  }

  //dynamicall create the "WHERE" condition of the CQL statement from a map of column/value conditions

  def where(conditions: Map[String,String], in: List[String] = List.empty) = {
    val c = (conditions.map{case (key, value) => key + " = " + value}).mkString(" AND ")
    val i = (in.map{i => i + " IN ?"}).mkString(" AND ")
    " WHERE " + List(c,i).filter(s => s.size > 0).mkString(" AND ")
  }  

  //////////////////////////////////////////////////////////////////////////////
  //helper functions for just serializing objects to the cassandra ring
  //this way no object relational mapping
  def getSavedBlob(condition: String) = {
    get(List(blobColumnName), condition)
  }

  def getSavedBlobWithList(condition: List[String]) = {
    getWithList(List(blobColumnName), condition)
  }  

  def getSavedBlobWithMap(condition: List[String]) = {
    getWithList(List(blobColumnName), condition)
  }    

  def getAllColumnsIn(in: List[String]) = {
    getWithList(List("*"), List.empty[String], in)
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //very generic helper function to better pull out and use the overall underlying implementation
  def get(columns: List[String], condition: String): BoundStatement = {
    getWithList(columns, List(condition))
  }

  //sometimes we are goin gto get it from many columns as the key
  def getWithList(columns: List[String], conditions: List[String], in: List[String] = List.empty): BoundStatement = {
    getWithMap(columns, conditions.map(c => c -> "?").toMap, in) //we want to build up the blanks for the bound satement
  }

  //some magic happening here to obfuscate the CQL away from the code so it can do its thing
  def getWithMap(columns: List[String], conditions: Map[String,String], in: List[String] = List.empty): BoundStatement = { 
    val query = "SELECT " + columns.mkString(", ") + " FROM " + tableName + where(conditions, in)
    debug(query)
    val boundStatement = new BoundStatement(CQL.session.prepare(query)) //prepare the bound statement query
    boundStatement
  }

  val binaryDeserializer = new TDeserializer(new TBinaryProtocol.Factory())

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //this gets the blob stored that is 
  def getBlob(saved: TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum], boundStatement: BoundStatement) = {
    val future = CQL.session.executeAsync(boundStatement)
    val result = future.get(defaultFutureNum, defaultFutureUnit)

    var blob: Array[Byte] = null
    for (row <- future.getUninterruptibly().asScala) {
      blob = getRowBytes(row,blobColumnName)
    }
    binaryDeserializer.deserialize(saved,blob)
    saved
  }  

  def processRows(process: (Row)=> Unit, boundStatement: BoundStatement) = {
    val future = CQL.session.executeAsync(boundStatement)
    val result = future.get(defaultFutureNum, defaultFutureUnit)

    for (row <- future.getUninterruptibly().asScala) {
      process(row)
    }    
  }
}