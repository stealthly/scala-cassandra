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
import com.datastax.driver.core.{Cluster, Session, BoundStatement}
import com.datastax.driver.core.exceptions.InvalidQueryException
import kafka.utils.{Logging => SysLogging}
import java.nio.ByteBuffer
import com.codahale.metrics.MetricRegistry
import scala.collection.JavaConverters._
import ly.stealth.thrift.{Meta => TMeta, Tag => TTag}

object Metrics {

  lazy val registry: MetricRegistry = new MetricRegistry()
}

object CQL extends SysLogging {
	private var cluster: Cluster = null
	var session: Session = null

	def init(): Unit = init("localhost")

	def init(hosts: String): Unit = {
		cluster = Cluster.builder()
              .addContactPoints(hosts)
              .build()

    }

    def startup(ks: String) = { 
    	info("** starting cassandra ring connection for keyspace = %s".format(ks))
    	init() 
    	
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


trait Table {

	var tableName = ""

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
}