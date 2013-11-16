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
import java.nio.ByteBuffer
import com.codahale.metrics.MetricRegistry
import scala.collection.JavaConverters._
import ly.stealth.thrift.{Meta => TMeta, Tag => TTag}

object Meta extends Table {

	//this is the CQL table name you want to give this object
	tableName = "Meta"
	
	//this is a basic retrieve using the uuid like an index for the object we are storing
	def apply(metaUUID: UUID): TMeta = {
		val context = Metrics.registry.timer(MetricRegistry.name(Meta.getClass, "get")).time()
	    try {
			val query = "SELECT metaBlob from " + Meta.tableName + " where metaUUID = " + metaUUID
			val future = CQL.session.executeAsync(query)

			var metaBlob: Array[Byte] = null

			for (row <- future.getUninterruptibly().asScala) {
				metaBlob = getRowBytes(row,"metaBlob")
			}

			def deserialize(metaBlob: Array[Byte]): TMeta = {
				val context = Metrics.registry.timer(MetricRegistry.name(Meta.getClass, "deserialize")).time()
				try {
					val binaryDeserializer = new TDeserializer(new TBinaryProtocol.Factory())
					val sMeta = new TMeta();

			        binaryDeserializer.deserialize(sMeta,metaBlob)
			        sMeta
			    } finally {
			    	context.stop()
			    }
			}
			
			deserialize(metaBlob)
		} finally {
		      context.stop()
		}
	}

	def save(tMeta: TMeta) = {
		val context = Metrics.registry.timer(MetricRegistry.name(Meta.getClass, "save")).time()
		try {
			//use the propertyUUID that is on the object as the object's index
			val metaUUID = UUID.fromString(tMeta.getId())
	        
	        val metaDAO = Meta(metaUUID,tMeta)

	        metaDAO.save()
        } finally {
        	context.stop()
        }
	}

	def createTable() = {
		CQL.session.execute(Meta(null,null).table())
	}
}

case class Meta(metaUUID: UUID, metaBlob: ByteBuffer) extends Table  {

	def table() = {
		CQL.table(Meta.tableName,
				List("metaUUID uuid","metaBlob blob"),
				 "metaUUID")
	}

	def insert() = {
		CQL.insert(Meta.tableName,
					List("metaUUID", "metaBlob"))
	}	

	def save() = {
		val boundStatement = new BoundStatement(CQL.session.prepare(insert()))
		CQL.session.execute(boundStatement.bind(metaUUID, metaBlob))
	}
}