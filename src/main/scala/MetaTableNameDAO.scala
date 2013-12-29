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

case class MetaTableName(id: UUID, blob: ByteBuffer) extends Table with Instrument {
  def this() = this(null, null)
  def this(id: UUID) = this(id, null)

  tableName = "MetaTableName"
  tableColumns = List("id uuid",blobColumnName + " blob")
  tablePrimaryKey = "id"

  def bindIdAndBlob(exec:(List[String])=>BoundStatement) = {
    val blob: ByteBuffer = savedBlob 
    exec(List("id", blobColumnName)).bind(id,blob)
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