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

package ly.stealth.testing.cassandra

import org.specs2.mutable._
import scala.collection.JavaConverters._
import com.datastax.driver.core.{Cluster, Row}
import ly.stealth.cassandra._
import ly.stealth.thrift.{Meta => TMeta, Tag => TTag}
import java.util.UUID
import scala.util.Random
import kafka.utils.{Logging => AppLogging}

class ScalaCassandraSpec extends Specification with AppLogging {

   CQL.startup("MetaStore","172.16.7.2")
   val ddl = new MetaTableName()
   ddl.createTable()

   def saveNewMetaObject(metaUUID: UUID, dataum: String) = {
         //we use a Thrift object here for portability of the data stored
         val tMeta = new TMeta() 
         
         tMeta.setId(metaUUID.toString)
         tMeta.setDatum(dataum)

         val newMetaObj = new MetaTableName(metaUUID, tMeta)
         newMetaObj.save() //Saved to C*      
   }

   "Meta objects" should {
      "be able to store their binary state starting from a random state and then retrieving it" in {
         //setting up some randomness so we can confirm what we are writing is what we get back
         val metaUUID = UUID.randomUUID() 
         val dataum = Random.alphanumeric.take(1000000).mkString
         saveNewMetaObject(metaUUID, dataum)

         val savedMetaObj = new MetaTableName(metaUUID)
         val someNewTMeta = savedMetaObj.get()
         someNewTMeta.getId() must_== metaUUID.toString

         someNewTMeta.getDatum() must_== dataum
      }

      "check for multiple values and processing with get all rows" in {
         //setting up some randomness so we can confirm what we are writing is what we get back
         val metaUUID1 = UUID.randomUUID() 
         val dataum1 = Random.alphanumeric.take(1000000).mkString
         saveNewMetaObject(metaUUID1, dataum1)
         
         val metaUUID2 = UUID.randomUUID() 
         val dataum2 = Random.alphanumeric.take(1000000).mkString
         saveNewMetaObject(metaUUID2, dataum2)       

         var found1 = false
         var found2 = false
         def process(row: Row) = {
            def getRowBytes(row: Row) = {
              val dml = new MetaTableName()
              val tMeta = new TMeta() 
              val blob = dml.getRowBytes(row,dml.blobColumnName)
              dml.binaryDeserializer.deserialize(tMeta,blob)
              tMeta
            }

            val savedMetaUUID = row.getUUID("id")

            savedMetaUUID match {
               case `metaUUID1` => {
                  info("processing metaUUID = " + row.getUUID("id"))
                  getRowBytes(row).getDatum() must_== dataum1
                  found1 = true
               }
               case `metaUUID2` => {
                  info("processing metaUUID = " + row.getUUID("id"))
                  getRowBytes(row).getDatum() must_== dataum2
                  found2 = true
               }
               case _ => false must_== true //fail it
            }
         }

         val mtn = new MetaTableName()
         mtn.getRows(process, List(metaUUID1, metaUUID2))

         found1 must_== true
         found2 must_== true
      }
   }
}