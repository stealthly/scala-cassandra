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
import com.datastax.driver.core.Cluster
import ly.stealth.cassandra._
import ly.stealth.thrift.{Meta => TMeta, Tag => TTag}
import java.util.UUID
import scala.util.Random

class ScalaCassandraSpec extends Specification {

   CQL.startup("MetaStore","172.16.7.2")
   val ddl = new MetaTableName()
   ddl.createTable()

   "Meta objects" should {
      "be able to store their binary state starting from a random state and then retrieving it" in {

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