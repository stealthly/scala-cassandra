# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash -x
apt-get -y update
apt-get install -y software-properties-common python-software-properties
add-apt-repository -y ppa:webupd8team/java
apt-get -y update
/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java7-installer oracle-java7-set-default

EXPECTED_ARGS=1
E_BADARGS=65

if [ $# -ne $EXPECTED_ARGS ]
then
  echo "What is the Cassandra version number? e.g. 2.0.4"
  exit $E_BADARGS
fi

cassandra_version=$1
cassandra_archive="apache-cassandra-$cassandra_version-bin.tar.gz"

setupDirectory() {
  mkdir -p $1
  chown vagrant:vagrant $1
  chmod 755 $1
}

setupDirectory /opt/apache
setupDirectory /var/lib/cassandra
setupDirectory /var/log/cassandra
setupDirectory /var/lib/cassandra/saved_caches

cd /tmp
#we want to use this and NOT archive so we can keep up with releases
#it might make sense to paramaterize this so you can use the version that is in produciton and flip it
#for when you release new versions to regress against in your test flow
wget http://www.apache.org/dist/cassandra/$cassandra_version/$cassandra_archive
cd /opt/apache
tar xfv /tmp/${cassandra_archive}
cp /vagrant/vagrant/cassandra.yaml /opt/apache/apache-cassandra-$cassandra_version/conf

#starter up!
/opt/apache/apache-cassandra-${cassandra_version}/bin/cassandra


