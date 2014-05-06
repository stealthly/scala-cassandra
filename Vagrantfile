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
# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.define :sc do |cassandra_config|
    cassandra_config.vm.host_name = "sc"

    cassandra_config.vm.box = "precise64"
    cassandra_config.vm.box_url = "http://files.vagrantup.com/precise64.box"

    cassandra_config.vm.network :private_network, ip: "172.16.7.2"
    
    cassandra_config.vm.provider "virtualbox" do |vb|
      vb.customize ["modifyvm", :id, "--memory", "1048"]
    end

    cassandra_config.vm.provision "shell", path: "vagrant/install_cassandra.sh", :args => "2.0.7"
  end

end
