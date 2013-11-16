name := "scala-cassandra"

version := "0.1.0.0"

scalaVersion := "2.10.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Apache Staging" at "https://repository.apache.org/content/groups/staging/"

libraryDependencies ++= Seq(
	"org.specs2" %% "specs2" % "2.2.2" % "test",
	"com.datastax.cassandra" % "cassandra-driver-core" % "1.0.3",
	"org.apache.kafka" % "kafka_2.10" % "0.8.0",
	"com.codahale.metrics" % "metrics-core" % "3.0.1" % "compile",
	"nl.grons" %% "metrics-scala" % "3.0.3",
	"org.apache.thrift" % "libthrift" % "0.9.1"
)
