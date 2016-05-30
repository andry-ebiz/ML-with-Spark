name := "ML-with-Spark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.0"

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.4.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.2"

libraryDependencies += "com.quantifind" %% "wisp" % "0.0.4"

resolvers += "clojars" at "https://clojars.org/repo"

resolvers += "conjars" at "http://conjars.org/repo"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"