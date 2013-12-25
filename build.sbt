import AssemblyKeys._

name := "simrank"

version := "0.1.0"

scalaVersion := "2.9.3"

retrieveManaged := true

assemblySettings

unmanagedJars in Compile <++= baseDirectory map { base =>
  val baseDirectories = (base / "lib")
  val customJars = (baseDirectories ** "*.jar")
  customJars.classpath
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating-SNAPSHOT"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.1.2"

libraryDependencies += "com.googlecode.matrix-toolkits-java" % "mtj" % "1.0.1"

resolvers ++= Seq(
   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
   "Maven Repository" at "http://repo1.maven.org/maven2",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Spray Repository" at "http://repo.spray.cc/"
)

mergeStrategy in assembly := {
   case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
   case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
   case "META-INF/services/org.apache.hadoop.fs.FileSystem" => MergeStrategy.concat
   case "reference.conf" => MergeStrategy.concat
   case _ => MergeStrategy.first
}

