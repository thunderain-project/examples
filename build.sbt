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

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.8.0-incubating"

resolvers ++= Seq(
   "Maven Repository" at "http://repo1.maven.org/maven2",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Spray Repository" at "http://repo.spray.cc/"
)
