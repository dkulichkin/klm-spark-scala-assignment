name := "SparkKLMExample"

version := "0.0.1"

scalaVersion := "2.12.12"
val sparkVersion = "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1" % Compile
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % Test

// test run settings
assembly / test := {}

// Measure time for each test
Test / testOptions += Tests.Argument("-oD")

// don't include Scala in the JAR file
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}
