libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.spark" % "spark-core_2.10" % "1.4.0",
  "org.apache.spark" % "spark-sql_2.10" % "1.4.0"
)

resolvers ++= Seq(
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Spray" at "http://repo.spray.cc"
)
