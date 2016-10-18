libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.1" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.1" % "provided"
)

resolvers ++= Seq(
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Spray" at "http://repo.spray.cc"
)
