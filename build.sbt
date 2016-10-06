lazy val root = (project in file(".")).
  settings(
    name := "spark-tasks",
    version := "0.1",
    scalaVersion := "2.11.7",

    jarName in assembly := "spark-uservisits.jar",
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),

    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",

      "org.apache.spark" %% "spark-core" % "2.0.0",

      "org.apache.spark" %% "spark-sql" % "2.0.0",

      "eu.bitwalker" % "UserAgentUtils" % "1.20",

      "com.h2database" % "h2" % "1.4.192",

      "org.springframework" % "spring-jdbc" % "4.2.6.RELEASE"

      , "org.apache.spark" %% "spark-mllib" % "2.0.0"

      , "com.databricks" %% "spark-csv" % "1.2.0"

      , "org.apache.kafka" %% "kafka" % "0.8.2.0"
        exclude("jmxri", "com.sun.jmx")
        exclude("jmxtools", "com.sun.jdmk")
        exclude("jms", "javax.jms")
        exclude("commons-beanutils", "commons-beanutils-core")

       , "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0"

),

EclipseKeys.withSource := true
)

net.virtualvoid.sbt.graph.Plugin.graphSettings
