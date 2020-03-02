import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-avalanche"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-avalanche"),
  "scm:git@github.com:slamdata/quasar-destination-avalanche.git"))


lazy val QuasarVersion = IO.read(file("./quasar-version")).trim
val DoobieVersion = "0.8.8"
val AsyncBlobstoreVersion = "2.0.0"

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-avalanche")
  .settings(
    performMavenCentralSync := false,
    publishAsOSSProject := true,
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value

      cp.filter(_.data.getName != "iijdbc.jar") // exclude everything but iijdbc.jar
    },
    quasarPluginName := "avalanche",
    quasarPluginQuasarVersion := QuasarVersion,
    quasarPluginDestinationFqcn := Some("quasar.destination.avalanche.AvalancheDestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
      "com.slamdata" %% "async-blobstore-azure" % AsyncBlobstoreVersion,
      "com.slamdata" %% "async-blobstore-core" % AsyncBlobstoreVersion),
    excludeDependencies += "org.typelevel" % "scala-library",
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % "4.9.2" % Test),
    packageBin in Compile := (assembly in Compile).value)
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
