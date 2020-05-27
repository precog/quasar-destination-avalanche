import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-avalanche"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-avalanche"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-avalanche"),
  "scm:git@github.com:precog/quasar-destination-avalanche.git"))

val DoobieVersion = "0.8.8"

lazy val buildSettings = Seq(
  logBuffered in Test := githubIsWorkflowBuild.value)

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val commonSettings = buildSettings ++ publishTestsSettings

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .settings(commonSettings)
  .aggregate(core, azure)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(name := "quasar-destination-avalanche-core")
  .settings(
    performMavenCentralSync := false,
    publishAsOSSProject := true,
    libraryDependencies ++= Seq(
      "io.argonaut" %% "argonaut" % "6.3.0-M2",
      "org.typelevel" %% "cats-core" % "2.1.0"
    )
  )
  // .settings(
  //   performMavenCentralSync := false,
  //   publishAsOSSProject := true,
  //    quasarPluginName := "avalanche",
  //    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
  //    quasarPluginDestinationFqcn := Some("quasar.destination.avalanche.AvalancheDestinationModule$")
  // )
  // .enablePlugins(QuasarPlugin)

lazy val azure = project
  .in(file("azure"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(name := "quasar-destination-avalanche-azure")
  .settings(
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value

      cp.filter(_.data.getName != "iijdbc.jar") // exclude everything but iijdbc.jar
    },
    quasarPluginName := "avalanche-azure",
    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
    quasarPluginDestinationFqcn := Some("quasar.destination.avalanche.azure.AvalancheDestinationModule$"),
    libraryDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
      "com.precog" %% "async-blobstore-azure" % managedVersions.value("precog-async-blobstore"),
      "com.precog" %% "async-blobstore-core" % managedVersions.value("precog-async-blobstore")),
    excludeDependencies += "org.typelevel" % "scala-library",
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % "4.8.3" % Test),
    packageBin in Compile := (assembly in Compile).value)
  .enablePlugins(QuasarPlugin)
