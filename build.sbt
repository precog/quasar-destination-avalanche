import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-avalanche"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-avalanche"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-avalanche"),
  "scm:git@github.com:precog/quasar-destination-avalanche.git"))

//val DoobieVersion = "0.8.8"
val DoobieVersion = "0.9.0"

lazy val buildSettings = Seq(
  logBuffered in Test := githubIsWorkflowBuild.value
  // libraryDependencies ++= Seq(
  //   "org.slf4s" %% "slf4s-api" % "1.7.25",
  //   "org.tpolecat" %% "doobie-core" % DoobieVersion,
  //   "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
  //   "io.argonaut" %% "argonaut" % "6.3.0-M2",
  //   "org.typelevel" %% "cats-core" % "2.1.0",
  //   "com.precog" %% "async-blobstore-core" % managedVersions.value("precog-async-blobstore"),
  //   "org.specs2" %% "specs2-core" % "4.8.3" % Test
  // )
  )

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

lazy val commonSettings = buildSettings ++ publishTestsSettings ++ assemblySettings

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .settings(commonSettings)
  .aggregate(core, azure, s3)
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
     "org.typelevel" %% "cats-core" % "2.1.0", 
     "org.specs2" %% "specs2-core" % "4.8.3" % Test)
  )

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
    quasarPluginDependencies ++= Seq(
      "io.argonaut" %% "argonaut" % "6.3.0-M2",
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "org.typelevel" %% "cats-core" % "2.1.0",
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
      "com.precog" %% "async-blobstore-azure" % managedVersions.value("precog-async-blobstore"),
      "com.precog" %% "async-blobstore-core" % managedVersions.value("precog-async-blobstore")),
    excludeDependencies += "org.typelevel" % "scala-library",
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % "4.8.3" % Test),
    packageBin in Compile := (assembly in Compile).value)
  .enablePlugins(QuasarPlugin)


lazy val s3 = project
  .in(file("s3"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(name := "quasar-destination-avalanche-s3")
  .settings(
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp.filter(_.data.getName != "iijdbc.jar") // exclude everything but iijdbc.jar
    },
    quasarPluginName := "avalanche-s3",
    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
    quasarPluginDestinationFqcn := Some("quasar.destination.avalanche.s3.AvalancheS3DestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "io.argonaut" %% "argonaut" % "6.3.0-M2",
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "org.typelevel" %% "cats-core" % "2.1.0",
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
      "com.precog" %% "async-blobstore-s3" % managedVersions.value("precog-async-blobstore"),
      "com.precog" %% "async-blobstore-core" % managedVersions.value("precog-async-blobstore")),
    excludeDependencies += "org.typelevel" % "scala-library",
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % "4.8.3" % Test),
    packageBin in Compile := (assembly in Compile).value)
  .enablePlugins(QuasarPlugin)