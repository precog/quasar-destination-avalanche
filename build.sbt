import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-avalanche"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-avalanche"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-avalanche"),
  "scm:git@github.com:precog/quasar-destination-avalanche.git"))

lazy val quasarVersion =
  Def.setting[String](managedVersions.value("precog-quasar"))

lazy val asyncBlobstoreVersion =
  Def.setting[String](managedVersions.value("precog-async-blobstore"))

lazy val quasarPluginJdbcVersion =
  Def.setting[String](managedVersions.value("precog-quasar-plugin-jdbc"))

lazy val specs2Version = "4.9.4"

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
  .aggregate(core, azure, s3)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(name := "quasar-destination-avalanche-core")
  .settings(
    performMavenCentralSync := false,
    publishAsOSSProject := true,
    libraryDependencies ++= Seq(
      "com.precog" %% "async-blobstore-core" % asyncBlobstoreVersion.value,
      "com.precog" %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "io.chrisdavenport" %% "log4cats-slf4j" % "1.0.1",
      "org.specs2" %% "specs2-core" % specs2Version % Test),
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp.filter(_.data.getName != "iijdbc.jar") // exclude everything but iijdbc.jar
    },
    packageBin in Compile := (assembly in Compile).value)

lazy val azure = project
  .in(file("azure"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(name := "quasar-destination-avalanche-azure")
  .settings(
    quasarPluginName := "avalanche-azure",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDestinationFqcn := Some("quasar.destination.avalanche.azure.AvalancheAzureDestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "com.precog" %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "com.precog" %% "async-blobstore-azure" % asyncBlobstoreVersion.value),
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % specs2Version % Test))
  .enablePlugins(QuasarPlugin)

lazy val s3 = project
  .in(file("s3"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(name := "quasar-destination-avalanche-s3")
  .settings(
    quasarPluginName := "avalanche-s3",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDestinationFqcn := Some("quasar.destination.avalanche.s3.AvalancheS3DestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "com.precog" %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "com.precog" %% "async-blobstore-s3" % asyncBlobstoreVersion.value),
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % specs2Version % Test))
  .enablePlugins(QuasarPlugin)
