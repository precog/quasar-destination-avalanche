resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")
addSbtPlugin("com.slamdata" % "sbt-slamdata"  % "4.0.1")
addSbtPlugin("com.slamdata" % "sbt-quasar-plugin" % "0.2.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
