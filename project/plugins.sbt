resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayIvyRepo("slamdata-inc", "sbt-plugins")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.slamdata" % "sbt-slamdata" % "5.4.0-45f3e27")
addSbtPlugin("com.slamdata" % "sbt-quasar-plugin" % "0.2.3")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
