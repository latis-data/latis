ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.12.18"

val nexus = "https://artifacts.pdmz.lasp.colorado.edu/repository/"

lazy val latis = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    name := "latis",
    libraryDependencies ++= Seq(
      "javax.servlet"              %  "javax.servlet-api"        % "4.0.1" % "provided",
      "org.scala-lang.modules"     %% "scala-parser-combinators" % "2.2.0",
      "org.scala-lang.modules"     %% "scala-xml"       % "1.3.1",
      "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.5",
      "com.typesafe.play"          %% "play-json"       % "2.9.4",
      "com.lihaoyi"                %% "scalatags"       % "0.12.0",
      "ch.qos.logback"             %  "logback-classic" % "1.2.8", //updating this will break groovy log config?
      "org.apache.commons"         %  "commons-math3"   % "3.6.1",
      "commons-net"                %  "commons-net"     % "3.10.0",
      "net.sf.ehcache"             %  "ehcache"         % "2.10.9.2",
      "com.lihaoyi"                %% "requests"        % "0.8.0",
      "org.codehaus.groovy"        %  "groovy-all"      % "3.0.16" % Runtime
    ),
    // Some tests fail unless we set this.
    Test / parallelExecution := false,
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "latis.util"
  )

lazy val bench = project
  .dependsOn(latis)
  .enablePlugins(JmhPlugin)
  .settings(compilerFlags)

lazy val commonSettings = compilerFlags ++ Seq(
  // Test suite dependencies
  libraryDependencies ++= Seq(
    "junit"            % "junit"           % "4.13.2"    % Test,
    "com.github.sbt"   % "junit-interface" % "0.13.3"    % Test,
    "org.apache.derby" % "derby"           % "10.10.2.0" % Test
  ),
  // Resolvers for our Nexus repos
  resolvers ++= Seq(
    "Nexus Release" at nexus + "web-releases",
    "Nexus Snapshot" at nexus + "web-snapshots"
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
    "-release", "8",
    "-unchecked",
    "-Xfuture",
    "-Xlint:-unused,_",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused",
    "-Ywarn-value-discard"
  ),
  Compile / console / scalacOptions --= Seq(
    "-Ywarn-unused"
  )
)

lazy val publishSettings = Seq(
  publishTo := {
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "web-snapshots")
    } else {
      Some("releases" at nexus + "web-releases")
    }
  },
  credentials ++= Seq(
    Path.userHome / ".sbt" / ".credentials"
  ).filter(_.exists).map(Credentials(_)),
  releaseVersionBump := sbtrelease.Version.Bump.Minor,
  updateOptions := updateOptions.value.withGigahorse(false)
)
