ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.12.17"

val nexus = "https://artifacts.pdmz.lasp.colorado.edu/repository/"

lazy val latis = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    name := "latis",
    libraryDependencies ++= Seq(
      "javax.servlet"              %  "javax.servlet-api"        % "3.1.0" % "provided",
      "org.scala-lang.modules"     %% "scala-parser-combinators" % "1.1.2",
      "org.scala-lang.modules"     %% "scala-xml"       % "1.3.0",
      "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.2",
      "com.typesafe.play"          %% "play-json"       % "2.9.2",
      "com.lihaoyi"                %% "scalatags"       % "0.9.3",
      "ch.qos.logback"             %  "logback-classic" % "1.2.3",
      "org.apache.commons"         %  "commons-math3"   % "3.6.1",
      "commons-net"                %  "commons-net"     % "3.7.2",
      "net.sf.ehcache"             %  "ehcache"         % "2.10.6",
      "com.lihaoyi"                %% "requests"        % "0.6.5",
      "org.codehaus.groovy"        %  "groovy-all"      % "3.0.7" % Runtime
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
    "com.github.sbt"   % "junit-interface" % "0.13.2"    % Test,
    "org.apache.derby" % "derby"           % "10.10.1.1" % Test
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
