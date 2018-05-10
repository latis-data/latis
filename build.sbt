ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.11.8"

val artifactory = "http://web-artifacts.lasp.colorado.edu/artifactory/"

lazy val latis = (project in file("."))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    name := "latis",
    libraryDependencies ++= Seq(
      "javax.servlet"              %  "servlet-api"     % "2.5",
      "org.scala-lang"             %  "scala-parser-combinators" % "2.11.0-M4",
      "org.scala-lang.modules"     %% "scala-xml"       % "1.0.4",
      "com.typesafe.scala-logging" %% "scala-logging"   % "3.4.0",
      "com.typesafe.play"          %% "play-json"       % "2.3.10",
      "com.lihaoyi"                %% "scalatags"       % "0.6.7",
      "ch.qos.logback"             %  "logback-classic" % "1.1.7",
      "org.apache.commons"         %  "commons-math3"   % "3.5",
      "net.sf.ehcache"             %  "ehcache"         % "2.9.0",
      "org.codehaus.groovy"        %  "groovy-all"      % "2.4.10" % Runtime
    ),
    // Some tests fail unless we set this.
    Test / parallelExecution := false
  )

lazy val commonSettings = compilerFlags ++ Seq(
  Compile / compile / wartremoverWarnings ++= Warts.allBut(
    Wart.Any,         // false positives
    Wart.Nothing,     // false positives
    Wart.Product,     // false positives
    Wart.Serializable // false positives
  ),
  // Test suite dependencies
  libraryDependencies ++= Seq(
    "junit"            % "junit"           % "4.12"      % Test,
    "com.novocode"     % "junit-interface" % "0.11"      % Test,
    "org.apache.derby" % "derby"           % "10.10.1.1" % Test
  ),
  // Resolvers for our Artifactory repos
  resolvers ++= Seq(
    "Artifactory Release" at artifactory + "sbt-release",
    "Artifactory Snapshot" at artifactory + "sbt-snapshot"
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
  ),
  Compile / compile / scalacOptions ++= Seq(
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard"
  )
)

lazy val publishSettings = Seq(
  publishTo := {
    if (isSnapshot.value) {
      Some("snapshots" at artifactory + "sbt-snapshot")
    } else {
      Some("releases" at artifactory + "sbt-release")
    }
  },
  credentials ++= Seq(
    Path.userHome / ".artifactorycredentials"
  ).filter(_.exists).map(Credentials(_)),
  releaseVersionBump := sbtrelease.Version.Bump.Minor
)
