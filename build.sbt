import ReleaseTransformations._

lazy val latis = (project in file(".")).
  settings(
    name         := "latis",
    releaseIgnoreUntrackedFiles := true,
    scalaVersion := "2.11.8",
    
    // Determines Artifactory repo based on the current git branch
    publishTo := {
      if(git.gitCurrentBranch.value == "master")
        Some("Artifactory Realm" at "http://int-web-03:8081/artifactory/sbt-prod-local")
      else if(git.gitCurrentBranch.value == "dev")
        Some("Artifactory Realm" at "http://int-web-03:8081/artifactory/sbt-dev-local")
      else
        throw sys.error("Trying to deploy from invalid branch")
    },
    
    // Determines the release version based on the current git branch
    releaseVersion := {
      if (git.gitCurrentBranch.value == "master")
        // Current version name without '-SNAPSHOT'
        { ver =>  sbtrelease.Version(ver).map(_.withoutQualifier.string).getOrElse(sbtrelease.versionFormatError) }
      else if (git.gitCurrentBranch.value == "dev")
        // Current version name with '-SNAPSHOT'
        { ver =>  sbtrelease.Version(ver).map(_.asSnapshot.string).getOrElse(sbtrelease.versionFormatError) }
      else
        throw sys.error("Trying to deploy from invalid branch")
    },
    
    // Point to the credentials file (ask product owner for this)
    credentials += Credentials(Path.userHome / ".artifactorycredentials"),
    
    // Define custom sbt-release process (skip version increment, skip create tag, etc.)
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      publishArtifacts
    ),
    
    libraryDependencies ++= Seq(
      "javax.servlet"               % "servlet-api"     % "2.5",
      "org.scala-lang"              % "scala-parser-combinators" % "2.11.0-M4",
      "org.scala-lang.modules"     %% "scala-xml"       % "1.0.4",
      "com.typesafe.scala-logging" %% "scala-logging"   % "3.4.0",
      "com.typesafe.play"          %% "play-json"       % "2.3.10",
      "ch.qos.logback"              % "logback-classic" % "1.1.7",
      "org.apache.commons"          % "commons-math3"   % "3.5",
      "net.sf.ehcache"              % "ehcache"         % "2.9.0",
      "junit"                       % "junit"           % "4.+"       % Test,
      "com.novocode"                % "junit-interface" % "0.11"      % Test,
      "org.apache.derby"            % "derby"           % "10.10.1.1" % Test
    ),
    parallelExecution in Test := false
    webappWebInfClasses := true
  )
  enablePlugins(JettyPlugin, ReleasePlugin, GitPlugin)

