lazy val latis = (project in file(".")).
  settings(
    name         := "latis",
    scalaVersion := "2.11.8",
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
    )
  )
