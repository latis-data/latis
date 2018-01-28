package latis

import org.junit._
import Assert._
import java.nio.file.Files
import java.nio.file.Paths
import latis.util.LatisProperties

class TestVersion {
  
  @Test
  def checkForVersionMatch = {
    val sbtVersion = getSbtVersionWithoutSNAPSHOT
    LatisProperties.get("version") match {
      case Some(propertiesVersion) => assertEquals(sbtVersion, propertiesVersion)
      case _ => fail("The 'version' property could not be found.")
    }
  }
  
  def getSbtVersionWithoutSNAPSHOT: String = {
    val versionFile: String = new String(Files.readAllBytes(Paths.get("./version.sbt")))
    val version = versionFile.split(" := ")(1).replaceAll("\"", "").trim
    val versionNum = version.split("-")(0)
    versionNum
  }
  
  
}