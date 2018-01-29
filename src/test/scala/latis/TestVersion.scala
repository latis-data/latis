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
    getMainPropertiesVersion match {
      case Some(propertiesVersion) => assertEquals(sbtVersion, propertiesVersion)
      case _ => fail("The 'version' property could not be found.")
    }
  }
  
  def getSbtVersionWithoutSNAPSHOT: String = {
    val versionFile: String = new String(Files.readAllBytes(Paths.get("./version.sbt")))
    val version = versionFile.split(":=")(1).replaceAll("\"", "").trim
    val versionNum = version.split("-")(0)
    versionNum
  }
  
  /*
   * Used to pull the "version" property from the main latis.properties (not test).
   */
  def getMainPropertiesVersion: Option[String] = {
    val propertiesFile: String = new String(Files.readAllBytes(Paths.get("./src/main/resources/latis.properties")))
    propertiesFile contains "version" match {
      case true => {
        val versionIndex = propertiesFile.indexOf("version")
        val version = propertiesFile.substring(versionIndex, propertiesFile.indexOf("\n", versionIndex))
        val versionNum = version.split("=")(1).trim
        Some(versionNum)
      }
      case false => None
    }
  }
  
}