package latis.util

import org.junit._
import Assert._
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

class TestProperties {
  
  /**
   * Cause the reload of properties before each test.
   */
  @After
  def resetProperties = LatisProperties.reset
  
  @Test
  def precedence_of_test_properties = {
    val dsDir = LatisProperties.getOrElse("dataset.dir", "Not Found")
    assertEquals("datasets/test", dsDir)
  }
  
  @Test
  def build_info_version = {
    val sbtVersion = getSbtVersion
    val version = latis.util.BuildInfo.version
    assertEquals(version, sbtVersion) 
  }
  
  @Test
  def parameterized_value = {
    val s = LatisProperties("test.parameterized.value")
    assertTrue((new File(s)).exists)
  }
  
  
  @Test
  def latis_cofig_property = {
    val file = System.getProperty("user.dir") + "/src/test/resources/" + "latis_test.properties"
    System.setProperty("latis.config", file)
    val name = LatisProperties.getOrElse("name", "Not Found")
    System.clearProperty("latis.config") //don't affect the other tests
    assertEquals("latis_config_test", name)
  }
  
  @Test
  def file_path_with_spaces = {
    //Needed for jenkins job. The workspace is the name of the project.
    //TODO: but doesn't replicate the case where it finds the file via the classpath: getClass.getResource.
    //  that returns a URL.
    val file = System.getProperty("user.dir") + "/src/test/resources/" + "latis with spaces.properties"
    System.setProperty("latis.config", file)
    val name = LatisProperties.getOrElse("name", "Not Found")
    System.clearProperty("latis.config") //don't affect the other tests
    assertEquals("latis_spaces_test", name)
  }
  
  @Test
  def override_with_system_properties_then_undo = {
    assertEquals("datasets/test", LatisProperties("dataset.dir"))
    System.setProperty("dataset.dir", "woozle")
    assertEquals("woozle", LatisProperties("dataset.dir"))
    System.clearProperty("dataset.dir")
    assertEquals("datasets/test", LatisProperties("dataset.dir"))
  }
  
  def getSbtVersion: String = {
    val versionFile: String = new String(Files.readAllBytes(Paths.get("./version.sbt")))
    val version = versionFile.split(":=")(1).replaceAll("\"", "").trim
    version
  }
}

