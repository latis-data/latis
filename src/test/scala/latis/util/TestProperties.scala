package latis.util

import org.junit._
import Assert._
import java.io.File

class TestProperties {
  
  /**
   * Cause the reload of properties before each test.
   */
  @After
  def resetProperties = LatisProperties.reset
  
  
  @Test
  def precedence_of_test_properties {
    val version = LatisProperties.getOrElse("version", "Not Found")
    assertEquals("test", version)
  }
  
  @Test
  def parameterized_value {
    val s = LatisProperties("test.parameterized.value")
    assertTrue((new File(s)).exists)
  }
  
  
  @Test
  def latis_cofig_property {
    val file = System.getProperty("user.dir") + "/src/test/resources/" + "latis_test.properties"
    System.setProperty("latis.config", file)
    val version = LatisProperties.getOrElse("version", "Not Found")
    System.clearProperty("latis.config") //don't affect the other tests
    assertEquals("latis_config_test", version)
  }
  
  @Test
  def file_path_with_spaces {
    //Needed for jenkins job. The workspace is the name of the project.
    //TODO: but doesn't replicate the case where it finds the file via the classpath: getClass.getResource.
    //  that returns a URL.
    val file = System.getProperty("user.dir") + "/src/test/resources/" + "latis with spaces.properties"
    System.setProperty("latis.config", file)
    val version = LatisProperties.getOrElse("version", "Not Found")
    System.clearProperty("latis.config") //don't affect the other tests
    assertEquals("latis_spaces_test", version)
  }
}

