package latis.util

import org.junit._
import Assert._

class TestProperties {
  
  @Test
  def precedence_of_test_properties {
    val version = LatisProperties.getOrElse("version", "Not Found")
    assertEquals("test", version)
  }
  
  @Test
  def resolve_string_with_system_property {
    val s = StringUtils.resolveParameterizedString("Hi ${user.name}!")
    val a = "Hi " + System.getProperty("user.name") + "!"
    assertEquals(a, s)
  }
  
  @Test
  def resolve_string_with_environment_variable {
    val s = StringUtils.resolveParameterizedString("Hi ${USER}!")
    val a = "Hi " + System.getenv("USER") + "!"
    assertEquals(a, s)
  }
  
  @Test
  def try_to_resolve_string_without_valid_property {
    //no changes made
    val s = StringUtils.resolveParameterizedString("Hi ${z1x2c3v4}!")
    val a = "Hi ${z1x2c3v4}!" 
    assertEquals(a, s)
  }
    
  @Test
  def resolve_string_with_latis_property {
    val s = StringUtils.resolveParameterizedString("Hi ${test.name}!")
    val a = "Hi George!"
    assertEquals(a, s)
  }
  
  @Test 
  def resolve_string_with_latis_property_with_space {
    val s = StringUtils.resolveParameterizedString("Hi ${test.name.with.space}!")
    val a = "Hi George !"
    assertEquals(a, s)
  }
  
  @Test
  def resolve_string_with_latis_property_without_new_line {
    val s = StringUtils.resolveParameterizedString("Hi ${test.name.without.nl}!")
    val a = "Hi George!"
    assertEquals(a, s)
  }
  
}