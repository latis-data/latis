package latis.util

import org.junit._
import Assert._

class UtilTest {

  @Test
  def resolve_string_with_system_property {
    val s = Util.resolveParameterizedString("Hi ${user.name}!")
    val a = "Hi " + System.getProperty("user.name") + "!"
    assertEquals(a, s)
  }
  
  @Test
  def resolve_string_with_environment_variable {
    val s = Util.resolveParameterizedString("Hi ${USER}!")
    val a = "Hi " + System.getenv("USER") + "!"
    assertEquals(a, s)
  }
  
  @Test
  def try_to_resolve_string_without_valid_property {
    //no changes made
    val s = Util.resolveParameterizedString("Hi ${z1x2c3v4}!")
    val a = "Hi ${z1x2c3v4}!" 
    assertEquals(a, s)
  }
  
  //TODO: test from properties file
  
  //----//
  
  //TODO: test data methods
}