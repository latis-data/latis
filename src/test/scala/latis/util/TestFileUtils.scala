package latis.util

import org.junit._
import Assert._
import java.io.File

class TestFileUtils {
  
  @Test
  def delete_nested_directory_with_files {
    val dirName = System.getProperty("java.io.tmpdir") + File.separator + "latis_file_util_test"
    val dir = new File(dirName)
    dir.mkdir
    val d1 = new File(dir, "d1")
    d1.mkdir
    val d2 = new File(dir, "d1")
    d1.mkdir
    
    (new File(dir, "a")).createNewFile
    (new File(d1, "b")).createNewFile
    (new File(d2, "c")).createNewFile
    
    assertTrue(dir.exists)
    assertEquals(3, FileUtils.listAllFiles(dir.getPath).length)
    
    FileUtils.delete(dir)
    assertFalse(dir.exists)
  }
  
  //TODO: test how it handles io exceptions: permissions, file not found
}