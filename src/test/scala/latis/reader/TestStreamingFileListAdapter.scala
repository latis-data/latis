package latis.reader

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import org.junit.AfterClass
import org.junit.Assert.assertEquals
import org.junit.BeforeClass
import org.junit.Test
import latis.dm.Dataset
import latis.ops.BinAverage
import latis.ops.Operation
import latis.ops.Projection
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter
import latis.ops.filter.LimitFilter
import latis.ops.filter.Selection
import latis.ops.math.MathOperation
import latis.reader.tsml.TsmlReader
import latis.util.FileUtils
import org.junit.Ignore

class TestStreamingFileListAdapter { //can't extend AdapterTests because Samples are unordered
  
  @Test
  def test {
    val ds = TsmlReader("datasets/test/files_streaming.tsml").getDataset
    val data = ds.toStringMap
    assertEquals(6, data.size)
    assertEquals(3, data("myTime").length)
    assert(data("fileSize").contains("4"))
    assert(data("myText").contains("B"))
    assert(data("file").contains("Foo1970003bar3v3.3C.dat"))
  }
  
  @Test
  @Ignore // ignore this test by default, because it takes a lot of time & system resources to run
  def large_nested_dir {
    try {
      val numFiles = TestStreamingFileListAdapter.populateLargeNestedTmpDir()
      val ds = TsmlReader("datasets/test/files_streaming_large_nested.tsml").getDataset()
      val data = ds.toStringMap
      assertEquals(numFiles, data("index").length)
    }
    finally {
      val dir = TestStreamingFileListAdapter.largeNestedTmpDir
      if (dir.exists()) {
        FileUtils.delete(dir)
      }
    }
  }
  
  @Test
  @Ignore // ignore this test by default, because it takes a lot of time & system resources to run
  def large_flat_dir {
    try {
      val numFiles = TestStreamingFileListAdapter.populateLargeFlatTmpDir()
      val ds = TsmlReader("datasets/test/files_streaming_large_flat.tsml").getDataset()
      val data = ds.toStringMap
      assertEquals(numFiles, data("index").length)
    }
    finally {
      val dir = TestStreamingFileListAdapter.largeFlatTmpDir
      if (dir.exists()) {
        FileUtils.delete(dir)
      }
    }
  }
  
  //The rest of the tests are copied from AdapterTests with specific value checks changed to containment checks
  def datasetName: String = "files_streaming"
  
  def getDataset(ops: Seq[Operation]) = TsmlReader("datasets/test/" + datasetName + ".tsml").getDataset(ops)
  def getDataset: Dataset = getDataset(Seq[Operation]())
  
  //---- Test Projection ----------------------------------------------------//
  
  @Test
  def project_all {
    val ops = List(Projection("myTime, myInt, myReal, myText"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length) //got all samples
    assertEquals(4, data.keySet.size)    //4 projected variables
  }
  
  @Test
  def project_domain_and_one_range {
    //TODO: reduce unnamed tuple of one
    val ops = List(Projection("myTime, myInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length) //got all samples
    assertEquals(2, data.keySet.size)    //2 projected variables
  }
  
  @Test
  def project_domain_and_two_range {
    val ops = List(Projection("myTime, myInt, myReal"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length) //got all samples
    assertEquals(3, data.keySet.size)    //3 projected variables
  }
  
  //@Test
  //TODO: projection order not yet supported
  def project_diff_order {
    val ops = List(Projection("myTime, myReal, myInt"))
    val ds = getDataset(ops)
    assertEquals("myInt", ds.toSeq(2).getName)
  }
  
  @Test
  def project_without_domain {
    val ops = List(Projection("myInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myInt").length)
    assertEquals(2, data.keySet.size) //index and time
  }
  
  @Test
  def project_only_domain {
    val ops = List(Projection("myTime"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length)
    assertEquals(2, data.keySet.size) //index and time
  }
  
  //---- Test Selection -----------------------------------------------------//
  
  @Test
  def select_on_domain {
    //Note, domain is Time as Text with format yyyy/MM/dd but ISO form works
    //TODO: support time selection with native format: "time > 1970/01/01"
    val ops = List(Selection("time > 1970-01-01"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assertEquals(2, data("myInt").length)
    assertEquals(2, data("myReal").length)
    assertEquals(2, data("myText").length)
    assert(data("myInt").contains("2"))
  }
  
  @Test
  def select_on_one_range_value {
    val ops = List(Selection("myInt >= 2"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myInt").length)
    assert(data("myInt").contains("2"))
  }
  
  @Test
  def select_on_two_range_values { //TODO: misnamed or wrong test
    val ops = List(Selection("time > 1970-01-01"), Selection("myInt <= 3"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assert(data("myInt").contains("2"))
  }
  
  @Test
  def two_selects_on_domain {
    val ops = List(Selection("time > 1970-01-01"), Selection("time <= 1970-01-03"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assert(data("myInt").contains("2"))
  }
  
  @Test
  def two_selects_on_range_value {
    val ops = List(Selection("myInt > 1"), Selection("myInt <= 3"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myInt").length)
    assert(data("myInt").contains("2"))
  }
  
  @Test
  def string_match {
    val ops = List(Selection("myText =~ B"))
    val data = getDataset(ops).toStringMap
    assertEquals(1, data("myText").length)
    assert(data("myInt").contains("2"))
  }
  
  //TODO: test various equals, match,... or leave to Operation tests?
  
  //---- Test Filters -------------------------------------------------------//
  
  //@Test
  def first {
    val ops = List(FirstFilter())
    val data = getDataset(ops).toStringMap
    assertEquals(1, data("myTime").length)
    assert(data("myInt").contains("1"))
  }  
  
  //@Test
  def last {
    val ops = List(LastFilter())
    val data = getDataset(ops).toStringMap
    assertEquals(1, data("myTime").length)
    assert(data("myInt").contains("3"))
  }
  
  //@Test
  def limit {
    val ops = List(LimitFilter(2))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assert(data("myInt").contains("1"))
  }
  
  
  //---- Test Combined Operations -------------------------------------------//
  //TODO: lots of permutations
    
  @Test
  def projection_before_selection {
    val ops = List(Projection("myTime, myReal"), Selection("myReal > 2"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //2 projected variables
    assertEquals(2, data("myTime").length)
    assert(data("myReal").contains("2.2"))
  }
  
  @Test
  def selection_before_projection {
    val ops = List(Selection("myReal > 2"), Projection("myTime, myReal"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //2 projected variables
    assertEquals(2, data("myTime").length)
    assert(data("myReal").contains("2.2"))
  }
  
  @Test
  def select_on_non_projected_domain_with_selection_first {
    val ops = List(Selection("time < 1970-01-02"), Projection("myReal"))
    val data = getDataset(ops).toDoubleMap
    assertEquals(2, data.keySet.size)  //index and myReal
    assertEquals(1, data("myReal").length)
    assert(data("myReal").contains(1.1))
  }
  
  //@Test
  def select_on_index_when_no_projected_domain {
    val ops = List(Projection("myReal"), Selection("index > 1"))
    //val ops = List(Selection("index > 1"), Projection("myReal")) //doesn't work, no Index unless domain not projected
    val ds = getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(2, data.keySet.size)  //index and myReal
    assertEquals(1, data("index").length)
    assert(data("index").contains(0))
    assert(data("myReal").contains(3.3))
  }
  
  @Test
  def select_on_index_when_no_projected_range {
    val ops = List(Projection("myTime"), Selection("index = 1"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //index and time
    assertEquals(1, data("index").length)
    assert(data("index").contains("0"))
  }
  
  //@Test
  def apply_math_then_binave {
    val ops = List(MathOperation((d: Double) => d*2), new BinAverage(172800000.0)) //2 days
    val ds = getDataset(ops)
    val data = ds.toDoubleMap
    assert(data("myTime").contains(43200000))
    assert(data("myInt").contains(3.0))
  }
  

}

object TestStreamingFileListAdapter {
  
  private var _tmpDir: File = null
  
  def getTmpDir(): File = {
    if (_tmpDir == null) {
      _tmpDir = new File("/tmp/latis_file_test")
    }
    if (!_tmpDir.exists()) {
      _tmpDir.mkdirs()
    }
    return _tmpDir
  }
  
  val largeNestedTmpDir: File = new File("/tmp/latis_file_test_large_nested")
  val largeFlatTmpDir: File = new File("/tmp/latis_file_test_large_flat")
  
  // Super hacky: check if the JVM is running in debug mode. If we are, it's going
  // to take waaay to long to create these files, so we should trim our numbers a
  // bit.
  val isJvmDebug: Boolean = java.lang.management.ManagementFactory.getRuntimeMXBean().
    getInputArguments().toString().indexOf("jdwp") >= 0;
  
  def populateLargeNestedTmpDir(): Int = {
    
    if (isJvmDebug) {
      println("ALERT: Jvm Debug Mode detected - making many fewer files so that this actually finishes")
    }
    
    // This is a *very* rough approximation of the MMS mission. The numbers
    // have been fiddled with a bit to get us roughly a few hundred-thousand 
    // files. This is in no way real data.
    val numSpacecraft = if (isJvmDebug) 2 else 4 
    val numInstruments = if (isJvmDebug) 5 else  22
    val numModes = if (isJvmDebug) 2 else 5
    val numDays = if (isJvmDebug) 10 else { 365 * 2 }
    
    if (largeNestedTmpDir.exists()) {
      FileUtils.delete(largeNestedTmpDir)
    }
    largeNestedTmpDir.mkdirs()
    
    var spacecraft: Int = -1
    var instrument: Int = -1
    var mode: Int = -1
    var day: Int = -1
    
    var filesCreated = 0
    
    for (spacecraft <- 1 to numSpacecraft) {
      val spacecraftDir = new File(largeNestedTmpDir, s"spacecraft${spacecraft}")
      spacecraftDir.mkdir()
      
      for (instrument <- 1 to numInstruments) {
        val instrumentDir = new File(spacecraftDir, s"instrument${instrument}")
        instrumentDir.mkdir()
        
        for (mode <- 1 to numModes) {
          val modeDir = new File(instrumentDir, s"mode${mode}")
          modeDir.mkdir()
          
          for (day <- 1 to numDays) {
            val file = new File(modeDir, s"day${"%05d".format(day)}.txt")
            file.createNewFile()
            filesCreated += 1
          }
        }
      }
    }
    
    filesCreated
  }
  
  def populateLargeFlatTmpDir(): Int = {
    
    if (isJvmDebug) {
      println("ALERT: Jvm Debug Mode detected - making many fewer files so that this actually finishes")
    }
    
    // This is a *very* rough approximation of the MMS mission. The numbers
    // have been fiddled with a bit to get us roughly a few hundred-thousand 
    // files. This is in no way real data.
    val numSpacecraft = if (isJvmDebug) 2 else 4 
    val numInstruments = if (isJvmDebug) 5 else  22
    val numModes = if (isJvmDebug) 2 else 5
    val numDays = if (isJvmDebug) 10 else { 365 * 2 }
    
    if (largeFlatTmpDir.exists()) {
      FileUtils.delete(largeFlatTmpDir)
    }
    largeFlatTmpDir.mkdirs()
    
    var spacecraft: Int = -1
    var instrument: Int = -1
    var mode: Int = -1
    var day: Int = -1
    
    var filesCreated = 0
    
    for (
        spacecraft <- 1 to numSpacecraft;
        instrument <- 1 to numInstruments;
        mode <- 1 to numModes;
        day <- 1 to numDays
    ) {
      val filename = s"spacecraft${spacecraft}_instrument${instrument}_mode${mode}_day${day}.txt"
      val file = new File(largeFlatTmpDir, filename)
      file.createNewFile()
      filesCreated += 1
    }
    
    filesCreated
  }
  
  @BeforeClass
  def makeTmpFiles {
    val tmpDir = getTmpDir()
    
    //make sure this remains consistent with shared AdapterTests
    (new File(tmpDir, "Foo1970001bar1v1.1A.dat")).createNewFile
    Files.write(Paths.get(tmpDir.getPath,"Foo1970001bar1v1.1A.dat"),List(1,2,3,4).map(_.toByte).toArray)
    (new File(tmpDir, "Foo1970002bar2v2.2B.dat")).createNewFile
    (new File(tmpDir, "Foo1970003bar3v3.3C.dat")).createNewFile
    //test with non-matching file
    (new File(tmpDir, "junk")).createNewFile
    //(new File(tmpDir, "subDir")).mkdir
    //(new File("/tmp/latis_file_test/subDir/Foo1970004bar4v4.4D.dat")).createNewFile
  }
  
  @AfterClass
  def removeTmpFiles {
    if (_tmpDir != null && _tmpDir.exists()) {
      FileUtils.delete(_tmpDir)
    }
  }
}