//package latis.writer
//
//import org.junit._
//import latis.reader.tsml.TsmlReader
//import latis.ops.filter.Selection
//
//class TestInfoWriter extends WriterTest{
//  
//  @Test
//  def test_dap2 {
//    test_writer(getDataset("dap2"),"info")
//  }
//  @Test
//  def test_fof {
//    test_writer(getDataset(fof),"info")
//  }
//  @Test
//  def test_scalar {
//    test_writer(getDataset("scalar"),"info")
//  }
//  @Test
//  def test_tsi {
//    test_writer(getDataset("tsi"),"info")
//  }
//  @Test
//  def test_tof {
//    test_writer(getDataset(tof),"info")
//  }
//  
//  //@Test
//  def print_info {
//    print(fof, "info")
//  }
//  
//  //@Test 
//  def write_info_file {
//    write_to_file(fof, "info")
//  }
//
//}