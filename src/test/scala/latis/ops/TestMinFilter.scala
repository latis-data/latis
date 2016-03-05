package latis.ops

import latis.dm.Function
//import latis.dm.implicits._
import latis.dm.Real
import latis.metadata.Metadata
import latis.data.SampledData
import org.junit._
import Assert._
import latis.ops.filter._
import latis.dm.Dataset
import latis.dm.TestDataset
import latis.dm.Sample
import latis.writer.Writer
import latis.dm.Integer
import latis.dm.Tuple
import latis.dm.Text
import latis.dm.Index
import latis.time.Time
import latis.writer._

class TestMinFilter {
  
  //@Test
  def test_canonical = {
    val ds1 = TestDataset.canonical
    val ds2 = MinFilter("myReal")(ds1)
    latis.writer.AsciiWriter.write(ds2) 
  }
  
  @Test
  def canonical_min_int = {
    val ds1 = TestDataset.canonical
    val ds2 = MinFilter("myInt")(ds1)
    
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Text(t), Tuple(Seq(Integer(i), Real(r), Text(txt)))) => 
          assertEquals(1, i)}
      }
    }
  }
  
  @Test
  def canonical_min_real = {
    val ds1 = TestDataset.canonical
    val ds2 = MinFilter("myReal")(ds1)
    
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Text(t), Tuple(Seq(Integer(i), Real(r), Text(txt)))) => 
          assertEquals(1.1, r, 0.001)}
      }
    }
  }
  
  @Test
  def canonical_min_text = {
    val ds1 = TestDataset.canonical
    val ds2 = MinFilter("myText")(ds1)
    
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Text(t), Tuple(Seq(Integer(i), Real(r), Text(txt)))) => 
          assertEquals("A", txt)}
      }
    }
  }
  
  @Test
  def canonical_min_invalid_name = {
    val ds1 = TestDataset.canonical
    val ds2 = MinFilter("ShawnIsACoolDude")(ds1)
    
    ds2 match {
      case Dataset(Function(it)) => {
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def canonical_min_length = {
    val ds1 = TestDataset.canonical
    val ds2 = MinFilter("myInt")(ds1)
    
    ds2 match {
      case Dataset(func) => {
        assertEquals("1", func.getMetadata("length").get)
      }
    }
  }

  @Test
  def one_min_in_middle = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 5), Integer(Metadata("int2"), 1)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 2), Integer(Metadata("int2"), 1)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 3), Integer(Metadata("int2"), 1)))
    
    val samples = List(sample1, sample2, sample3)
    
    //Create dataset and run test
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("int1")(ds1)
    AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (2, (2, 1)), (t, (i1, i2)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def three_mins_in_middle = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 5)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 2)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 2)))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 2)))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 3)))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5)
    
    //Create dataset and run tests
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("int2")(ds1)
    AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (2, (1, 2)), (t, (i1, i2)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (3, (1, 2)), (t, (i1, i2)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (4, (1, 2)), (t, (i1, i2)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def three_mins_in_beginning = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 2)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 2)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 2)))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 5)))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), 3)))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5)
    
    //Create dataset and run tests
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("int2")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (1, (1, 2)), (t, (i1, i2)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (2, (1, 2)), (t, (i1, i2)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (3, (1, 2)), (t, (i1, i2)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def three_texts_in_middle = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "Z")))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "A")))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "A")))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "A")))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "S")))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5)
    
    //Create dataset and run tests
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("text")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Text(txt)))) => assertEquals( (2, (1, "A")), (t, (i1, txt)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Text(txt)))) => assertEquals( (3, (1, "A")), (t, (i1, txt)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Text(txt)))) => assertEquals( (4, (1, "A")), (t, (i1, txt)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def three_reals_in_middle = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 42.0)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 21.0)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 21.0)))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 21.0)))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 9000.0)))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5)
    
    //Create dataset and run tests
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("real")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Real(r)))) => assertEquals( (2, (1, 21.0)), (t, (i1, r)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Real(r)))) => assertEquals( (3, (1, 21.0)), (t, (i1, r)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Real(r)))) => assertEquals( (4, (1, 21.0)), (t, (i1, r)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def three_mins_non_contiguous = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 42.0)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), -8.0)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 12.0)))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 9000.0)))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), -8.0)))
    val sample6 = Sample(Integer(Metadata("t"), 6), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 500.0)))
    val sample7 = Sample(Integer(Metadata("t"), 7), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), -8.0)))
    val sample8 = Sample(Integer(Metadata("t"), 8), Tuple(Integer(Metadata("int1"), 1), Real(Metadata("real"), 22.0)))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8)
    
    //Create dataset and run tests
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("real")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Real(r)))) => assertEquals( (2, (1, -8.0)), (t, (i1, r)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Real(r)))) => assertEquals( (5, (1, -8.0)), (t, (i1, r)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Real(r)))) => assertEquals( (7, (1, -8.0)), (t, (i1, r)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def text_mins_non_contiguous = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "A")))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "Z")))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "S")))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "A")))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Integer(Metadata("int1"), 1), Text(Metadata("text"), "A")))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5)
    
    //Create dataset and run tests
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("text")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Text(txt)))) => assertEquals( (1, (1, "A")), (t, (i1, txt)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Text(txt)))) => assertEquals( (4, (1, "A")), (t, (i1, txt)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Text(txt)))) => assertEquals( (5, (1, "A")), (t, (i1, txt)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  /*
   * Test passes because comparison between Number and Text converts the Text to a double.
   * (Note that if the Number is NOT a double -- e.g. is an int -- the comparison fails)
   */
  @Test
  def real_text_mismatch = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Real(Metadata("real1"), 5.0), Integer(Metadata("int"), 1)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Real(Metadata("real1"), 2.0), Integer(Metadata("int"), 1)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Text(Metadata("real1"), "2.0"), Integer(Metadata("int"), 1)))   
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Real(Metadata("real1"), 3.0), Integer(Metadata("int"), 1)))
    
    val samples = List(sample1, sample2, sample3, sample4)
    
    //Create dataset and run test
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("real1")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Real(r), Integer(i)))) => assertEquals( (2, (2.0, 1)), (t, (r, i)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Text(r), Integer(i)))) => assertEquals( (3, ("2.0", 1)), (t, (r, i)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  /*
   * Test passes because comparison between Text and Number converts the Number to a string.
   */
  @Test
  def text_real_mismatch = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Text(Metadata("real1"), "5.0"), Integer(Metadata("int"), 1)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Text(Metadata("real1"), "2.0"), Integer(Metadata("int"), 1)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Real(Metadata("real1"), 2.0), Integer(Metadata("int"), 1)))   
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Text(Metadata("real1"), "3.0"), Integer(Metadata("int"), 1)))
    
    val samples = List(sample1, sample2, sample3, sample4)
    
    //Create dataset and run test
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("real1")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Text(r), Integer(i)))) => assertEquals( (2, ("2.0", 1)), (t, (r, i)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Real(r), Integer(i)))) => assertEquals( (3, (2.0, 1)), (t, (r, i)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def text_int_mismatch = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Text(Metadata("int1"), "5"), Integer(Metadata("int2"), 1)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Text(Metadata("int1"), "2"), Integer(Metadata("int2"), 1)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 2), Integer(Metadata("int2"), 1)))   
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Text(Metadata("int1"), "3"), Integer(Metadata("int2"), 1)))
    
    val samples = List(sample1, sample2, sample3, sample4)
    
    //Create dataset and run test
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("int1")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Text(i1), Integer(i2)))) => assertEquals( (2, ("2", 1)), (t, (i1, i2)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (3, (2, 1)), (t, (i1, i2)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def text_real_mismatch_at_start = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Real(Metadata("real1"), 2.0), Integer(Metadata("int"), 1)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Text(Metadata("real1"), "5.0"), Integer(Metadata("int"), 1)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Text(Metadata("real1"), "2.0"), Integer(Metadata("int"), 1)))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Real(Metadata("real1"), "3.0"), Integer(Metadata("int"), 1)))   
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Text(Metadata("real1"), "2.0"), Integer(Metadata("int"), 1)))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5)
    
    //Create dataset and run test
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("real1")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Real(r), Integer(i)))) => assertEquals( (1, (2.0, 1)), (t, (r, i)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Text(r), Integer(i)))) => assertEquals( (3, ("2.0", 1)), (t, (r, i)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Text(r), Integer(i)))) => assertEquals( (5, ("2.0", 1)), (t, (r, i)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def negative_min_value = {
    //Create samples for test dataset
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), -5)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), -5)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), -2)))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), -5)))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Integer(Metadata("int1"), 1), Integer(Metadata("int2"), -3)))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5)
    
    //Create dataset and run tests
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = MinFilter("int2")(ds1)
    //AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (1, (1, -5)), (t, (i1, i2)) )
        }  
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (2, (1, -5)), (t, (i1, i2)) )
        } 
        it.next match {
          case Sample(Integer(t), Tuple(Seq(Integer(i1), Integer(i2)))) => assertEquals( (4, (1, -5)), (t, (i1, i2)) )
        }  
        assert(!it.hasNext)
      }
    }
  }
  
}