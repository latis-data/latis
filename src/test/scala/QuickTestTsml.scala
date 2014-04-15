import latis.dm._
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import latis.reader.tsml._
import latis.reader.tsml.ml._
import latis.ops.Projection
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.ops.filter._
import latis.writer.CsvWriter
import java.io.FileOutputStream

object QuickTestTsml extends App {
  
  //TODO: make an app that takes the same request as a server
  
    //val tsml = "/home/lindholm/git/latis-maven/src/test/resources/datasets/iuvs_periapse_radiance.tsml"
    //val tsml = "datasets/test/nested_function.tsml"
    val tsml =   "/home/lindholm/git/latis-mms/src/test/resources/datasets/mms_burst_data_segment.tsml"
        
      
    val ops = ArrayBuffer[Operation]()
    //ops += Selection("time < 1982-01-01")
    //ops += Selection("area!=-999")
    //ops += Projection("time,data_rate_mode,file")
    //ops += Operation("first")
    //ops += Operation("last")
    //ops += LimitFilter(3)
    
    
    val ds = TsmlReader(tsml).getDataset(ops)
    
    AsciiWriter.write(ds)
    
    //val out = new FileOutputStream("/data/tmp/usaf_mwl.csv")
    //Writer(out, "csv").write(ds)
}
