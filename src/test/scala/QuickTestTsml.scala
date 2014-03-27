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
import latis.ops.Selection
import latis.ops.LastFilter
import latis.ops.FirstFilter
import latis.ops.LimitFilter
import latis.writer.CsvWriter
import java.io.FileOutputStream

object QuickTestTsml extends App {
  
  //TODO: make an app that takes the same request as a server
  
    val tsml = "/home/lindholm/git/nrlssi/src/main/resources/datasets/usaf_mwl.tsml"
      
    val ops = ArrayBuffer[Operation]()
    //ops += Selection("time<=2015-04-01T01:00:00")
    //ops += Projection("time,data_rate_mode,file")
    //ops += Operation("first")
    //ops += Operation("last")
    //ops += LimitFilter(3)
    
    
    val ds = TsmlReader(tsml).getDataset(ops)
    
    //Writer("asc").write(ds)
    
    val out = new FileOutputStream("/data/tmp/usaf_mwl.csv")
    Writer(out, "csv").write(ds)
}
