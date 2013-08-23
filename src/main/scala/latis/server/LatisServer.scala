package latis.server

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import latis.util.LatisProperties
import latis.reader.tsml.TsmlReader
import latis.metadata.Registry
import latis.util.LatisServerProperties
import latis.writer.HttpServletWriter

class LatisServer extends HttpServlet {

  override def init() {
    LatisProperties.init(getServletConfig)
  }
  
  override def doGet(request : HttpServletRequest, response : HttpServletResponse) {
    //TODO: error handling
    
    //Get the request not including the constraints
    val path = request.getPathInfo()
                    
    //Get the type of the output request from the dataset suffix.
    val index = path.lastIndexOf(".");
    val suffix = path.substring(index+1); //suffix
    val dsname = path.substring(1,index); //dataset name, drop leading "/"
    
    
    //Get Dataset
    val url = Registry.getTsmlUrl(dsname)
    val reader = TsmlReader(url)
    
    
    
    /*
     * TODO: apply constraints
     * Map with type so we can be api agnostic?
     *   DAP or SQL...
     *   projection, selection, operation?
     * delegate to DapHandler... to build a Seq of Ops?
     * add keyword to URL: latis/dap/dataset.json...?
     */
    val constraints = DapConstraintParser.parseQuery(request.getQueryString)
    val dataset = reader.getDataset(constraints)


    val writer = HttpServletWriter(response, suffix)
    
    writer.write(dataset)
    
    //Let Reader know that it can release resource (e.g. open files, database connections).
    reader.close()
  }

}