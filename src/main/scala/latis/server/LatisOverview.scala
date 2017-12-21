package latis.server

import java.io.PrintWriter
import javax.servlet.ServletConfig
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import scala.util.Try

import com.typesafe.scalalogging.LazyLogging
import scalatags.stylesheet._
import scalatags.Text.Frag

import latis.dm._
import latis.reader.DatasetAccessor
import latis.util.LatisProperties

/**
 * For creating the LaTiS overview page.
 */
object LatisOverview extends LazyLogging {

  /**
   * Define the style for the overview page.
   */
  object Style extends StyleSheet {
    import scalatags.Text.all._
    initStyleSheet()

    override def customSheetName =
      Option("overview")

    val bold = cls(
      fontWeight := "bold"
    )

    val italic = cls(
      fontStyle := "italic"
    )

    val `hanging-indent` = cls(
      textIndent := "-22px",
      paddingLeft := "22px"
    )

    // Can't figure out how to express '.hanging-indent > p'
    val `hanging-indent-p` = cls(
      margin := "5px 0"
    )

    val `section-content` = cls(
      marginLeft := "20px"
    )
  }

  /**
   * Return a Frag representing the overview page.
   */
  def makePage(usage: Frag, datasets: Frag, output: Frag, filter: Frag): Frag = {
    import scalatags.Text.short._
    import scalatags.Text.tags2.{style, title}

    html(
      head(
        title("LaTiS Overview"),
        // We can't express everything we need to using scalatag's CSS
        // support, so this gets hard-coded.
        style(
          """html, body {
          |    margin: 0 auto;
          |    padding: 0;
          |    max-width: 800px;
          |}
          |
          |dt {
          |    font-weight: bold;
          |    margin-bottom: 3px;
          |}
          |
          |dd {
          |    margin-bottom: 8px;
          |}
          """.stripMargin
        ),
        style(Style.styleSheetText)
      ),
      body(
        h1("LaTiS Overview"),
        usage,
        div(*.id:="datasets")(
          h2("Available Datasets"),
          div(Style.`section-content`)(datasets)
        ),
        output,
        filter
      )
    )
  }

  /**
   * Return a Frag describing the usage.
   */
  def makeUsage(url: String): Frag = {
    import scalatags.Text.short._

    div(Style.`section-content`, Style.`hanging-indent`)(
      p(Style.`hanging-indent-p`)(
        span(Style.bold, Style.italic)("Usage: "),
        s"$url",
        span(Style.italic)("dataset"),
        ".",
        span(Style.italic)("suffix"),
        "?",
        span(Style.italic)("projection"),
        "&",
        span(Style.italic)("selection"),
        "&",
        span(Style.italic)("filter")
      ),
      br(),
      p(Style.`hanging-indent-p`)(
        span(Style.bold, Style.italic)("dataset: "),
        "Name of dataset (see ",
        a(*.href:="#datasets")("Available Datasets"),
        ")"
      ),
      p(Style.`hanging-indent-p`)(
        span(Style.bold, Style.italic)("suffix: "),
        "Type of output (see ",
        a(*.href:="#suffixes", "Output Options"),
        ")"
      ),
      p(Style.`hanging-indent-p`)(
        span(Style.bold, Style.italic)("projection: "),
        "List of variables to return with optional ",
        span("hyperslab"),
        " (index subset) definitions. Defaults to all. The Dataset Descriptor ",
        "Structure (DDS) will describe the variables for each dataset. Use the ",
        span("dds"),
        " suffix to get a dataset's DDS."
      ),
      p(Style.`hanging-indent-p`)(
        span(Style.bold, Style.italic)("selection: "),
        "Zero or more relative constraints on a variable ",
        "(e.g. time>=2010-01-01). Each much be preceded by a '&'."
      ),
      p(Style.`hanging-indent-p`)(
        span(Style.bold, Style.italic)("filter: "),
        "Zero or more filters to be applied to the data (see ",
        a(*.href:="#filters")("Filter Options"),
        "). Each must be preceded by a '&'."
      )
    )
  }

  /**
   * Return a Frag describing the available datasets.
   */
  def makeAvailableDatasets(catalog: Dataset): Frag = {
    import scalatags.Text.short._

    val ds: Seq[Frag] = catalog match {
      case Dataset(Function(it)) => it.map {
        case Sample(Text(name), TupleMatch(Text(desc), TupleMatch(Text(url)))) =>
          Option {
            frag(
              dt(a(*.href:=url)(name)),
              dd(desc)
            )
          }
        case _ => None
      }.toSeq.flatten
      case _ => Seq.empty
    }

    dl(ds)
  }

  /**
   * Return a Frag describing the available outputs.
   */
  def makeOutputOptions: Frag = {
    import scalatags.Text.short._

    val ds: Seq[Frag] =
      LatisProperties.getPropertiesWithRoot("writer").toSeq.collect {
        case (k, v) if k.contains("description") && v.nonEmpty =>
          frag(dt(k.split('.').head), dd(v))
      }

    div(*.id:="suffixes")(
      h2("Output Options (suffix)"),
      div(Style.`section-content`)(
        p(
          "These suffixes can be appended to the dataset name ",
          "(as in dataset.json) to specify the output format for the data."
        ),
        dl(ds)
      )
    )
  }

  /**
   * Return a Frag describing the available operations.
   */
  def makeFilterOptions: Frag = {
    import scalatags.Text.short._

    val ds: Seq[Frag] =
      LatisProperties.getPropertiesWithRoot("operation").toSeq.collect {
        case (k, v) if k.contains("description") && v.nonEmpty =>
          frag(dt(k.split('.').head), dd(v))
      }

    div(*.id:="filters")(
      h2("Filter Options (Operations)"),
      div(Style.`section-content`)(
        p(
          "These filter options (operations) can be added to the query string ",
          "to affect what data is returned."
        ),
        dl(ds)
      )
    )
  }

  /**
   * Write the overview page.
   */
  def write(req: HttpServletRequest, res: HttpServletResponse): Unit = {
    res.setContentType("text/html")

    val catalog: Try[Dataset] = Try {
      DatasetAccessor.fromName("catalog").getDataset()
    }

    catalog.failed.foreach { e =>
      logger.error("Failed to get catalog.", e)
    }

    val overview = {
      import scalatags.Text.short._

      makePage(
        makeUsage(req.getRequestURL().toString()),
        catalog.map(makeAvailableDatasets(_)).getOrElse(
          p("The catalog could not be loaded.")
        ),
        makeOutputOptions,
        makeFilterOptions
      )
    }

    val writer = new PrintWriter(res.getOutputStream())
    writer.print(overview.render)
    writer.flush()
  }
}
