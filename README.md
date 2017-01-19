# LaTiS

LaTiS is a software framework for data access, processing, and output.
The modular architecture supports reusable and custom *Readers* to read a
dataset from its native source, *Operations* to manipulate the dataset,
and *Writers* to output the dataset in the desired form. Datasets can be
read from diverse sources, combined in various ways to derive new datasets,
and written to any number of formats. LaTiS can enable simple access to a
single data file or it can be used to orchestrate an entire data processing workflow.

The core feature of LaTiS that enables these capabilities is its *Functional Data Model*.
This data model extends the Relational Data Model to add the concept of
Functional Relationships between independent and dependent variables which are
prevalent in scientific data. This model provides a mathematical foundation for
describing any dataset in terms of only three variable types:

    Scalar:   A single Variable.
    Tuple:    A group of Variables.
    Function: A mapping from one Variable (domain) to another (range).

Since *Variables* can be any one of these three types, they can be composed in
arbitrarily complex ways to represent the underlying nature of any dataset.
LaTiS does not simply provide an *alternate* data model, it can be used to
represent the fundamental mathematical structure of any other data model.
In other words, any data model can be mapped to the lower level LaTiS Data Model.
The Variable types can then be extended to add semantics that are specific to a
particular scientific discipline (e.g. Temperature as a specific type of Scalar).
The resulting datasets can still be used by those outside that discipline, albeit
with a loss of the extra semantics, because the model representation can always be
reduced to the basic three variable types. In this way, LaTiS provides a new level
of interoperability between disparate datasets.

The Open Source Scala implementation of the LaTiS Data Model managed here is designed
to serve as a Domain Specific Language (DSL) for scientific data analysis. The version
2.x series is in part a testbed for the evolution of the LaTiS DSL. Version 3 of LaTiS
will provide a full featured interactive data analysis tool using the Scala REPL
(command-line interface) to build on the ability of LaTiS to represent scientific
datasets with higher level Functional semantics (e.g. time series of grids) as opposed
to the more commonly used multi-dimensional array constructs. Combining the Functional
semantics of the data model with the Functional Programming constructs of Scala promises
to be quite powerful.

The LaTiS Data Model can also encapsulate information about the dataset (metadata)
such as units and standard names. Operations that modify the data can also add
provenance information to the metadata of the new data products.

Perhaps the current "killer app" for the LaTiS framework is its web service
interface to the underlying LaTiS DSL. This RESTful API implements the standardized
OPeNDAP (DAP2) data request and reply specification. Data providers can easily install
a LaTiS Server and expose the datasets they wish to serve by creating *TSML* dataset
descriptors. The service layer will accept queries that include *selection* constraints
(e.g. time > 2014-05-01), as opposed to requiring array index notation, and function
calls (e.g. format_time(yyyyDDD)) that can subset, aggregate, and perform other
operations on the server before writing the resulting dataset to the client.

For more information, see the [wiki](https://github.com/latis-data/latis/wiki)
