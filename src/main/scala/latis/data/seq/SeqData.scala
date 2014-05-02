package latis.data.seq

import latis.data.IterableData

/**
 * Data that represents multiple data records.
 * Not multiple data values in a single record.
 * Each element is assumed to have the same type and size.
 */
abstract class SeqData extends IterableData  //TODO: extends Seq[Data]?
