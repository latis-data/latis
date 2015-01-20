package latis.ops.resample

trait ResamplingStrategy {

  /*
   * add static members that implement "resample"?
   * that can override Function's default resample? mixed in
   * 
   * but at Function or Dataset (monad) level?
   * 
   * Resample has resample(sample1: Sample, sample2: Sample, domain: Variable): Sample
   * don't want to exclude strategies that need more than neighbor samples
   * 
   * use DomainSet? 
   *   indexOf like visad
   *   but can we generally get the DomainSet?
   *     easy for Function with SampledData
   *     seems unfortunate to have to decompose sample iterator
   */
}