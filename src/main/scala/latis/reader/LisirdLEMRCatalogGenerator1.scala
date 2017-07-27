package latis.reader

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Text
import latis.metadata.Metadata
import latis.ops.Operation

/**
 * Creates a (hardcoded) catalog of the datasets that LEMR serves to the lisird website.
 */
class LisirdLEMRCatalogGenerator1 extends DatasetAccessor {
  
  override def getDataset: Dataset = generateFakeCatalog
  
  def getDataset(operations: Seq[Operation]): Dataset = {
    val dataset = generateFakeCatalog
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }
  
  def generateFakeCatalog: Dataset = {
    val s1 = Sample(Text(Metadata("name"), "cak"), Real(0))
    val s2 = Sample(Text(Metadata("name"), "composite_lyman_alpha"), Real(0))
    val s3 = Sample(Text(Metadata("name"), "composite_mg_index"), Real(0))
    val s4 = Sample(Text(Metadata("name"), "debrecen_photoheliographic"), Real(0))
    val s5 = Sample(Text(Metadata("name"), "historical_tsi"), Real(0))
    val s6 = Sample(Text(Metadata("name"), "international_sunspot_number"), Real(0))
    val s7 = Sample(Text(Metadata("name"), "noaa_radio_flux"), Real(0))
    val s8 = Sample(Text(Metadata("name"), "nrl2_ssi_P1D"), Real(0))
    val s9 = Sample(Text(Metadata("name"), "nrl2_ssi_P1M"), Real(0))
    val s10 = Sample(Text(Metadata("name"), "nrl2_ssi_P1Y"), Real(0))
    val s11 = Sample(Text(Metadata("name"), "nrl2_tsi_P1D"), Real(0))
    val s12 = Sample(Text(Metadata("name"), "nrl2_tsi_P1M"), Real(0))
    val s13 = Sample(Text(Metadata("name"), "nrl2_tsi_P1Y"), Real(0))
    val s14 = Sample(Text(Metadata("name"), "penticton_radio_flux_observed"), Real(0))
    val s15 = Sample(Text(Metadata("name"), "sdo_eve_bands_l3"), Real(0))
    val s16 = Sample(Text(Metadata("name"), "sdo_eve_diodes_l3"), Real(0))
    val s17 = Sample(Text(Metadata("name"), "sdo_eve_lines_l3"), Real(0))
    val s18 = Sample(Text(Metadata("name"), "sdo_eve_ssi_1nm_l3"), Real(0))
    val s19 = Sample(Text(Metadata("name"), "sfo_sunspot_indices"), Real(0))
    val s20 = Sample(Text(Metadata("name"), "sme_ssi"), Real(0))
    val s21 = Sample(Text(Metadata("name"), "sorce_mg_index"), Real(0))
    val s22 = Sample(Text(Metadata("name"), "sorce_ssi_l3"), Real(0))
    val s23 = Sample(Text(Metadata("name"), "sorce_tsi_24hr_l3"), Real(0))
    val s24 = Sample(Text(Metadata("name"), "sorce_tsi_6hr_l3"), Real(0))
    val s25 = Sample(Text(Metadata("name"), "tcte_tsi_24hr"), Real(0))
    val s26 = Sample(Text(Metadata("name"), "tcte_tsi_6hr"), Real(0))
    val s27 = Sample(Text(Metadata("name"), "timed_see_egs_ssi_l2"), Real(0))
    val s28 = Sample(Text(Metadata("name"), "timed_see_lines_l3"), Real(0))
    val s29 = Sample(Text(Metadata("name"), "timed_see_ssi_l3"), Real(0))
    val s30 = Sample(Text(Metadata("name"), "timed_see_ssi_l3a"), Real(0))
    val s31 = Sample(Text(Metadata("name"), "timed_see_xps_diodes_l2"), Real(0))
    val s32 = Sample(Text(Metadata("name"), "timed_see_xps_diodes_l3"), Real(0))
    val s33 = Sample(Text(Metadata("name"), "timed_see_xps_diodes_l3a"), Real(0))
    val s34 = Sample(Text(Metadata("name"), "timed_see_xps_ssi_l4"), Real(0))
    val s35 = Sample(Text(Metadata("name"), "timed_see_xps_ssi_l4a"), Real(0))
    val s36 = Sample(Text(Metadata("name"), "uars_solstice_ssi"), Real(0))
    
    val samples = Seq(s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,
                      s20,s21,s22,s23,s24,s25,s26,s27,s28,s29,s30,s31,s32,s33,s34,s35,s36)
    
    val f = Function(samples, Metadata("datasets"))
    val dataset = Dataset(f, Metadata("FakeLEMRCatalog"))
    dataset
  }
  
  def close: Unit = {}
  
}


object LisirdLEMRCatalogGenerator1 {
  def apply(): LisirdLEMRCatalogGenerator1 = new LisirdLEMRCatalogGenerator1
}
