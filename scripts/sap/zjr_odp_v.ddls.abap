@AbapCatalog.sqlViewName: 'ZJRODPVSQL'
@AbapCatalog.compiler.compareFilter: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'ODP delta test view'
@Analytics.dataCategory: #CUBE
@Analytics.dataExtraction.enabled: true
// changeDataCapture is what makes the ODP delta-capable. The byElement form that used to
// be here activates and looks right, but RODPS_REPL_ODP_GET_DETAIL still reports
// supports_delta = ' ', so no DeltaLinksOf entity set is generated and no delta link is
// ever returned. See the ODP section of CLAUDE.md.
@Analytics.dataExtraction.delta.changeDataCapture.automatic: true
define view ZJR_ODP_V as select from zjr_odp_data {
  key item_id    as ItemId,
      item_name  as ItemName,
      item_value as ItemValue,
      changed_at as ChangedAt
}
