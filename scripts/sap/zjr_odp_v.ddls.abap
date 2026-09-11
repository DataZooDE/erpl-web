@AbapCatalog.sqlViewName: 'ZJRODPVSQL'
@AbapCatalog.compiler.compareFilter: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'ODP delta test view'
@Analytics.dataCategory: #CUBE
@Analytics.dataExtraction.enabled: true
@Analytics.dataExtraction.delta.byElement.name: 'changed_at'
@Analytics.dataExtraction.delta.byElement.maxDelayInSeconds: 1800
define view ZJR_ODP_V as select from zjr_odp_data {
  key item_id    as ItemId,
      item_name  as ItemName,
      item_value as ItemValue,
      changed_at as ChangedAt
}
