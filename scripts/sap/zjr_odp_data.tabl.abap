@EndUserText.label : 'ODP delta test data'
@AbapCatalog.enhancement.category : #NOT_EXTENSIBLE
@AbapCatalog.tableCategory : #TRANSPARENT
@AbapCatalog.deliveryClass : #A
@AbapCatalog.dataMaintenance : #RESTRICTED
define table zjr_odp_data {
  key client   : abap.clnt not null;
  key item_id  : abap.char(10) not null;
  item_name    : abap.char(40);
  item_value   : abap.int4;
  changed_at   : timestampl;
}
