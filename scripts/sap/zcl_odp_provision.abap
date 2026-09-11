* Provision an ODP OData service on an SAP system, headlessly.
*
* SAP ships no non-UI entry point for this: CL_RSODP_ODATA_GENERATOR only generates the
* MPC/DPC classes, and everything around it (model, service registration, hub activation)
* lives in CL_RSODP_ODATA_ODP_OUT_GA, which is driven by the SEGW guided-activity UI.
* This class performs the same three steps directly:
*
*   1. CL_RSODP_ODATA_GENERATOR->GENERATE_CLASSES      - generate MPC/DPC
*   2. /IWBEP/IF_REGISTER_MDL_SERVICE->REGISTER_SERVICE - register model + service (backend)
*   3. /IWFND/CL_MGW_ACTIVATION_API->ACTIVATE_SERVICE   - add to the Gateway hub + ICF node
*
* Run it with erpl-adt:
*   erpl-adt object create --type CLAS/OC --name ZCL_ODP_PROVISION --package '$TMP' \
*       --description 'ODP provisioning'
*   erpl-adt source write ZCL_ODP_PROVISION --type CLAS --file zcl_odp_provision.abap
*   erpl-adt activate ZCL_ODP_PROVISION
*   erpl-adt object run ZCL_ODP_PROVISION
*
* Adjust the constants and the ODP name below. The ODP name is derived from the CDS view's
* SQL VIEW name (not the CDS name) plus a category suffix: $F for @Analytics.dataCategory
* #CUBE, $P for #DIMENSION, $T for #TEXT. Query the available ones with
*   SELECT viewname, odpname FROM rsodpabapcdsextb( p_langu = @sy-langu ).
*
* $TMP is deliberate: the extraction annotations and the classic ODP APIs are not released
* for ABAP Cloud, so a HOME-tier package rejects them on an ABAP Cloud developer trial.

CLASS zcl_odp_provision DEFINITION PUBLIC FINAL CREATE PUBLIC.
  PUBLIC SECTION.
    INTERFACES if_oo_adt_classrun.
ENDCLASS.

CLASS zcl_odp_provision IMPLEMENTATION.
  METHOD if_oo_adt_classrun~main.

    CONSTANTS lc_model   TYPE /iwbep/med_mdl_technical_name VALUE 'Z_ODP_FCT_MDL'.
    CONSTANTS lc_service TYPE /iwbep/med_grp_technical_name VALUE 'Z_ODP_FCT_SRV'.
    CONSTANTS lc_mpc     TYPE seoclsname VALUE 'ZCL_Z_ODP_FCT_MPC'.
    CONSTANTS lc_dpc     TYPE seoclsname VALUE 'ZCL_Z_ODP_FCT_DPC'.

    DATA lt_odp TYPE cl_rsodp_odata_odp_out_mpc=>tt_odp.
    DATA ls_odp TYPE cl_rsodp_odata_odp_out_mpc=>ts_odp.
    DATA lv_rc  TYPE sysubrc.

    ls_odp-odpname = 'ZJRODPVSQL$F'.              APPEND ls_odp TO lt_odp.

    NEW cl_rsodp_odata_generator( )->generate_classes(
      EXPORTING
        i_corrnr          = ''
        i_devclass        = '$TMP'
        i_model_name      = lc_model
        i_model_version   = '0001'
        i_service_name    = lc_service
        i_service_version = '0001'
        i_mpc_name        = lc_mpc
        i_dpc_name        = lc_dpc
        i_context         = 'ABAP_CDS'
        i_t_odp           = lt_odp
      IMPORTING
        e_rc              = lv_rc ).
    out->write( |generate_classes rc = { lv_rc }| ).
    IF lv_rc <> 0.
      RETURN.
    ENDIF.

    DATA lv_transport TYPE trkorr.
    DATA lv_package   TYPE devclass VALUE '$TMP'.
    TRY.
        CAST /iwbep/if_register_mdl_service( NEW /iwbep/cl_register_mdl_service( ) )->register_service(
          EXPORTING
            iv_service_technical_name = lc_service
            iv_service_version        = '0001'
            iv_service_external_name  = 'Z_ODP_FCT_SRV'
            iv_model_technical_name   = lc_model
            iv_model_version          = '0001'
            iv_model_provider_class   = lc_mpc
            iv_data_provider_class    = lc_dpc
            iv_model_description      = 'ODP jr model'
            iv_service_description    = 'ODP jr service'
          CHANGING
            cv_transport              = lv_transport
            cv_package                = lv_package ).
        out->write( 'register_service: OK' ).
      CATCH /iwbep/cx_mgw_med_exception INTO DATA(lx_reg).
        out->write( |register_service failed: { lx_reg->get_text( ) }| ).
        RETURN.
    ENDTRY.

    DATA lv_srg  TYPE /iwfnd/med_mdl_srg_identifier.
    DATA lv_tech TYPE /iwfnd/med_mdl_srg_name.
    TRY.
        /iwfnd/cl_mgw_activation_api=>get_instance( )->activate_service(
          EXPORTING
            iv_service_name         = lc_service
            iv_service_version      = '0001'
            iv_system_alias         = 'LOCAL'
            iv_package              = '$TMP'
            iv_default_client       = abap_true
            iv_do_activate_icf_node = abap_true
            iv_suppress_dialog      = abap_true
          IMPORTING
            ev_srg_identifier       = lv_srg
            ev_tech_service_name    = lv_tech ).
        out->write( |activate_service OK; srg={ lv_srg }| ).
      CATCH /iwfnd/cx_med_remote INTO DATA(lx_act).
        out->write( |activate_service failed: { lx_act->get_text( ) }| ).
    ENDTRY.

  ENDMETHOD.
ENDCLASS.
