REPORT z_odp_odata_bootstrap.

"--- Parameters: give your service name once you have generated it in SEGW
PARAMETERS p_srv TYPE /iwfnd/med_mdl_srg_name OBLIGATORY DEFAULT 'Z_ODP_DEMO_SRV'.
PARAMETERS p_ver TYPE /iwfnd/med_mdl_version  DEFAULT '0001'.

"--- Local helper to activate ICF nodes recursively
CLASS lcl_icf DEFINITION FINAL.
  PUBLIC SECTION.
    CLASS-METHODS activate_icf_path IMPORTING iv_path TYPE icfurlbuf.
ENDCLASS.

CLASS lcl_icf IMPLEMENTATION.
  METHOD activate_icf_path.
    DATA lv_host TYPE icfname VALUE 'DEFAULT_HOST'.  " <-- elementary type, no VALUE( ) for structures
    CALL FUNCTION 'HTTP_ACTIVATE_NODE'
      EXPORTING
        url      = iv_path
        hostname = lv_host
        expand   = abap_true
      EXCEPTIONS
        node_not_existing        = 1
        enqueue_error            = 2
        no_authority             = 3
        url_and_nodeguid_space   = 4
        url_and_nodeguid_fill_in = 5
        OTHERS                   = 6.
    IF sy-subrc <> 0.
      MESSAGE ID sy-msgid TYPE 'E' NUMBER sy-msgno
        WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    ELSE.
      WRITE: / |Activated ICF path { iv_path } under DEFAULT_HOST (recursive).|.
    ENDIF.
  ENDMETHOD.
ENDCLASS.

START-OF-SELECTION.
lcl_icf=>activate_icf_path( '/sap/public/opu' ).
lcl_icf=>activate_icf_path( '/sap/opu' ).

WRITE: / 'Opening Gateway Service Maintenance…'.
CALL TRANSACTION '/IWFND/MAINT_SERVICE'.
WRITE: / |In /IWFND/MAINT_SERVICE: Add Service → System Alias = LOCAL → Register { p_srv } v{ p_ver }.|.
