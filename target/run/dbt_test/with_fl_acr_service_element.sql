
        insert into OPA_DEV.DBT_TEST.with_fl_acr_service_element (SK_SERVICE_ELEMENT_ID, SK_SERVICE_ID, SERVICE_VERSION, ATCOM_SUB_SER_ID, FILE_DT)
        (
            select SK_SERVICE_ELEMENT_ID, SK_SERVICE_ID, SERVICE_VERSION, ATCOM_SUB_SER_ID, FILE_DT
            from OPA_DEV.DBT_TEST.with_fl_acr_service_element__dbt_tmp
        );
      
    