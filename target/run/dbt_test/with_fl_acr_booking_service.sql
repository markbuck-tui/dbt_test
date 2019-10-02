
        insert into OPA_DEV.DBT_TEST.with_fl_acr_booking_service (SK_BOOKING_SERVICE_ID, SK_BOOKING_ID, SK_SERVICE_ID, SERVICE_VERSION, BOOKING_VERSION, FILE_DT)
        (
            select SK_BOOKING_SERVICE_ID, SK_BOOKING_ID, SK_SERVICE_ID, SERVICE_VERSION, BOOKING_VERSION, FILE_DT
            from OPA_DEV.DBT_TEST.with_fl_acr_booking_service__dbt_tmp
        );
      
    