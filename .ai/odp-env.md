
## Test service Endpoints: 

User: DEVELOPER
Password: :ABAPtr2023#00
Auth: Basic

### Example: Service Root:
```
http -a 'DEVELOPER:ABAPtr2023#00' 'http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_2_SRV/?$format=json'
```

Result:
```json
HTTP/1.1 200 OK
cache-control: no-store, no-cache, must-revalidate, max-age=0, post-check=0, pre-check=0
content-length: 57
content-type: application/json; charset=utf-8
dataserviceversion: 1.0
expires: Tue, 03 Jul 2001 06:00:00 GMT
last-modified: Sat, 30 Aug 2025 10:57:32 GMT
pragma: no-cache
sap-metadata-last-modified: Sat, 30 Aug 2025 10:57:32 GMT
sap-perf-fesrec: 39924.000000
sap-processing-info: ODataBEP=,crp=,RAL=,st=,MedCacheHub=Table,codeployed=X,softstate=
sap-server: true
set-cookie: sap-usercontext=sap-client=001; path=/
set-cookie: MYSAPSSO2=AjQxMDMBABhEAEUAVgBFAEwATwBQAEUAUgAgACAAIAACAAYwADAAMQADABBBADQASAAgACAAIAAgACAABAAYMgAwADIANQAwADgAMwAwADEAMQAxADYABQAEAAAACAYAAlgACQACRQD%2fAPswgfgGCSqGSIb3DQEHAqCB6jCB5wIBATELMAkGBSsOAwIaBQAwCwYJKoZIhvcNAQcBMYHHMIHEAgEBMBowDjEMMAoGA1UEAxMDQTRIAggKIBcCAhUJATAJBgUrDgMCGgUAoF0wGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAcBgkqhkiG9w0BCQUxDxcNMjUwODMwMTExNjM0WjAjBgkqhkiG9w0BCQQxFgQUxAEwnZ2Uh2EScTgnzBBX7bEAmLYwCQYHKoZIzjgEAwQuMCwCFGfWgqD7k%2fZ6H40DskKYRrLmRm%21RAhRvPHYzZyPrR55N7e7HN8AYLu5wCw%3d%3d; path=/; domain=localhost
set-cookie: SAP_SESSIONID_A4H_001=upBMhLMrVspCZn4yl2HWesmtztOFkhHwuKICQqwRAAI%3d; path=/

{
    "d": {
        "EntitySets": [
            "AttrOfSEPM_ISO",
            "AttrOfSEPM_ISOI"
        ]
    }
}
```

### Example: Orders
```
http -a 'DEVELOPER:ABAPtr2023#00' 'http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_2_SRV/AttrOfSEPM_ISO?$top=5&$format=json'
```
gives

```json
HTTP/1.1 200 OK
cache-control: no-store, no-cache
content-encoding: gzip
content-length: 730
content-type: application/json; charset=utf-8
dataserviceversion: 2.0
sap-metadata-last-modified: Sat, 30 Aug 2025 10:57:32 GMT
sap-perf-fesrec: 2577290.000000
sap-processing-info: ODataBEP=,crp=,RAL=,st=X,MedCacheHub=Table,MedCacheBEP=Table,codeployed=X,softstate=
sap-server: true
set-cookie: sap-usercontext=sap-client=001; path=/
set-cookie: MYSAPSSO2=AjQxMDMBABhEAEUAVgBFAEwATwBQAEUAUgAgACAAIAACAAYwADAAMQADABBBADQASAAgACAAIAAgACAABAAYMgAwADIANQAwADgAMwAwADEAMQAxADEABQAEAAAACAYAAlgACQACRQD%2fAPswgfgGCSqGSIb3DQEHAqCB6jCB5wIBATELMAkGBSsOAwIaBQAwCwYJKoZIhvcNAQcBMYHHMIHEAgEBMBowDjEMMAoGA1UEAxMDQTRIAggKIBcCAhUJATAJBgUrDgMCGgUAoF0wGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAcBgkqhkiG9w0BCQUxDxcNMjUwODMwMTExMTI1WjAjBgkqhkiG9w0BCQQxFgQU%21hVVqw7aVy8AUn%21fztGfEadfNFAwCQYHKoZIzjgEAwQuMCwCFDEpqMfP6EEHl1td%21kJWKTEMj%21q2AhRdDJfbWlfagvXwiL8RdWueES23Uw%3d%3d; path=/; domain=localhost
set-cookie: SAP_SESSIONID_A4H_001=gc_gbGXCcDbf8fqikX3zMBFEogmFkhHwuKICQqwRAAI%3d; path=/

{
    "d": {
        "results": [
            {
                "BILLTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D3BE5E",
                "CREATEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "CREATIONDATETIME": "20141231230000.0000000",
                "CUSTOMERCONTACTUUID": "0A71E5D7E6551EEAA8AD416995DD9E5E",
                "CUSTOMERUUID": "0A71E5D7E6551EEAA8AD416995DABE5E",
                "GROSSAMOUNTINTRANSACCURRENCY": "98.47",
                "ISCREATEDBYBUSINESSPARTNER": "",
                "ISLASTCHANGEDBYBUSINESSPARTNER": "",
                "LASTCHANGEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "LASTCHANGEDDATETIME": "20150107230000.0000000",
                "NETAMOUNTINTRANSACTIONCURRENCY": "82.75",
                "ODQ_CHANGEMODE": "",
                "ODQ_ENTITYCNTR": "0",
                "OPPORTUNITY": "",
                "SALESORDER": "0500000000",
                "SALESORDERBILLINGSTATUS": "P",
                "SALESORDERDELIVERYSTATUS": "D",
                "SALESORDERLIFECYCLESTATUS": "C",
                "SALESORDEROVERALLSTATUS": "P",
                "SALESORDERPAYMENTMETHOD": "",
                "SALESORDERPAYMENTTERMS": "",
                "SALESORDERUUID": "0A71E5D7E6551EEAA8AD42D1C8CE5E5F",
                "SHIPTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D3BE5E",
                "SHORTTEXTGROUPUUID": "0A71E5D7E6551EEAA8AD42CEBAE1DE5F",
                "TAXAMOUNTINTRANSACTIONCURRENCY": "15.72",
                "TRANSACTIONCURRENCY": "USD",
                "__metadata": {
                    "id": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CE5E5F')",
                    "type": "Z_ODP_DEMO_1_SRV.AttrOfSEPM_ISO",
                    "uri": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CE5E5F')"
                }
            },
            {
                "BILLTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D53E5E",
                "CREATEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "CREATIONDATETIME": "20141231230000.0000000",
                "CUSTOMERCONTACTUUID": "0A71E5D7E6551EEAA8AD416995DE9E5E",
                "CUSTOMERUUID": "0A71E5D7E6551EEAA8AD416995DB3E5E",
                "GROSSAMOUNTINTRANSACCURRENCY": "10.12",
                "ISCREATEDBYBUSINESSPARTNER": "",
                "ISLASTCHANGEDBYBUSINESSPARTNER": "",
                "LASTCHANGEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "LASTCHANGEDDATETIME": "20150107230000.0000000",
                "NETAMOUNTINTRANSACTIONCURRENCY": "8.50",
                "ODQ_CHANGEMODE": "",
                "ODQ_ENTITYCNTR": "0",
                "OPPORTUNITY": "",
                "SALESORDER": "0500000001",
                "SALESORDERBILLINGSTATUS": "P",
                "SALESORDERDELIVERYSTATUS": "D",
                "SALESORDERLIFECYCLESTATUS": "C",
                "SALESORDEROVERALLSTATUS": "P",
                "SALESORDERPAYMENTMETHOD": "",
                "SALESORDERPAYMENTTERMS": "",
                "SALESORDERUUID": "0A71E5D7E6551EEAA8AD42D1C8CE7E5F",
                "SHIPTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D53E5E",
                "SHORTTEXTGROUPUUID": "0A71E5D7E6551EEAA8AD42CEBAE1FE5F",
                "TAXAMOUNTINTRANSACTIONCURRENCY": "1.62",
                "TRANSACTIONCURRENCY": "USD",
                "__metadata": {
                    "id": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CE7E5F')",
                    "type": "Z_ODP_DEMO_1_SRV.AttrOfSEPM_ISO",
                    "uri": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CE7E5F')"
                }
            },
            {
                "BILLTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D35E5E",
                "CREATEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "CREATIONDATETIME": "20141231230000.0000000",
                "CUSTOMERCONTACTUUID": "0A71E5D7E6551EEAA8AD416995DD5E5E",
                "CUSTOMERUUID": "0A71E5D7E6551EEAA8AD416995DA9E5E",
                "GROSSAMOUNTINTRANSACCURRENCY": "29.51",
                "ISCREATEDBYBUSINESSPARTNER": "",
                "ISLASTCHANGEDBYBUSINESSPARTNER": "",
                "LASTCHANGEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "LASTCHANGEDDATETIME": "20150107230000.0000000",
                "NETAMOUNTINTRANSACTIONCURRENCY": "24.80",
                "ODQ_CHANGEMODE": "",
                "ODQ_ENTITYCNTR": "0",
                "OPPORTUNITY": "",
                "SALESORDER": "0500000002",
                "SALESORDERBILLINGSTATUS": "P",
                "SALESORDERDELIVERYSTATUS": "D",
                "SALESORDERLIFECYCLESTATUS": "C",
                "SALESORDEROVERALLSTATUS": "P",
                "SALESORDERPAYMENTMETHOD": "",
                "SALESORDERPAYMENTTERMS": "",
                "SALESORDERUUID": "0A71E5D7E6551EEAA8AD42D1C8CE9E5F",
                "SHIPTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D35E5E",
                "SHORTTEXTGROUPUUID": "0A71E5D7E6551EEAA8AD42CEBAE21E5F",
                "TAXAMOUNTINTRANSACTIONCURRENCY": "4.71",
                "TRANSACTIONCURRENCY": "USD",
                "__metadata": {
                    "id": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CE9E5F')",
                    "type": "Z_ODP_DEMO_1_SRV.AttrOfSEPM_ISO",
                    "uri": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CE9E5F')"
                }
            },
            {
                "BILLTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D3BE5E",
                "CREATEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "CREATIONDATETIME": "20141231230000.0000000",
                "CUSTOMERCONTACTUUID": "0A71E5D7E6551EEAA8AD416995DD9E5E",
                "CUSTOMERUUID": "0A71E5D7E6551EEAA8AD416995DABE5E",
                "GROSSAMOUNTINTRANSACCURRENCY": "136.73",
                "ISCREATEDBYBUSINESSPARTNER": "",
                "ISLASTCHANGEDBYBUSINESSPARTNER": "",
                "LASTCHANGEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "LASTCHANGEDDATETIME": "20150107230000.0000000",
                "NETAMOUNTINTRANSACTIONCURRENCY": "114.90",
                "ODQ_CHANGEMODE": "",
                "ODQ_ENTITYCNTR": "0",
                "OPPORTUNITY": "",
                "SALESORDER": "0500000003",
                "SALESORDERBILLINGSTATUS": "P",
                "SALESORDERDELIVERYSTATUS": "D",
                "SALESORDERLIFECYCLESTATUS": "C",
                "SALESORDEROVERALLSTATUS": "P",
                "SALESORDERPAYMENTMETHOD": "",
                "SALESORDERPAYMENTTERMS": "",
                "SALESORDERUUID": "0A71E5D7E6551EEAA8AD42D1C8CEBE5F",
                "SHIPTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D3BE5E",
                "SHORTTEXTGROUPUUID": "0A71E5D7E6551EEAA8AD42CEBAE23E5F",
                "TAXAMOUNTINTRANSACTIONCURRENCY": "21.83",
                "TRANSACTIONCURRENCY": "USD",
                "__metadata": {
                    "id": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CEBE5F')",
                    "type": "Z_ODP_DEMO_1_SRV.AttrOfSEPM_ISO",
                    "uri": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CEBE5F')"
                }
            },
            {
                "BILLTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D2FE5E",
                "CREATEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "CREATIONDATETIME": "20141231230000.0000000",
                "CUSTOMERCONTACTUUID": "0A71E5D7E6551EEAA8AD416995DD1E5E",
                "CUSTOMERUUID": "0A71E5D7E6551EEAA8AD416995DA7E5E",
                "GROSSAMOUNTINTRANSACCURRENCY": "21.06",
                "ISCREATEDBYBUSINESSPARTNER": "",
                "ISLASTCHANGEDBYBUSINESSPARTNER": "",
                "LASTCHANGEDBYUSER": "0A71E5D7E6551EEAA8AD416995C01E5E",
                "LASTCHANGEDDATETIME": "20150107230000.0000000",
                "NETAMOUNTINTRANSACTIONCURRENCY": "17.70",
                "ODQ_CHANGEMODE": "",
                "ODQ_ENTITYCNTR": "0",
                "OPPORTUNITY": "",
                "SALESORDER": "0500000004",
                "SALESORDERBILLINGSTATUS": "P",
                "SALESORDERDELIVERYSTATUS": "D",
                "SALESORDERLIFECYCLESTATUS": "C",
                "SALESORDEROVERALLSTATUS": "P",
                "SALESORDERPAYMENTMETHOD": "",
                "SALESORDERPAYMENTTERMS": "",
                "SALESORDERUUID": "0A71E5D7E6551EEAA8AD42D1C8CEDE5F",
                "SHIPTOPARTYADDRESSUUID": "0A71E5D7E6551EEAA8AD416995D2FE5E",
                "SHORTTEXTGROUPUUID": "0A71E5D7E6551EEAA8AD42CEBAE25E5F",
                "TAXAMOUNTINTRANSACTIONCURRENCY": "3.36",
                "TRANSACTIONCURRENCY": "USD",
                "__metadata": {
                    "id": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CEDE5F')",
                    "type": "Z_ODP_DEMO_1_SRV.AttrOfSEPM_ISO",
                    "uri": "http://localhost:50000/sap/opu/odata/sap/Z_ODP_DEMO_1_SRV/AttrOfSEPM_ISO('0A71E5D7E6551EEAA8AD42D1C8CEDE5F')"
                }
            }
        ]
    }
}

```