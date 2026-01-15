```
Backend/
├── main_watchdog.py        # The "Brain": Constantly monitors for new files                      script
├── Drop_xlsx_here/         # The "Trigger": Put your raw Excel files here                        folder
├── Process/                                                                                      folder
│   ├── 1_layer.py          # parsing the entire one xlxs                      - outputs 1 csv    script
│   ├── 2_layer.py          # extracts the order&deals tables                  - outputs 2 csv    script
│   ├── 3_layer.py          # from 2 csv, merge into one                       - outputs 1 csv    script
│   ├── 4_layer.py          # only all red-rolling_28-metrics (MT5)            - outputs 1 csv    script
│   ├── 5_layer.py          # only all blue-rolling_13-metrics (non-MT5)       - outputs 1 csv    script
│   ├── 6_layer.py          # from 2 csv, all balance-based_41-metrics         - outputs 1 csv    script
│   ├── 7_layer.py          # only all orange (equity-based) rolling_metrics   - outputs 1 csv    script
│   ├── 8_layer.py          # from 1 csv, all equity-based_35metrics           - outputs 1 csv    script
│   └── 9_layer.py          # all balance & equity-based_76metrics             - outputs 1 csv    script
├── Output_csv_files/       # The "Result": Final processed data ends up here                     folder
├── Others/                                                                                       
│   ├── cells_dataframe_counter.py                                                                
│   ├── linechart_one_row_tester.py                                                               
│   └── PQI.py                                                                                         
└── workflow.md










QuasarVaultage
├── .venv/
├── .vscode/
├── backend/
│   ├── [1]_main_watchdog.py       # The "Brain": Constantly monitors for new files                     
│   ├── [2]_Drop_xlsx_here/        # The "Trigger": Put your raw Excel files here                        
│   ├── [3]_Process/                                                                                      
│   │   ├── 1_layer.py          # parsing the entire one xlxs                     
│   │   ├── 2_layer.py          # extracts the order&deals tables                
│   │   ├── 3_layer.py          # from 2 csv, merge into one                     
│   │   ├── 4_layer.py          # only all red-rolling_28-metrics (MT5)           
│   │   ├── 5_layer.py          # only all blue-rolling_13-metrics (non-MT5)       
│   │   ├── 6_layer.py          # from 2 csv, all balance-based_41-metrics         
│   │   ├── 7_layer.py          # only all orange (equity-based) rolling_metrics  
│   │   ├── 8_layer.py          # from 1 csv, all equity-based_35metrics          
│   │   └── 9_layer.py          # all balance & equity-based_76metrics             
│   ├── [4]_output_csv_files/   # The "Result": Final processed data ends up here      
│   │   ├── Upload-1_ID/            # This is where the parsed file for first uploaded file
│   │   |   ├── 1_layer_output.csv  # example file
│   │   |   ├── 2_layer_output.csv  # example file
│   │   |   ├── 3_layer_output.csv  # example file 
│   │   |   ├── 4_layer_output.csv  # example file
│   │   |   ├── 5_layer_output.csv  # example file
│   │   |   ├── 6_layer_output.csv  # example file
│   │   |   ├── 7_layer_output.csv  # example file 
│   │   |   ├── 8_layer_output.csv  # example file
│   │   |   └── 9_layer_output.csv  # example file
│       ├── Upload-2_ID/             # This is where the parsed file for second uploaded file
│       ├── Upload-3_ID/             # This is where the parsed file for third uploaded file
│       └── Upload-4_ID/            # and so on, the pattern goes infinite
├── database/                   # only stores metadata
├── authentication/             # mont secure authentication process which mainly include email and no password needed 
└── frontend/






QuasarVaultage
├── .venv/
├── .vscode/
├── backend/
│   ├── [1]_main_watchdog.py                      
│   ├── [2]_Drop_xlsx_here/             
│   ├── [3]_Process/                                                                                      
│   │   ├── 1_layer.py                     
│   │   ├── 2_layer.py                      
│   │   ├── 3_layer.py                           
│   │   ├── 4_layer.py                  
│   │   ├── 5_layer.py             
│   │   ├── 6_layer.py              
│   │   ├── 7_layer.py         
│   │   ├── 8_layer.py         
│   │   └── 9_layer.py          
│   └── [4]_output_csv_files/      
├── database/                  
├── authentication/            
└── frontend/         




