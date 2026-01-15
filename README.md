
# MT-Parsee
An `MT5-trade-report.xlsx` file parser constructs statistical graphs for clear visualization of the trading strategy performance. For demonstration, it handles 71,478 cells dataframe to generate 76 rolling trading metrics and graphs with [consideration](https://github.com/algorembrant/MTParsee/blob/main/metrics_consideration.pdf) of balance-based and equity-based calculations. Drop the xlsx file once — then let the bot do the rest.



```
├── backend/
│   ├── [1]_main_watchdog.py       # monitors and conduct automation                  
│   ├── [2]_Drop_xlsx_here/        # drog the xlsx file here                   
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
│   │   |   ├── 9_layer_output.csv  # example file
│   │   |   └──  visualize_results.py  # 76 plot maker in one tab
│   │   ├── Upload-2_ID/               # This is where the parsed file for second uploaded file
│   │   ├── Upload-3_ID/               # This is where the parsed file for third uploaded file
│   │   └── Upload-4_ID/               # and so on, the pattern goes infinite

```


## Very simple steps

- install python requirements <br>
- run watchdog.py once<br>
- for demonstration, drag-n-drop MT5-trade-report xlsx file into [2] folder
- let the bot do the rest


## All 76 Trading metrics

![file](https://github.com/algorembrant/MTPRS/blob/main/newplot.png)

## Citation

```bibtex
@misc{MTParsee,
  author = {Albeos, Rembrant},
  title = {{MTParsee}},
  year = {2026},
  url = {https://github.com/algorembrant/MTParsee},
  note = {GitHub repository}
}
```

