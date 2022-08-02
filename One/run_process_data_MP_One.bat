set CUR_YYYY=%date:~10,4%
set CUR_MM=%date:~4,2%
set CUR_DD=%date:~7,2%
set CUR_HH=%time:~0,2%
if %CUR_HH% lss 10 (set CUR_HH=0%time:~1,1%)
set CUR_NN=%time:~3,2%
set CUR_SS=%time:~6,2%
set CUR_MS=%time:~9,2%
set FILENAME=%CUR_YYYY%%CUR_MM%%CUR_DD%-%CUR_HH%%CUR_NN%%CUR_SS%

call C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3
cd C:\Code\One
powercfg /setactive e9a42b02-d5df-448d-aa00-03f14749eb61
python process_data_MP_One.py>\\jack-nas\Work\Python\ProcessedData\%FILENAME%.txt
powercfg /setactive a1841308-3541-4fab-bc81-f71556f20b4a
exit