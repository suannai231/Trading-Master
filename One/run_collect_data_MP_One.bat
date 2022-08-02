call C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3
cd C:\Code\One
powercfg /setactive e9a42b02-d5df-448d-aa00-03f14749eb61
python collect_data_MP_One.py
powercfg /setactive a1841308-3541-4fab-bc81-f71556f20b4a
exit