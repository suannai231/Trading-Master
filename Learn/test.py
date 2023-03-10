
import openai
import os

openai.api_key = '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'

prompt ='''
Please explain what this call stack does:

19: kd> kC
 # Call Site
00 nt!KeBugCheckEx
01 watchdog!WdLogSingleEntry5
02 dxgmms2!VidSchiResetHwEngine
03 dxgmms2!VidSchiResetEngines
04 dxgmms2!VidSchiCheckHwProgress
05 dxgmms2!VidSchiScheduleCommandToRun
06 dxgmms2!VidSchiRun_PriorityTable
07 dxgmms2!VidSchiWorkerThread
08 nt!PspSystemThreadStartup
09 nt!KiStartSystemThread

'''


response = openai.Completion.create(
  engine="code-davinci-002",
  prompt=prompt,
  max_tokens=1024,
  n=1,
  stop=None,
  temperature=0.5
)

generated_text = response.choices[0].text.strip()

print(generated_text)
