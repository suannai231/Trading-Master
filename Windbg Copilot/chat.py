import pykd
pykd.loadDump("C:\dump\cmd.dmp")

# pykd.execute("!analyze -v")

# Print the contents of the current thread's stack
for frame in pykd.getCurrentThread().getFrames():
    print(frame)

# Print the value of the EAX register
print(pykd.reg("eax"))

# Read a memory address and print its contents
address = pykd.ptrPtr(pykd.reg("ebp") + 4)
print(pykd.loadChars(address, 16))

pykd.quit()
