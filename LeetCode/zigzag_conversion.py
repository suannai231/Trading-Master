def convert(s, numRows):
    if numRows == 1 or numRows >= len(s):
        return s

    L = [''] * numRows
    index, step = 0, 1

    for x in s:
        L[index] += x
        if index == 0:
            step = 1
        elif index == numRows -1:
            step = -1
        index += step

    return ''.join(L)

converted_string = convert("PAYPALISHIRING", 3)
print(converted_string)

# class Solution:
#     def convert(self, s: str, numRows: int) -> str:
#         if numRows == 1:
#             return s

#         n = len(s)
#         sections = ceil(n/(2*numRows-2))
#         numCols = sections*(numRows-1)
#         matrix = [[' ']*numCols for _ in range(numRows)]
#         currRow,currCol = 0,0
#         curr_string_index = 0

#         while curr_string_index < n:
#             while currRow < numRows and curr_string_index<n:
#                 matrix[currRow][currCol] = s[curr_string_index]
#                 currRow+=1
#                 curr_string_index+=1
#             currRow-=2
#             currCol+=1

#             while currRow>0 and currCol < numCols and curr_string_index < n:
#                 matrix[currRow][currCol] = s[curr_string_index]
#                 currRow-=1
#                 currCol+=1
#                 curr_string_index+=1
#         answer = ""
#         for row in matrix:
#             answer+="".join(row)
#         return answer.replace(" ","")