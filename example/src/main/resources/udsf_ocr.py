
import easyocr
import cv2
import numpy as np

class UDSFOcr:
  def __init__(self):
    pass

  def transform(self, data, args, kvargs):
    res = self.buildHeader(data)
    print(type(data[0][0]), type(data[1][0]), data[0][0], data[0][1], data[1][0], data[1][1])
    ###
    sPos = 0
    # 第一列是key，跳过
    if data[0][0].lower()=="key":
        sPos = 1
    # 初始化图像存储空间
    binary_data = []
    for j in range(sPos,len(data[0])):
        binary_data.append(bytearray())
    
    # 组合图像
    for i in range(2, len(data)):
        for j in range(sPos,len(data[0])):
            binary_data[j-sPos] += data[i][j]

    # 拼接后的每一列数据转换为图像，并分别进行识别
    results = []
    reader = easyocr.Reader(['ch_sim','en']) # this needs to run only once to load the model into memory
    for j in range(sPos,len(data[0])):
        nparr = np.frombuffer(binary_data[j-sPos], np.uint8)
        img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        results.append(reader.readtext(img_np))

    # 整理识别结果，合并成最终结果
    pos = 0
    while True:
        row = []
        nCnt = 0;
        for result in results:
            if len(result)>pos:
                # bbox 是文本框的坐标（左, 上, 右, 下）;text 是识别的文本; prob 是识别的置信度
                row.append(result[pos][1].encode('gb2312')) #text
                row.append(float(result[pos][2])) #prob
            else:
                row.append("NULL")
                row.append(-1.0)
                nCnt += 1
        
        if len(data[0])-sPos==nCnt:# 所有列都取空了
            break
        else:#至少有一列不空
            res.append(row)
        pos+=1

    print(res)
    
    return res

  def buildHeader(self, data):
    colNames = []
    types = []
    for name in data[0]:
        if name.lower() != "key":
            colNames.append("ocrtext(text_"+name+")")
            colNames.append("ocrtext(prob_"+name+")")
            types.append("binary")
            types.append("double")
    return [colNames, types]
