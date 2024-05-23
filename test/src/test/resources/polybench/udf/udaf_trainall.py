import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
import time
from datetime import datetime

MAX_LOSS = 100.0  # 最大损失，用于判断梯度是否正确
EPOCHS = 1000  # 最大迭代次数
COLUMN_NAME = "trainall(trainall.key, trainall.label, trainall.lepton_pt, trainall.lepton_eta, trainall.lepton_phi, trainall.missing_energy_magnitude, trainall.missing_energy_phi, trainall.jet1_pt, trainall.jet1_eta, trainall.jet1_phi, trainall.jet1_b_tag, trainall.jet2_pt, trainall.jet2_eta, trainall.jet2_phi, trainall.jet2_b_tag, trainall.jet3_pt, trainall.jet3_eta, trainall.jet3_phi, trainall.jet3_b_tag, trainall.jet4_pt, trainall.jet4_eta, trainall.jet4_phi, trainall.jet4_b_tag, trainall.m_jj, trainall.m_jjj, trainall.m_lv, trainall.m_jlv, trainall.m_bb, trainall.m_wbb, trainall.m_wwbb)"
class UDAFtrainall:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        print('enter udf, time: ', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        res = self.buildHeader(data)
        # convert data[2:]to df
        df = pd.DataFrame(data[2:])
        # set df column index to data[0]
        df.columns = data[0]
        # print(df)
        # train
        print('before train, time: ', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        pred = sgd(df)
        print('end train, time: ', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        res.extend(pred.to_numpy().tolist())  # data[2:] is the data
        print('exit udf, time: ', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return res

    def buildHeader(self, data):
        colNames = ["key", COLUMN_NAME]
        return [colNames, ["LONG", "BINARY"]]  # data[1] is the type


def sgd(df: pd.DataFrame):
    start = time.time()
    # 转换为pandas.Dataframe
    #df['postgres.higgstrainall.label'] = df['postgres.higgstrainall.label'].apply(lambda x: x.decode('utf-8'))
    # 这里需要将最后一列中的's'和'b'分别转为1、0
    #df['postgres.higgstrainall.label'] = df['postgres.higgstrainall.label'].map({'s': 1, 'b': 0})
    # 这里要去掉'label'
    X = df.drop(['trainall.label'], axis=1)
    print(X.head())
    y = df['trainall.label']
    # 划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=100)

    # 特征缩放
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # 创建SGDClassifier模型
    sgd_classifier = SGDClassifier(loss='hinge', alpha=0.005, max_iter=1000, random_state=100)

    print(X_train_scaled.shape)
    print(y_train.shape)
    # print(y_train[:100])
    # # check nan
    # print(y_train.isnull().sum())
    # 训练模型
    sgd_classifier.fit(X_train_scaled, y_train)

    # 预测
    y_pred = sgd_classifier.predict(X_test_scaled)

    # 评估模型
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy}")
    # print(sgd_classifier.coef_.tolist()[0])
    end = time.time()
    print('Training time: ', end - start)
    # 存储最终得到的theta
    theta = (str(list(sgd_classifier.coef_.tolist()[0]))).encode('utf-8')
    model = pd.DataFrame({'key': [1], COLUMN_NAME: [theta]})
    # print(model)
    return model

