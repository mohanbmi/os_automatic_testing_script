import pymongo
from pymongo import MongoClient
import urllib.parse
import pprint
import os
import pandas as pd
from collections import defaultdict
from seqeval.metrics.sequence_labeling import get_entities
import numpy as np
from tqdm import tqdm
import json
import re
import xlwt
import difflib, operator, ast
from copy import copy, deepcopy
import yaml


class json_compare:

    # Initializing Variables
    def __init__(self):
        self.Q_id = []
        self.inst = []
        self.que = []
        self.qty = []
        self.cl = []
        self.app = []
        self.ans = []
        self.order = []

        self.Q_id2 = []
        self.inst2 = []
        self.que2 = []
        self.qty2 = []
        self.cl2 = []
        self.app2 = []
        self.ans2 = []
        self.order2 = []
        self.confidence_level = 0

    # Method to clean the received text
    def declean(self, sent):
        sent = sent.replace(' , ', ', ')
        sent = sent.replace(' ?', '?')
        sent = sent.replace(' ? ', '?')
        sent = sent.replace(' .', '.')
        sent = sent.replace(' . ', '.')
        sent = sent.replace(' / ', '/')
        sent = sent.replace("' '", " ")
        sent = sent.replace("'", " ")
        return sent

    def deep_clean(self, sentence):
        sentence = re.sub(r'[^\w\s]', '', sentence)
        string = ''.join(sentence.split())
        return sentence

    def declean_QID(self,sent):
        if not isinstance(sent, str):
            sent = str(sent)
        sent = sent.replace(' , ', ', ')
        sent = sent.replace(' .', '.')
        sent = sent.replace('. ', '.')
        sent = sent.replace(' . ', '.')
        sent = sent.replace(' . ', '.')
        sent = sent.replace(' / ', '/')
        sent = sent.replace("' '", " ")
        sent = sent.replace("'", " ")
        return sent

    def mongo_client(self):
        config_values = yaml.safe_load(open(os.path.join(os.getcwd(), "config.yml")))
        mongo_uri = "mongodb://admin:" + urllib.parse.quote(config_values['password']) + "@159.89.32.62:27017"
        print('Connecting to the Database')
        client = MongoClient(mongo_uri)  # server.local_bind_port is assigned local port
        mydb = client.blueOcean_shapiroraj
        try:
            mydb.command("serverStatus")
        except Exception as e:
            print(e)
        else:
            print("You are connected!")
        db_contents = mydb["projects"]
        return db_contents

    def extract_json_db(self, base_jsn, pred_jsn):
        myquery_base = {'projectName': base_jsn}
        myquery_pred = {'projectName': pred_jsn}
        db_schema = self.mongo_client()

        EditedDocs = db_schema.find(myquery_base)
        for value, document in enumerate(EditedDocs, 1):
            data1 = document
            gen = data1.get('genericJSON')
            survey = gen.get('Survey')
            total_count = 0
            high_count = 0
            for item in survey:
                self.Q_id2.append(item['Q_ID'])
                self.inst2.append(item['Q_Instruction'])
                self.que2.append(item['Q_Text'])
                self.qty2.append(item['Q_Type'])
                self.cl2.append(item['Confidence_Level'])

                #app.append(item['selectedAppearance'])
                Ans = item['Answers']
                if len(Ans) > 0:
                    ans_list = Ans['Answer_List']
                    full_ans = []
                    for i in ans_list:
                        text = i['O_Text']
                        code = i['Code']
                        full_ans.append(text)
                    self.ans2.append(full_ans)
                else:
                    self.ans2.append('')
                prop = item['Q_Properties']
                sort = prop['Answer_List_Sorting_Order']
                self.order2.append(sort)
                total_count += 1
                if item['Confidence_Level'] == 'HIGH':
                    high_count += 1
            self.confidence_level = high_count/total_count
            # print("The Confidence Level Accuracy is :", high_count/total_count)


        EditedDocs = db_schema.find(myquery_pred)
        for value, document in enumerate(EditedDocs, 1):
            data2 = document
            gen = data2.get('genericJSON')
            survey = gen.get('Survey')
            for item in survey:
                self.Q_id.append(item['Q_ID'])
                self.inst.append(item['Q_Instruction'])
                self.que.append(item['Q_Text'])
                self.qty.append(item['Q_Type'])
                self.cl.append(item['Confidence_Level'])



                #app.append(item['selectedAppearance'])
                Ans = item['Answers']
                if len(Ans) > 0:
                    ans_list = Ans['Answer_List']
                    full_ans = []
                    for i in ans_list:
                        text = i['O_Text']
                        code = i['Code']
                        full_ans.append(text)
                    self.ans.append(full_ans)
                else:
                    self.ans.append('')
                prop = item['Q_Properties']
                sort = prop['Answer_List_Sorting_Order']
                self.order.append(sort)

        self.df1_dict = {'QID': self.Q_id, 'QUESTION': self.que, 'INSTRUCTION': self.inst, 'ANSWERS': self.ans,'Q_TYPE': self.qty, 'CONFIDENCE_LEVEL': self.confidence_level, 'ORDER': self.order}
        self.dataFrame1 = pd.DataFrame(self.df1_dict)
        self.df2_dict = {'QID': self.Q_id2, 'QUESTION': self.que2, 'INSTRUCTION': self.inst2, 'ANSWERS': self.ans2,'Q_TYPE': self.qty2, 'CONFIDENCE_LEVEL': self.confidence_level, 'ORDER': self.order2}
        self.dataFrame2 = pd.DataFrame(self.df2_dict)

        # print(self.dataFrame1)
        # print("_______________")
        # print(self.dataFrame2)
        return self.dataFrame1, self.dataFrame2

    def compare_final(self,excel_file_act, excel_file_pred):

        cols = ['QID', 'QUESTION', 'INSTRUCTION', 'ANSWERS', 'Q_TYPE', 'CONFIDENCE_LEVEL', 'ORDER']
        num_cols = len(cols)
        act_df = excel_file_act
        act_df = act_df.reset_index(drop=True)
        act_df['QUESTION'] = act_df['QUESTION'].apply(self.declean)

        act_df['QID'] = act_df['QID'].apply(self.declean_QID)
        act_df = act_df[pd.notnull(act_df['QUESTION'])]
        act_df = act_df[cols]

        pred_df = excel_file_pred
        pred_df = pred_df.reset_index(drop=True)
        pred_df['QUESTION'] = pred_df['QUESTION'].apply(self.declean)
        pred_df = pred_df[pd.notnull(pred_df['QUESTION'])]
        pred_df = pred_df[cols]

        match_df = pd.DataFrame(columns=cols)

        for i in range(len(act_df)):
            a_qt = act_df.loc[i, 'QUESTION']
            match_qt = []
            for j in range(len(pred_df)):
                match_qt.append(difflib.SequenceMatcher(None, a_qt, pred_df.loc[j, 'QUESTION']).ratio())
            max_index, max_value = max(enumerate(match_qt), key=operator.itemgetter(1))
            match_df = match_df.append(pred_df.loc[max_index, :])
            pred_df.drop(pred_df.index[max_index])
            pred_df = pred_df.reset_index(drop=True)

        mask_frame = np.zeros(act_df.shape)
        match_df = match_df.reset_index()
        #     match_df.to_excel(r'/home/prashanth/Documents/OS/match.xlsx')

        for i in range(len(act_df)):
            for j in range(len(cols)):
                if (cols[j] == 'QID' or cols[j] == 'QUESTION'):
                    if np.any((pd.isnull(act_df.loc[i, cols[j]]) or act_df.loc[i, cols[j]] == np.nan or act_df.loc[
                        i, cols[j]] == '[]' or (act_df.loc[i, cols[j]] == 'nan'))):
                        mask_frame[i, j] = 0.0
                    else:
                        if (self.deep_clean(str(act_df.loc[i, cols[j]])) == self.deep_clean(str(match_df.loc[i, cols[j]]))):
                            mask_frame[i, j] = 1.0
                        else:
                            mask_frame[i, j] = -1.0
                else:
                    #                 if np.any(((pd.isnull(act_df.loc[i, cols[j]]): or (act_df.loc[i, cols[j]] == 'nan') or act_df.loc[i, cols[j]] == '[]'))):
                    if np.any(pd.isnull(act_df.loc[i, cols[j]])):
                        mask_frame[i, j] = 0.0
                    elif act_df.loc[i, cols[j]] == 'nan':
                        mask_frame[i, j] = 0.0
                    elif act_df.loc[i, cols[j]] == '[]':
                        mask_frame[i, j] = 0.0

                    else:
                        a_str = []
                        p_str = []
                        a_str = act_df.loc[i, cols[j]]
                        if (match_df.loc[i, cols[j]] != 'notanumber'):
                            p_str = match_df.loc[i, cols[j]]
                        else:
                            p_str = match_df.loc[i, cols[j]]
                        if (self.deep_clean(str(a_str)) == self.deep_clean(str(p_str))):
                            mask_frame[i, j] = 1.0
                        else:
                            mask_frame[i, j] = -1.0
        # Accuracy Measure
        unique, counts = np.unique(mask_frame, return_counts=True)
        dict_count = dict(zip(unique, counts))
        meas_acc = deepcopy(mask_frame)
        #     print(meas_acc)
        meas_acc[meas_acc == 0.0] = 1.0
        meas_acc[meas_acc == -1.0] = 0.0
        #meas_acc = self.confidence_level
        acc_class = np.mean(meas_acc, axis=0)
        Correct_Q_count = meas_acc.sum(axis=1)
        Correct_Q_count[Correct_Q_count != num_cols] = 0
        Correct_Q_count[Correct_Q_count == num_cols] = 1
        Correct_Q = Correct_Q_count.sum()
        Total_Q_count = meas_acc.shape[0]
        acc_dict = dict()
        for j in range(acc_class.shape[0]):
            acc_dict[cols[j]] = format(acc_class[j], '.2f')
        accuracy = np.mean(meas_acc)
        return accuracy, acc_dict, dict_count, Correct_Q, Total_Q_count, act_df, match_df


    def int_conversion(self,s):
        try:
            i = int(s)
        except ValueError as verr:
            i = ''
            pass
        except Exception as ex:
            i = ''
            pass
        return i

    def _get_a_precode(self, ans):
            ans_list = ans.split()
            if len(ans_list) > 1:
                x = self.int_conversion(ans_list[-1])
                if str(ans_list[1]) == str('.'):
                    ans_txt = ' '.join(ans_list[2:])
                elif x:
                    ans_txt = ' '.join(ans_list[:-1])
                else:
                    ans_txt = ' '.join(ans_list)
            else:
                ans_txt = ' '.join(ans_list)
            ans_txt = "'{0}'".format(ans_txt)
            return ans_txt.strip()

    def get_accuracy_report(self, base_json, pred_json, index):
        df1, df2 = self.extract_json_db(base_json, pred_json)
        df1[['INSTRUCTION', 'ANSWERS']] = df1[['INSTRUCTION', 'ANSWERS']].fillna(value='[]')
        df2[['INSTRUCTION', 'ANSWERS']] = df2[['INSTRUCTION', 'ANSWERS']].fillna(value='[]')
        for j in range(len(df1)):
            df1.at[j, 'ANSWERS'] = [self._get_a_precode(str(x)).strip() for x in df1.loc[j, 'ANSWERS']]
        for j in range(len(df2)):
            df2.at[j, 'ANSWERS'] = [self._get_a_precode(str(x)).strip() for x in df2.loc[j, 'ANSWERS']]

        acc, class_acc, a, Correct_Q, Total_Q_count, act_df, match_df = self.compare_final(df1, df2)
        class_acc = dict([a, float(x)] for a, x in class_acc.items())

        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print("DOCUMENT BEING EVALUATED: ", base_json_name)
        #print('SEGEMENT ACCURACY : %.2f' % acc)
        print('SEGMENT  :', class_acc)
        print('Confidence Level :', self.confidence_level)
        print('Correct_Q_count : ', int(Correct_Q))
        print('Total_Q_count   : ', Total_Q_count)
        print('QUESTION ACCURACY : %.2f' % (Correct_Q / Total_Q_count))
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

        correct_question = int(Correct_Q)
        #data_to_excel = [{'Document Name':base_json_name, 'Segment Accuracy':acc, "Total Question count":Total_Q_count,"Correct_question_count":correct_question,"Question Accuracy":(Correct_Q / Total_Q_count)}]
        data_to_excel = [{'Document Name': base_json_name,  "Total Question count": Total_Q_count, "Confidence Level":self.confidence_level, "Correct_question_count": correct_question, "Question Accuracy": (Correct_Q / Total_Q_count)}]
        data_formatted = pd.DataFrame(data_to_excel)

        if index == 0:
            with open('Accuracy_Report.csv', 'w+') as f:
                data_formatted.to_csv(f, sep='\t')
        else:
            # with open('Accuracy_Report.csv', 'a') as f:
            #     data_formatted.to_csv(f, header=False)
            data_formatted.to_csv('Accuracy_Report.csv', mode='a', sep='\t',header=False)


#json_cmp = json_compare()

data = pd.read_excel('Ground_Truth_validity.xlsx')
base_list = data['Base']
pred_list = data['Pred']

for i in range(len(base_list)):
    json_cmp = json_compare()
    base_json_name = base_list[i]
    pred_json_name = pred_list[i]
    json_cmp.get_accuracy_report(base_json_name, pred_json_name, i)


# base_json_name = 'MenuBoardBase'
# pred_json_name = 'MenuBoardPred'
# json_cmp.get_accuracy_report(base_json_name, pred_json_name)
#
# FrontierBase
# FrontierPred
#
# MenuBoardBase
# MenuBoardPred


