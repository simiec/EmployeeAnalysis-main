import sqlite3 as sl
from sqlite3 import Error
import math

from dbCodes.extract_keyword import KeywordExtractor

class TaskData:
    def __init__(self):
        self.company_id = 0
        self.task_code = 0
        self.task_giver = None
        self.task_reciever = None
        self.entry_title = None
        self.entry_description = None

class EntryAnalysis:
    def __init__(self, task_data, dbName_="employee_analysis"):
        self.conn = None
        self.cursor = None
        self.dbName = dbName_ + ".db"

        self.FACTOR_COUNT = 7
        
        self.task_data = task_data
        self.title_list = []
        self.description_list = []
        self.fillKeywordList()
        
        self.scoring_list = [] # Datatype [ employee#1 info, employee#2 info, ... ]
        self.final_score = []
        self.final_score_named = []
        self.max_id = self.getEmployeeNumber()
        for i in range(self.max_id):
            self.final_score_named.append([])
            self.scoring_list.append([])
            self.final_score.append([])
            for j in range(self.FACTOR_COUNT):
                self.scoring_list[i].append(0)
                                                        # employee#i info: [
                                                        # 0  title match employee factor (title), 
                                                        # 1  title match employee factor (description),
                                                        # 2  title match time factor (avarage_time),
                                                        # 3  description match employee factor (title),
                                                        # 4  description match employee factor (description),
                                                        # 5  description match time factor (avarage_time),
                                                        # 6  salary_factor
                                                        #  ]
                                                        #   
                                                        #
                                                        #
                                                        #
                                                        #
                                                        #
                                                        #
                                                        #
                                                        #

        
    def databaseConnector(self):
        try:
            self.conn = sl.connect(self.dbName)
            self.cursor = self.conn.cursor()
        except Error as e:
            print(e)
    
    def closeDB(self):
        self.conn.close()
     
    def getEmployeeNumber(self):
        self.databaseConnector()
        self.cursor.execute('''SELECT max(ID) FROM EMPLOYEE_ID_TABLE''')
        max_id = self.cursor.fetchone()[0]    
        self.closeDB()
        return max_id
    def fillKeywordList(self):
        extractor = KeywordExtractor(self.task_data.entry_title)
        self.title_list = extractor.extractKeywordList()
        
        extractor = KeywordExtractor(self.task_data.entry_description)
        self.description_list = extractor.extractKeywordList()
        
    def employeeFactorCalculator_(self, keyword, title_multiplier, description_multiplier, match_recorder, isTitle_):

        self.cursor.execute('''SELECT * FROM KEYWORD_RECORDER_TABLE WHERE COMPANY_ID = ? AND KEYWORD = ?''',(self.task_data.company_id ,keyword))
        keyword_record = self.cursor.fetchone()
        
        if(keyword_record == None):
            return
        
        keyword_id = keyword_record[1]
        
        self.cursor.execute('''SELECT * FROM EMPLOYEE_FACTOR_TABLE WHERE COMPANY_ID = ? AND KEYWORD_ID = ?''',(self.task_data.company_id , keyword_id))
        employee_factor_records = self.cursor.fetchall()    
        
        # Set Min & Max Vals
        minimum_avarage_time = 0
        maximum_avarage_time = 0

        minimum_title_count = 0
        maximum_title_count = 0

        minimum_description_count = 0
        maximum_description_count = 0



        try: minimum_avarage_time = employee_factor_records[0][6]
        except: pass

        try: minimum_title_count = employee_factor_records[0][3]
        except: pass

        try: minimum_description_count = employee_factor_records[0][4]
        except: pass

        for val_calc in employee_factor_records: 
            minimum_avarage_time = min(minimum_avarage_time, val_calc[6])
            maximum_avarage_time = max(maximum_avarage_time, val_calc[6])

            minimum_title_count = min(minimum_title_count, val_calc[3])
            maximum_title_count = max(maximum_title_count, val_calc[3])

            minimum_description_count = min(minimum_description_count, val_calc[4])
            maximum_description_count = max(maximum_description_count, val_calc[4])

        for record in employee_factor_records:

            employee_id         = record[2] # EMPLOYEE_ID
            title_count         = record[3] # UCT
            description_count   = record[4] # UCD 
            avarage_time_spend  = record[6] # AVARAGE_HOURS

            try:
                avarage_score = 1 - ( ( avarage_time_spend - minimum_avarage_time ) / ( maximum_avarage_time - minimum_avarage_time ) )
                avarage_title = 1 - ( ( title_count - minimum_title_count ) / ( maximum_title_count - minimum_title_count ) ) 
                avarage_description = 1 - ( ( description_count - minimum_description_count ) / ( maximum_description_count - minimum_description_count ) )
            except: 
                avarage_score = 1
                avarage_title = 1
                avarage_description = 1

            if(isTitle_):
                self.scoring_list[employee_id][0] += title_multiplier * avarage_title
                self.scoring_list[employee_id][1] += title_multiplier * avarage_description                 
                self.scoring_list[employee_id][2] += title_multiplier * avarage_score
            else:
                self.scoring_list[employee_id][3] += description_multiplier * avarage_title
                self.scoring_list[employee_id][4] += description_multiplier * avarage_description                 
                self.scoring_list[employee_id][5] += description_multiplier * avarage_score

            match_recorder[employee_id] += 1
    
    def employeeFactor(self,title_multiplier, description_multiplier):
        
        matching_keyword_multiplier_count_title = []
        matching_keyword_multiplier_count_description = []
        for i in range(self.max_id):
            matching_keyword_multiplier_count_title.append(0)
            matching_keyword_multiplier_count_description.append(0)

        for title in self.title_list:
            self.employeeFactorCalculator_(title, title_multiplier, description_multiplier, matching_keyword_multiplier_count_title, isTitle_= True)
        
        for description in self.description_list:
            self.employeeFactorCalculator_(description, title_multiplier, description_multiplier, matching_keyword_multiplier_count_description, isTitle_= False)

        for i in range(self.max_id):
            multiplier_title = 2 * ( (matching_keyword_multiplier_count_title[i] / 8) / math.sqrt( (matching_keyword_multiplier_count_title[i] / 8 ) + 1) ) + 1
            multiplier_description = 2 * ( (matching_keyword_multiplier_count_description[i] / 8) / math.sqrt( (matching_keyword_multiplier_count_description[i] / 8 ) + 1) ) + 1
            
            self.scoring_list[i][0] *= multiplier_title
            self.scoring_list[i][1] *= multiplier_title
            self.scoring_list[i][2] *= multiplier_title

            self.scoring_list[i][3] *= multiplier_description
            self.scoring_list[i][4] *= multiplier_description
            self.scoring_list[i][5] *= multiplier_description
    
    def salaryFactor(self, salary_multiplier):


        self.cursor.execute('''SELECT * FROM EMPLOYEE_ID_TABLE WHERE COMPANY_ID = ?''',(self.task_data.company_id,))
        employees_info = self.cursor.fetchall()

        minimum_salary = 0
        maximum_salary = 0

        try: minimum_salary = employees_info[0][4]
        except: pass
        
        for i, employee_info in enumerate(employees_info):
            minimum_salary = min(minimum_salary, employee_info[4])
            maximum_salary = max(maximum_salary, employee_info[4])

        for i ,employee_info in enumerate(employees_info):
            self.scoring_list[i][6] = 1 - ( ( employee_info[4] - minimum_salary) / (maximum_salary - minimum_salary) ) 
            self.scoring_list[i][6] *= salary_multiplier

    def scoreCalculator(self, factorMultiplier):
        
        for i in range(self.max_id):
            sumScore = 0
            for j in range(self.FACTOR_COUNT):
                sumScore += self.scoring_list[i][j] * factorMultiplier[j]
            
            self.final_score[i] = [i, sumScore]


        for i in range(len(self.final_score)-1):
            for j in range(0, len(self.final_score)- i - 1):
                if(self.final_score[j][1] < self.final_score[j+1][1]):
                    swapped = True
                    self.final_score[j], self.final_score[j+1] = self.final_score[j+1], self.final_score[j]

        
        for i, final_score in enumerate(self.final_score):
            self.cursor.execute('''SELECT * FROM EMPLOYEE_ID_TABLE WHERE COMPANY_ID = ? AND ID = ?''',(self.task_data.company_id, final_score[0] + 1))
            person_data =  self.cursor.fetchone()
            self.final_score_named[i] = [ person_data[2], final_score[1] ]

    
def main():

    factorMultiplier = [1,1,1,1,1,1,1]

    title_multiplier = 1.0            # AI input for employee factor
    description_multiplier = 1.0      # AI input for employee factor
    salary_multiplier = 1.0           # AI input for salary factor

    task_data = TaskData()
    
    task_data.company_id = 3
    task_data.task_code = 45
    task_data.task_giver = 1
    task_data.task_reciever = 6
    task_data.entry_title = "İHA tabanlı savunma sistemi"
    task_data.entry_description = "ARM tabanlı mimari ile İHA savunma sisteminin yerleştirilmesi"
    
    analysis_object = EntryAnalysis(task_data, dbName_ = "employee_analysis")
    analysis_object.databaseConnector()
    analysis_object.employeeFactor(title_multiplier, description_multiplier)
    analysis_object.salaryFactor(salary_multiplier)
    analysis_object.scoreCalculator(factorMultiplier)
    analysis_object.closeDB()

    for i in analysis_object.final_score_named:
        print(i)
    
        
if __name__ == "__main__":
    main()
    