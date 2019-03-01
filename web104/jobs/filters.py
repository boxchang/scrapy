# coding: utf-8
class ModelFilter:
    keywords = ('big data', 'TOEIC', 'IELTS', 'MES', 'Kubenetes', 'Django') # 關鍵字
    companies = ('Tata',)
    years = 0 # 年資
    excludes = ('Contract',)

    def data_filter(self, data):
        result = False

        # 關鍵字過濾法
        for keyword in self.keywords:
            if str(data.description).upper().__contains__(keyword.upper()):
                result = True
            elif str(data.other).upper().__contains__(keyword.upper()):
                result = True

        # 公司過濾法
        for company in self.companies:
            if str(data.custName).upper().__contains__(company.upper()):
                result = True

        # 排除法
        for exclude in self.excludes:
            if str(data.custName).upper().__contains__(exclude.upper()):
                result = False

        return result
