class ModelFilter:
    keywords = ('MES', 'C#', 'Python') # 關鍵字
    companies = ('Tata')
    years = 0 # 年資

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


        # 年資過濾法 (未完成，因為年資沒有固定格式)
        if self.years > 0:
            if self.years == 1:
                if str(data.history).__contains__('一'):
                    result = True
            if self.years == 2:
                if str(data.history).__contains__('一', '二'):
                    result = True
            if self.years == 3:
                if str(data.history).__contains__('一', '二', '三',):
                    result = True
            if self.years == 4:
                if str(data.history).__contains__('一', '二', '三', '四',):
                    result = True
            if self.years == 5:
                if str(data.history).__contains__('一', '二', '三', '四', '五',):
                    result = True
            if self.years == 6:
                if str(data.history).__contains__('一', '二', '三', '四', '五', '六'):
                    result = True

        return result
