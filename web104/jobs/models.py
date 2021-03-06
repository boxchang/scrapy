import sys
sys.path.append("..")
from jobs.filters import ModelFilter


class JobModel(ModelFilter):
    custName = ""
    jobNo = ""
    jobName = ""
    description = ""
    history = ""
    tool = ""
    other = ""
    jobAddrNoDesc = ""
    addr = ""
    update_date = ""
    jobLink = ""

    def __init__(self, row):
        self.custName = row[0]
        self.jobNo = row[1]
        self.jobName = row[2]
        self.description = row[3]
        self.history = row[4]
        self.tool = row[5]
        self.other = row[6]
        self.jobAddrNoDesc = row[7]
        self.benefit = row[8]
        self.addr = row[9]
        self.update_date = row[10]
        self.jobLink = row[11]

    def validate(self):
        return self.data_filter(self)