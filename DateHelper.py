class DateHelper(object):
    def convertDateValues(self, df):
        df['DateID']= df['FullDate'].dt.strftime('%Y%m%d')
        df['Day']= df['FullDate'].dt.strftime('%A')
        df['Month']= df['FullDate'].dt.strftime('%B')
        df['Year']= df['FullDate'].dt.strftime('%Y')
        df['DayOfYear']= df['FullDate'].dt.strftime('%w')
        df['DayOfWeek']= df['FullDate'].dt.strftime('%j')
        df['Quarter']= df['FullDate'].dt.quarter
        return df
