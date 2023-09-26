import pandas as pd
from ODS import ODS
from datetime import date
from DateHelper import DateHelper
import pyodbc

class ParseCSV(object):
    def __init__(self):
        print("Building ParseCSV via Constructor")

        self.sales_df = pd.read_csv("SalesCSV.csv", encoding="ISO-8859-1")

        connectionString = "DRIVER={SQL Server};SERVER=sql2016.fse.network;DATABASE=db_2013750_co5222_assignmentdb;UID=user_db_2013750_co5222_assignmentdb;PWD=P@55word"
        self.conn = pyodbc.connect(connectionString)
    
    def parseCSV(self):
        print("Parsing CSV files")
        self.parseStoreSales()
        self.parseDates()
        self.parseStaff()

    def parseStoreSales(self):
        print("\t Parsing CSV Sales")
        storeSales_df = pd.DataFrame(self.sales_df)
        storeSales_df['FullDate']= pd.to_datetime(storeSales_df['date'])
        storeSales_df['DateID']= storeSales_df['FullDate'].dt.strftime('%Y%m%d')
        storeSales_df[['FirstName','LastName']] = storeSales_df.employee.str.split(" ", expand=True)
        storeSales_df['ProductDescription'] = storeSales_df['item']
        storeSales_df['Quantity']=storeSales_df['quantity']
        storeSales_df = pd.merge(storeSales_df,ODS.DimProduct_df[['ProductID', 'ProductDescription','CategoryID','SupplierPrice','ProductPrice']], left_on='ProductDescription',right_on='ProductDescription', how='left')
        employeeSQL_df=pd.read_sql_query("Select EmployeeID, FirstName, LastName, StoreID FROM Employee", self.conn)
        storeSales_df =pd.merge(storeSales_df,employeeSQL_df[['EmployeeID','FirstName','LastName','StoreID']],left_on=['FirstName','LastName'],right_on=['FirstName','LastName'], how ='left')
        storeSales_df =pd.merge(storeSales_df,ODS.DimStore_df[['StoreID','PostalCode']],left_on='StoreID',right_on='StoreID', how ='left')
        storeSales_df['LocationID']=storeSales_df['PostalCode']
        storeSales_df.index += len(ODS.FactStoreSales_df)
        storeSales_df['InvoiceID'] = "CSV_" + storeSales_df.index.astype('string')
        storeSales_df['StaffID']=storeSales_df['EmployeeID']
        storeSales_df ['SubTotal'] = storeSales_df['ProductPrice'] * storeSales_df['Quantity']
        storeSales_df ['SalesTax'] = storeSales_df['SubTotal'] * .06 
        storeSales_df ['Profit']= storeSales_df['ProductPrice'] - storeSales_df['SupplierPrice'] 
        storeSales_df ['SaleProfit'] = storeSales_df['Quantity'] * storeSales_df['Profit'] 
        storeSales_df=storeSales_df.drop(columns=['FirstName','LastName','ProductDescription','CategoryID','PostalCode','EmployeeID','SupplierPrice','ProductPrice','Profit','date','sale','FullDate','item','quantity','total','employee'])
        ODS.FactStoreSales_df = ODS.FactStoreSales_df.append(storeSales_df)

    def parseDates(self):
        print("\t Parsing CSV Dates")
        dates_df = pd.DataFrame(self.sales_df['date'])
        dates_df['FullDate']= pd.to_datetime(dates_df['date'])
        dates_df = DateHelper.convertDateValues(self,dates_df)
        dates_df=dates_df.drop(columns=['date'])
        ODS.DimDate_df = ODS.DimDate_df.append(dates_df)
        ODS.DimDate_df.drop_duplicates(subset="DateID", keep="first",inplace=True)

    def parseStaff(self):
        print("\t Parsing CSV Employees")
        employee_df= pd.DataFrame(self.sales_df['employee'])
        employee_df[['FirstName','LastName']] = employee_df.employee.str.split(" ", expand=True)
        employee_df.drop_duplicates(subset="employee", keep="first",inplace=True)
        employee_df = employee_df.drop(columns=['employee'])
        ODS.DimStaff_df = ODS.DimStaff_df.append(employee_df)
        ODS.DimStaff_df.drop_duplicates(subset=['FirstName','LastName'], keep="first",inplace=True)
