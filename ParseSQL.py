import pyodbc
import pandas as pd
from ODS import ODS
import numpy as np
from datetime import date
from DateHelper import DateHelper
from ProfitHelper import ProfitHelper

class ParseSQL():
    def __init__(self):
        print("Building ParseSQL via Constructor")
        connectionString = "DRIVER={SQL Server};SERVER=sql2016.fse.network;DATABASE=db_2013750_co5222_assignmentdb;UID=user_db_2013750_co5222_assignmentdb;PWD=P@55word"
        self.conn = pyodbc.connect(connectionString)
        self.cursor = self.conn.cursor()

    def parseSQL(self):
        print("Parsing SQL")
        self.parseDates()
        self.parseLocations()
        self.parseStores()
        self.parseEmployees()
        self.parseProducts()
        self.parseCustomers()
        self.parseFactStore()
        self.parseFactInternet()

    def parseDates(self):
        print("\t Parsing SQL Dates")
        dates_df=pd.read_sql_query("SELECT DateOfSale FROM InternetSale",self.conn)
        datesStore_df=pd.read_sql_query("SELECT DateOfSale FROM StoreSale",self.conn)
        dates_df =dates_df.append(datesStore_df)
        dates_df['FullDate']=pd.to_datetime(dates_df['DateOfSale'])
        dates_df = DateHelper.convertDateValues(self,dates_df)
        dates_df = dates_df.drop(columns=['DateOfSale'])
        ODS.DimDate_df = ODS.DimDate_df.append(dates_df)
        ODS.DimDate_df.drop_duplicates(subset='DateID',keep='first',inplace=True)

    def parseLocations(self):
        print("\t Parsing SQL Locations")
        location_df=pd.read_sql_query("SELECT City, StateProvince, Country, PostalCode FROM Customer",self.conn)
        locStore_df=pd.read_sql_query("SELECT StoreCity, StoreStateProvince,StoreCountry,StorePostCode FROM Store",self.conn)
        locStore_df=locStore_df.rename(columns={"StoreCity":"City",
                                                "StoreStateProvince":"StateProvince",
                                                "StoreCountry":"Country",
                                                "StorePostCode":"PostalCode"})
        #adapted code from stackoverflow (2021)
        location_df=location_df.astype(object).replace({'United States' :'USA'})
        #end of adapted code
        location_df=location_df.append(locStore_df, ignore_index=True)
        location_df=location_df.rename(columns={"PostalCode":"LocationID"})
        location_df.drop_duplicates(subset="LocationID",keep="first",inplace=True,ignore_index=False)
        ODS.DimLocation_df=ODS.DimLocation_df.append(location_df)
        ODS.DimLocation_df.drop_duplicates(subset='LocationID',keep='first',inplace=True)

    def parseStores(self):
        print("\t Parsing SQL Stores")
        store_df=pd.read_sql_query("SELECT * FROM Store",self.conn)
        store_df=store_df.rename(columns={"StoreAddress":"Address",
                                          "StoreCity":"City",
                                          "StoreStateProvince":"StateProvince",
                                          "StoreCountry":"Country",
                                          "StorePostCode":"PostalCode",
                                          "StorePhone":"Phone"})
        store_df=store_df.astype(object).replace({'United States' :'USA'})
        ODS.DimStore_df=ODS.DimStore_df.append(store_df)
        ODS.DimStore_df.drop_duplicates(subset='StoreID',keep='first',inplace=True)

    def parseEmployees(self):
        print("\t Parsing SQL Employees")
        employee_df=pd.read_sql_query("SELECT EmployeeID, JobTitleID, Status, FirstName, LastName, Hiredate, BirthDate, Phone, EmailAddress FROM Employee",self.conn)
        now=pd.to_datetime('now')
        employee_df['Hiredate']=pd.to_datetime(employee_df['Hiredate'])
        employee_df['BirthDate']=pd.to_datetime(employee_df['BirthDate'])
        employee_df["Age"]=(now-employee_df['BirthDate']).dt.total_seconds()/(60*60*24*365.25)
        employee_df["Age"]=employee_df['Age'].apply(np.floor).astype(int)
        employee_df["YearsExp"]=(now-employee_df['Hiredate']).dt.total_seconds()/(60*60*24*365.25)
        employee_df["YearsExp"]=employee_df['YearsExp'].apply(np.floor).astype(int)
        employee_df=employee_df.drop(columns=['Hiredate','BirthDate'])
        employee_df=employee_df.rename(columns={"EmployeeID":"StaffID"})
        ODS.DimStaff_df=ODS.DimStaff_df.append(employee_df)
        ODS.DimStaff_df.drop_duplicates(subset='StaffID',keep='first',inplace=True)

    def parseCustomers(self):
        print("\t Parsing SQL Customers")
        customers_df=pd.read_sql_query("SELECT * FROM Customer",self.conn)
        ODS.DimCustomer_df= ODS.DimCustomer_df.append(customers_df)
        ODS.DimCustomer_df.drop_duplicates(subset='CustomerID',keep='first',inplace=True)

    def parseProducts(self):
        print("\t Parsing SQL Products")
        products_df=pd.read_sql_query("Select ProductID, ProductDescription, SupplierPrice, ProductPrice, CategoryID FROM Product", self.conn)
        ODS.DimProduct_df = ODS.DimProduct_df.append(products_df)
        ODS.DimProduct_df.drop_duplicates(subset='ProductID',keep='first',inplace=True)

    def parseFactStore(self):
        print("\t Parsing SQL FactStoreSales")
        saleItems_df=pd.read_sql_query("SELECT * FROM SaleItem",self.conn)
        sales_df=pd.read_sql_query('SELECT * FROM StoreSale',self.conn)
        sales_df = pd.merge(sales_df,saleItems_df, left_on='SaleID', right_on='SaleID', how='left')
        sales_df['FullDate']=pd.to_datetime(sales_df['DateOfSale'])
        sales_df['DateID']= sales_df['FullDate'].dt.strftime('%Y%m%d')
        sales_df = pd.merge(sales_df, ODS.DimStore_df[['StoreID', 'PostalCode']], left_on='StoreID', right_on='StoreID',how='left')
        sales_df=sales_df.rename(columns={"PostalCode":"LocationID"})
        sales_df = pd.merge(sales_df, ODS.DimProduct_df[['ProductID', 'SupplierPrice','ProductPrice']], left_on='ProductID',right_on='ProductID', how='left')
        sales_df = ProfitHelper.convertProfitValues(self,sales_df)
        sales_df['InvoiceID1'] = "SQL_"
        sales_df['InvoiceID2'] = range(1,len(sales_df.index)+1)
        sales_df['InvoiceID'] = sales_df['InvoiceID1'] + sales_df['InvoiceID2'].astype(str)
        sales_df ['SalesTax'] = sales_df['SubTotal'].astype(int) * .06 
        sales_df = sales_df.drop(columns =['Profit','SupplierPrice','ProductPrice','SaleID','SaleAmount','SaleTotal','DateOfSale','FullDate','InvoiceID1','InvoiceID2'])
        ODS.FactStoreSales_df= ODS.FactStoreSales_df.append(sales_df)

    def parseFactInternet(self):
        print("\t Parsing SQL FactInternetSales")
        intSales_df=pd.read_sql_query('SELECT * FROM InternetSale',self.conn)
        intSalesItem_df=pd.read_sql_query('Select * FROM InternetSaleItem',self.conn)
        intSales_df=pd.merge(intSales_df,intSalesItem_df,left_on='SaleID',right_on='SaleID',how='left')
        intSales_df['FullDate']=pd.to_datetime(intSales_df['DateOfSale'])
        intSales_df['ShipDate']=pd.to_datetime(intSales_df['DateShipped'])
        intSales_df['DateID']= intSales_df['FullDate'].dt.strftime('%Y%m%d')
        intSales_df = pd.merge(intSales_df, ODS.DimCustomer_df[['CustomerID', 'PostalCode']], left_on='CustomerID', right_on='CustomerID',how='left')
        intSales_df=intSales_df.rename(columns={"PostalCode":"LocationID"})
        intSales_df = pd.merge(intSales_df, ODS.DimProduct_df[['ProductID', 'SupplierPrice','ProductPrice']], left_on='ProductID',right_on='ProductID', how='left')
        intSales_df = ProfitHelper.convertProfitValues(self,intSales_df)
        #code adapted from kite (2022)
        intSales_df ['TransitPeriod'] = intSales_df['ShipDate'] - intSales_df['FullDate']
        #end of adapted code
        intSales_df ['SalesTax'] = intSales_df['SubTotal'].astype(int) * .06 
        intSales_df['InvoiceID1'] = "SQL_"
        intSales_df['InvoiceID2'] = range(1,len(intSales_df.index)+1)
        intSales_df['InvoiceID'] = intSales_df['InvoiceID1']+ intSales_df['InvoiceID2'].astype(str)
        intSales_df = intSales_df.drop(columns =['Profit','SupplierPrice','ProductPrice','SaleID','DateOfSale','SaleAmount','SaleTotal','DateShipped','FullDate','ShipDate','InvoiceID1','InvoiceID2'])
        ODS.FactInternetSales_df= ODS.FactInternetSales_df.append(intSales_df)
