import json
import pandas as pd
from DateHelper import DateHelper
from ODS import ODS

class ParseJSON:
    def __init__(self):
        print("Building Parse JSON via Constructor")

        self.file = "SalesJSON.json".encode("ISO-8859-1") 

        with open("SalesJSON.json") as f: 
            self.data = json.load(f)  

        self.sales_df = pd.json_normalize(data = self.data['Sale'])   
       
    def parseJSON(self):
        print("Parsing JSON File")
        self.parseDates()
        self.parseProducts()
        self.parseLocations()
        self.parseCustomers()
        self.parseSales()

    def parseDates(self):
        print("\t Parsing JSON Dates")        
        dates_df = pd.DataFrame(self.sales_df['DateOfSale'])
        dates_df['FullDate']=pd.to_datetime(dates_df['DateOfSale'])
        dates_df = DateHelper.convertDateValues(self,dates_df)
        dates_df=dates_df.drop(columns=['DateOfSale'])
        ODS.DimDate_df = ODS.DimDate_df.append(dates_df)
        ODS.DimDate_df.drop_duplicates(subset="DateID", keep='first',inplace=True)
       
    def parseProducts(self):
        print("\t Parsing JSON Products")
        product_df = pd.json_normalize(data =self.data['Product'])
        product_df = product_df.rename(columns={'id':'ProductID','prices.amountMax':'ProductPrice','prices.amountMin':'SupplierPrice','name':'ProductDescription','primaryCategories':'CategoryID'})
        product_df=product_df.drop(columns=['prices.currency','brand','manufacturer'])
        ODS.DimProduct_df = ODS.DimProduct_df.append(product_df)
        ODS.DimProduct_df.drop_duplicates(subset='ProductID',keep='first',inplace=True)
       
    def parseSales(self):
        print("\t Parsing JSON Sales")
        self.sales_df =self.sales_df.explode('Sales')
        self.sales_df = pd.concat(
            [   
                self.sales_df.drop(['Sales'], axis=1),
                self.sales_df['Sales'].apply(pd.Series)                
            ], axis=1)        
        self.sales_df[['FirstName','SecondName']] = self.sales_df.Customer.str.split(" ", expand=True)
        self.sales_df = pd.merge(self.sales_df, ODS.DimCustomer_df[
            ['CustomerID','FirstName','SecondName','PostalCode']
            ],left_on=['FirstName','SecondName'],right_on=['FirstName','SecondName'], how ='left')
        self.sales_df['ProductDescription'] = self.sales_df['Product']
        self.sales_df = pd.merge(self.sales_df,ODS.DimProduct_df[
            ['ProductID','ProductDescription','SupplierPrice','ProductPrice']],
            left_on='ProductDescription',right_on='ProductDescription', how='left')
        self.sales_df.index += len(ODS.FactInternetSales_df)
        self.sales_df['InvoiceID'] = "JSON_" + self.sales_df.index.astype('string')
        self.sales_df['Profit']= self.sales_df['ProductPrice'] - self.sales_df['SupplierPrice'] 
        self.sales_df ['SaleProfit'] = self.sales_df['Quantity'] * self.sales_df['Profit']
        self.sales_df['FullDate']=pd.to_datetime(self.sales_df['DateOfSale'])
        self.sales_df['DateID']= self.sales_df['FullDate'].dt.strftime('%Y%m%d')      
        self.sales_df['LocationID']= self.sales_df['PostalCode']
        self.sales_df['SalesTax'] = self.sales_df['Subtotal'] * .06 
        self.sales_df=self.sales_df.drop(columns=['Product','ProductDescription','Profit','SaleID','Customer','Delivery','SubTotal','SaleTotal','FirstName','SecondName','TaxRate','DateOfSale','FullDate','PostalCode','SupplierPrice','ProductPrice','SaleTax'])
        self.sales_df['SubTotal']=self.sales_df['Subtotal']
        self.sales_df=self.sales_df.drop(columns=['Subtotal'])
        ODS.FactInternetSales_df=ODS.FactInternetSales_df.append(self.sales_df)
        ODS.FactInternetSales_df.drop_duplicates(subset='InvoiceID',keep='first',inplace=True)
        #adapted code from stackoverflow (2017)
        ODS.FactInternetSales_df['ShippingType'].fillna("NULL",inplace=True)
        #end of adapted code

    def parseLocations(self):
        print("\t Parsing JSON Locations")
        location_df = pd.DataFrame(self.sales_df['Delivery'])
        location_df = location_df.explode('Delivery')
        #code adapted from devenum (2021)
        location_df[['City','StateProvince','Country','LocationID']] = location_df['Delivery'].str.split(',',expand=True)
        #end of adapted Code
        location_df=location_df.drop(columns=['Delivery'])
        location_df=location_df.astype(object).replace({' United States' :'USA'})
        ODS.DimLocation_df=ODS.DimLocation_df.append(location_df)
        ODS.DimLocation_df.drop_duplicates(subset='LocationID',keep='first',inplace=True)

    def parseCustomers(self):
        print("\t Parsing JSON Customers")
        customer_df = pd.DataFrame(self.sales_df['Customer'])
        customer_df[['FirstName','SecondName']] = customer_df.Customer.str.split(" ", expand=True)
        customer_df = pd.merge(customer_df, ODS.DimCustomer_df[['CustomerID','CustomerEmail','FirstName','SecondName','CustomerType','City','StateProvince','Country','PostalCode']],left_on=['FirstName','SecondName'],right_on=['FirstName','SecondName'], how ='left')
        customer_df=customer_df.drop(columns=['Customer'])
        ODS.DimCustomer_df = ODS.DimCustomer_df.append(customer_df)
        ODS.DimCustomer_df.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        ODS.DimCustomer_df=ODS.DimCustomer_df.astype(object).replace({'United States':'USA'})
