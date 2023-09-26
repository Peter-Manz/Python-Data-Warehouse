import pandas as pd

class ODS:
    tables = [
        'DimProduct',
        'DimStore',
        'DimStaff',
        'DimLocation',
        'DimDate',
        'DimCustomer']

    DimProduct_df=pd.DataFrame(columns = ['ProductID','ProductDescription','SupplierPrice','ProductPrice','CategoryID'])
    DimStore_df=pd.DataFrame(columns=['StoreID','Address','City','StateProvince','Country','PostalCode','Phone'])
    DimStaff_df=pd.DataFrame(columns=['StaffID','JobTitleID','YearsExp','FirstName','LastName','Age','Phone','EmailAddress','Status'])
    DimLocation_df=pd.DataFrame(columns=['LocationID','City','StateProvince','Country'])
    DimDate_df=pd.DataFrame(columns=['DateID','FullDate','Day','Month','Year','DayOfYear','DayOfWeek','Quarter'])
    DimCustomer_df=pd.DataFrame(columns=['CustomerID','CustomerEmail','FirstName','SecondName','CustomerType','City','StateProvince','Country','PostalCode'])

    FactStoreSales_df=pd.DataFrame(columns=['InvoiceID','ProductID','DateID','LocationID','StoreID','StaffID','Quantity','SalesTax','SubTotal','SaleProfit'])
    FactInternetSales_df=pd.DataFrame(columns=['InvoiceID','ProductID','DateID','CustomerID','LocationID','ShippingType','TransitPeriod','Quantity','SalesTax','SubTotal','SaleProfit'])


