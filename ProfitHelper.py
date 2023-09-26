class ProfitHelper(object):
    def convertProfitValues(self, df):
        #code adapted from Easytweaks (2022)
        df ['SubTotal'] = df['ProductPrice'] * df['Quantity']
        df ['Profit']= df['ProductPrice'] - df['SupplierPrice']
        df ['SaleProfit'] = df['Quantity'] * df['Profit']
        #end of adapted code 
        return df



