import pyodbc
import pandas as pd
from ODS import ODS

class ExportODS:
	buildSchema = '''
DROP TABLE IF EXISTS FactStoreSales
DROP TABLE IF EXISTS FactInternetSales
DROP TABLE IF EXISTS DimCustomer
DROP TABLE IF EXISTS DimStaff
DROP TABLE IF EXISTS DimStore
DROP TABLE IF EXISTS DimProduct
DROP TABLE IF EXISTS DimDate
DROP TABLE IF EXISTS DimLocation

CREATE TABLE DimLocation(
	LocationID nvarchar(10) PRIMARY KEY NOT NULL,
	City nvarchar(50),
	StateProvince nvarchar(25),
	Country nvarchar(25)
)

CREATE TABLE DimDate(
	DateID nvarchar(10) PRIMARY KEY NOT NULL,
	FullDate date,
	Day nvarchar(10),
	Month nvarchar(10),
	Year int,
	DayOfWeek int, 
	DayOfYear int,
	Quarter int
)

CREATE TABLE DimProduct(
	ProductID nvarchar(30) PRIMARY KEY NOT NULL,
	ProductDescription nvarchar(250),
	SupplierPrice money,
	ProductPrice money,
	CategoryID nvarchar(50)
)

CREATE TABLE DimStore(
	StoreID nvarchar(10) PRIMARY KEY NOT NULL,
	Address nvarchar(50),
	City nvarchar(50),
	StateProvince nvarchar(25),
	Country nvarchar(25),
	PostalCode nvarchar(10),
	Phone nvarchar(15)
)

CREATE TABLE DimStaff(
	StaffID nvarchar(15) PRIMARY KEY NOT NULL,
	JobTitleID nvarchar(10),
	YearsExp int,
	FirstName nvarchar(30),
	LastName nvarchar(30),
	Age int,
	Phone nvarchar(15),
	EmailAddress nvarchar(75), 
	Status nvarchar(15)
)

CREATE TABLE DimCustomer(
	CustomerID nvarchar(10) PRIMARY KEY NOT NULL,
	CustomerEmail nvarchar(75),
	FirstName nvarchar(30),
	SecondName nvarchar(30),
	CustomerType nvarchar(15),
	City nvarchar(50),
	StateProvince nvarchar(25),
	Country nvarchar(25),
	PostalCode nvarchar(10)
	
)

CREATE TABLE FactInternetSales (
	InvoiceID nvarchar(20) PRIMARY KEY NOT NULL,
	ProductID nvarchar(30),
	DateID nvarchar(10),
	CustomerID nvarchar(10),
	LocationID nvarchar(10),
	ShippingType nvarchar(20),
	TransitPeriod nvarchar(15),
	Quantity int, 
	SalesTax money,
	SubTotal money,
	SaleProfit money
	FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
	FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
	FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
	FOREIGN KEY (LocationID) REFERENCES DimLocation(LocationID)
)

CREATE TABLE FactStoreSales (
	InvoiceID nvarchar(20) PRIMARY KEY NOT NULL,
	ProductID nvarchar(30),
	DateID nvarchar(10),
	LocationID nvarchar(10),
	StoreID nvarchar(10),
	StaffID nvarchar(15),
	Quantity int, 
	SalesTax money,
	SubTotal money,
	SaleProfit money
	FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
	FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
	FOREIGN KEY (LocationID) REFERENCES DimLocation(LocationID),
	FOREIGN KEY (StoreID) REFERENCES DimStore(StoreID),
	FOREIGN KEY (StaffID) REFERENCES DimStaff(StaffID)
)

'''
	def __init__(self):
		print("Building ParseSQL Data WareHouse from schema")
		connectionString = "DRIVER={SQL Server};SERVER=sql2016.fse.network;DATABASE=db_2013750_co5222_warehousedb;UID=user_db_2013750_co5222_warehousedb;PWD=P@55word"
		self.conn = pyodbc.connect(connectionString)
		self.cursor = self.conn.cursor()

	def buildTables(self):
		print("\t Building tables from ODS into SQL WareHouse")
		self.cursor.execute(self.buildSchema)
		self.conn.commit()

	def exportUsingCSV(self, table, dataframe, chunksize):
		rows = len(dataframe.index)
		current = 0 #Starting at row 0
		while current < rows:
			if rows - current < chunksize: #Determine rows split into chunks
				stop = rows
			else:
				stop = current + chunksize
			#Create a CSV file from current position to end of the chunk
			CSV = dataframe.iloc[current:stop].to_csv(index=False, header=False,
				quoting=1, quotechar="'", line_terminator="),\n(")
			CSV = CSV[:-3] #Delete last three chars i.e., ",\n"
			values = f"({CSV}" #Convert CSV into a string
			#Write SQL string and replace empties with NULL
			SQL = f"\t \t INSERT INTO {table} VALUES {values}".replace("''", "NULL")
			current = stop #Move to end of chunk
			self.cursor.execute(SQL) #Execute the SQL
		self.conn.commit()

	def exportODS(self):
		print("Exporting ODS to SQL")
		chunksize=999
		for table in ODS.tables:
			df = getattr(ODS, f"{table}_df")
			print(f"\t Exporting to {table} from {table}_df with {len(df.index)} rows")			
			self.exportUsingCSV(table,df,chunksize)

	def exportFactStoreSalesODS(self):
		print("Exporting ODS.FactStoreSales to SQL")
		chunksize=999
		rows = len(ODS.FactStoreSales_df.index)
		current = 0 
		while current < rows:
			if rows - current < chunksize: 
				stop = rows
			else:
				stop = current + chunksize
			CSV = ODS.FactStoreSales_df.iloc[current:stop].to_csv(index=False, header=False,
				quoting=1, quotechar="'", line_terminator="),\n(")
			CSV = CSV[:-3] 
			values = f"({CSV}"
			SQL = f"\t \t INSERT INTO FactStoreSales VALUES {values}".replace("''", "NULL")
			current = stop 
			self.cursor.execute(SQL)
		self.conn.commit()
			
	def exportFactInternetSalesODS(self):
		print("Exporting ODS.FactInternetSales to SQL")
		chunksize=999
		rows = len(ODS.FactInternetSales_df.index)
		current = 0
		while current < rows:
			if rows - current < chunksize: 
				stop = rows
			else:
				stop = current + chunksize
			CSV = ODS.FactInternetSales_df.iloc[current:stop].to_csv(index=False, header=False,
				quoting=1, quotechar="'", line_terminator="),\n(")
			CSV = CSV[:-3]
			values = f"({CSV}"
			SQL = f"\t \t INSERT INTO FactInternetSales VALUES {values}".replace("''", "NULL")
			current = stop
			self.cursor.execute(SQL)
		self.conn.commit()
