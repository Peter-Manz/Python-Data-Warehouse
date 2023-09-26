from ParseSQL import ParseSQL
from ParseCSV import ParseCSV
from ParseJSON import ParseJSON
from ExportODS import ExportODS

class J86881Main:
    def __init__(self):
        sqlParser =ParseSQL()
        csvParser =ParseCSV()
        jsonParser = ParseJSON()
        export = ExportODS()

        sqlParser.parseSQL()
        csvParser.parseCSV()
        jsonParser.parseJSON()
        export.buildTables()
        export.exportODS()
        export.exportFactStoreSalesODS()
        export.exportFactInternetSalesODS()

main =J86881Main()