import json
import dicttoxml    
import logging
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
    
class DataExporter:
    def __init__(self, output_format):
        self.output_format = output_format.lower()
            
    def __export_json(self, data):
        logging.info("Exporting data to JSON")
        with open('results.json', 'w') as output:
            json.dump(data, output, cls=DecimalEncoder, ensure_ascii=False, indent=4)

            
    def __export_xml(self, data):
        logging.info("Exporting data to XML")
        xml = dicttoxml.dicttoxml(data, custom_root='results', attr_type=False)
        with open('results.xml', 'wb') as file:
            file.write(xml)
    
    def export(self, data):
        if self.output_format == 'json':
            self.__export_json(data) 
            
        elif self.output_format == 'xml':
            self.__export_xml(data)
            
        else:
            logging.error('Unsupported output format')
            raise ValueError('Unsupported output format')
        