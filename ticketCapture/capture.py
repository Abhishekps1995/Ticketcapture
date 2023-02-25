
import sys 
import os
import time
import base64
import hashlib
from Cryptodome import Random
from Cryptodome.Cipher import AES
from flask import Flask,Blueprint
from flask_script import Manager
from flask_restplus import Api, Resource,inputs
from flask_restplus import reqparse
from flask_cors import CORS

class IsDirectory(Exception):
    pass

class DuplicationError(Exception):
    pass

class OperationalError(Exception):
    pass

class InputError(Exception):
    pass

class NoticketException(Exception):
    pass

class RefundVoidTicket(Exception):
    pass

flask_app = Flask(__name__)
CORS(flask_app)
ins_blueprint = Blueprint('api', __name__)
flask_app.register_blueprint(ins_blueprint)
flask_app.app_context().push()
manager = Manager(flask_app)
app = Api(app = flask_app)
name_space = app.namespace('api', description='TicketCapturing API')

@name_space.route("/ticket-capturing/capturing")
class CaptureAPI(Resource):
    @app.doc(responses={ 200: 'Success', 400: 'Invalid Argument', 500: 'Mapping Key Error' }, 
			 params={ 'Token': 'SSO Token','Token-Type': 'Token Type','str_application':'Application Type',
                                  'str_gds_name' : 'GDS Name','str_file' : 'File Name','str_file_content' : 'GDS File content',
                                  'bln_start': 'Reset Settings' ,'int_count': 'Count' , 'bln_db_reset' : 'Reset database'
                                })    
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('Token', required=True, location='headers')
        parser.add_argument('Token-Type', required=True, location='headers')
        parser.add_argument('str_application', type = str , required=True )
        parser.add_argument('str_gds_name', type = str , required=True )
        parser.add_argument('str_file', type = str , required=True)
        parser.add_argument('str_file_content', type = str , required=True)
        parser.add_argument('bln_start', type=str, required=True )
        parser.add_argument('int_count', type= int , required=True )
        parser.add_argument('bln_db_reset', type= bool , required=True )
        dct_args = parser.parse_args()
        if dct_args['str_application'] == 'SAAS' :
            str_token_status = ins_general_methods.verify_sso_token_and_connect_tenant_database(dct_args['Token'],dct_args['Token-Type'])
            if str_token_status != 'VALID_TOKEN' :
                return {
                        "str_status": str_token_status,
                        "str_token_type" : dct_args['Token-Type']
                        }
                        
        str_return_msg = self.capture_ticket_data(dct_args['str_file'] ,dct_args['str_file_content'] ,dct_args['str_gds_name'] ,dct_args['int_count'],bool(dct_args['bln_start']),dct_args['bln_db_reset'])
        return {
                    "str_status": str(str_return_msg)

            }

    def capture_ticket_data(self,str_file ,str_file_content ,str_gds_name ,int_count,bln_start = False,bln_db_reset = False):
        
        try:
            import amadeusCapture as amadeusCapture
            ins_amadeus_capture = amadeusCapture.Capture()
        except Exception as msg:
            print ('Amadeus Files are not capturing since ' + str(msg))

        try:
            import galileoCapture as galileoCapture
            ins_galileo_capture = galileoCapture.Capture()
        except Exception as msg:
            print ('Galileo Files are not capturing since ' + str(msg))

        try:
            import sabreCapture as sabreCapture
            ins_sabre_capture = sabreCapture.Capture()
        except Exception as msg:
            print ('Sabre Files are not capturing since ' + str(msg))

        try:
            import worldspanCapture as worldspanCapture
            ins_worldspanCapture_capture = worldspanCapture.Capture()
        except Exception as msg:
            print ('Worldspan Files are not capturing since ' + str(msg))
        
        try:
            if bln_db_reset and ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS':
                time.sleep(3)
                ins_general_methods.connect_db()
                print ('Database reconnected...!')
        except Exception as msg:
            print ('Database reconnection failed...!')
            
        str_file_content = str(str_file_content).replace('|*#*|','\r')
        if not str_file_content.replace('\r','').strip():
            return 'NOTICKET'
        str_message = ''
        try :
            print('*********'*5,'\nStarting Capture : ',str_file)
            if str_gds_name == 'amadeus' :#and dct_conf_data.has_key('amadeus_directory'):                
                str_message = ins_amadeus_capture.extract_ticket_data_from_file(str_file , lst_tsm_number = [], bln_emd_ticket = False, lst_emd_tickets = [],bln_from_web_service = True,str_file_content = str_file_content,int_count = int_count,bln_start=bln_start)

            elif str_gds_name == 'galileo' :#and dct_conf_data.has_key('galileo_directory') :
                str_message = ins_galileo_capture.extract_ticket_data_from_file(str_file ,str_file , bln_from_web_service = True,str_file_content = str_file_content,int_count = int_count,bln_start=bln_start)

            elif str_gds_name == 'sabre' :#and dct_conf_data.has_key('sabre_directory') :
                str_message = ins_sabre_capture.extract_ticket_data_from_file(str_file ,bln_from_web_service = True,str_file_content = str_file_content,int_count = int_count,bln_start=bln_start)

            elif str_gds_name == 'worldspan' :#and 'worldspan_directory' in dct_conf_data :
                str_message = ins_worldspanCapture_capture.extract_ticket_data_from_file(str_file , bln_from_web_service = True,str_file_content = str_file_content,int_count = int_count,bln_start=bln_start)

#            elif str_gds_name == 'Farelogix' and 'farelogix_directory' in dct_conf_data:                
#                str_message = ins_farelogix_capture.extract_ticket_data_from_file(str_file , lst_tsm_number = [], bln_emd_ticket = False, lst_emd_tickets = [],bln_from_web_service = True,str_file_content = str_file_content,int_count = int_count,bln_start=bln_start)

            if not str_message :
                str_message = ''
                
        except IsDirectory:
            return 'SKIP'
        except NoticketException:
            return 'NOTICKET'
        except InputError as msg:
            print(str(msg))
            return 'FAIL'+ ' ' +str(msg)
        except DuplicationError as msg:
            print(str(msg))
            return 'DUPLICATION' + ' ' + str_message or ''
        except OperationalError as msg:
            print(str(msg))
            return 'FAIL'+str(msg)
        except Exception as msg:
            print(msg)
            if str(msg).startswith("Duplicate Ticket") :
                return 'SUCCESS' + ' ' + str(msg)
            if str(msg).find('NOTICKET') != -1 :
                return 'NOTICKET'
            
            str_line_no = str(sys.exc_info()[2].tb_lineno)
            str_message = str(sys.exc_info()[1])
            print(str(msg))
            return 'FAIL '+str(msg)#+ ' ' +str_line_no
        except RefundVoidTicket:
            return 'REFUNDVOIDERROR'
        else:
            
            return 'SUCCESS' + ' '+ str_message or ''
        

@name_space.route("/ticket-capturing/encrypt-client-configuration")
class ClientConfigAPI(Resource):
    @app.doc(responses={ 200: 'Success', 400: 'Invalid Argument' }, 
			 params={ 'Token': 'SSO Token','Token-Type': 'Token Type','crypt-type': 'Crypt type*(ENCRYPT/DECRYPT)','decrypt-raw':'Decrypt raw','user-name': 'User name',
                                  'user-password': 'User password','application':'Application Type*(WAVE/SAAS)','server-url' : 'Server URL*(Eg:http://0.0.0.0:5000)','sso-url' : 'SSO URL*(Eg:http://0.0.0.0:5000)',
                                  'amadeus-directory' : 'amadeus directory','galileo-directory': 'Galileo directory' ,'sabre-directory': 'Sabre directory' ,'worldspan-directory' : 'Worldspan directory'
                                })
    def post(self):
        parser1 = reqparse.RequestParser()
        parser1.add_argument('Token',  location='headers')
        parser1.add_argument('Token-Type',  location='headers')
        parser1.add_argument('crypt-type', required=True, type = str)
        parser1.add_argument('decrypt-raw', type = str )
        parser1.add_argument('user-name', type = str )
        parser1.add_argument('user-password', type = str )
        parser1.add_argument('application', required=True, type = str )
        parser1.add_argument('server-url', required=True, type = str )
        parser1.add_argument('sso-url', required=True, type = str )
        parser1.add_argument('amadeus-directory', type = str )
        parser1.add_argument('galileo-directory', type = str )
        parser1.add_argument('sabre-directory', type = str )
        parser1.add_argument('worldspan-directory', type = str )

        dct_args = parser1.parse_args()
        if dct_args['application'] == 'SAAS' :
            str_token_status = ins_general_methods.verify_sso_token_and_connect_tenant_database(dct_args['Token'],dct_args['Token-Type'])
            if str_token_status != 'VALID_TOKEN' :
                return {
                        "str_status": 'INVALID_TOKEN_PROVIDED',
                        "str_token_type" : dct_args['Token-Type']
                        }
                        
        if str(dct_args['crypt-type']).upper() == 'DECRYPT' and str(dct_args['decrypt-raw']).strip(): 
            str_return_msg = self.decrypt(dct_args['decrypt-raw'].strip())
            return {
                    "str_status": "Decrypted: "+str(str_return_msg.decode('utf8'))
                    }
        elif str(dct_args['crypt-type']).upper() == 'ENCRYPT' : 
            lst_return_msg = []
            if dct_args['application'].upper() == 'SAAS' and dct_args['user-name'] and dct_args['user-password'] and dct_args['server-url']  and dct_args['sso-url'] :
                lst_return_msg.append( self.encrypt('str_user_name = '+str(dct_args['user-name'])).decode('utf8') )
                lst_return_msg.append( self.encrypt('str_user_password = '+str(dct_args['user-password'])).decode('utf8') )
                lst_return_msg.append( self.encrypt('str_sso_url = '+str(dct_args['sso-url'])).decode('utf8') )
                
            lst_return_msg.append( self.encrypt('str_server_url = '+str(dct_args['server-url'])).decode('utf8') )
            lst_return_msg.append( self.encrypt('str_application = '+str(dct_args['application'])).decode('utf8') )
            if dct_args['amadeus-directory'] is not None :
                lst_return_msg.append( self.encrypt('amadeus_directory = '+str(dct_args['amadeus-directory'])).decode('utf8') )
            if dct_args['galileo-directory'] is not None :
                lst_return_msg.append( self.encrypt('galileo_directory = '+str(dct_args['galileo-directory'])).decode('utf8') )
            if dct_args['sabre-directory'] is not None :
                lst_return_msg.append( self.encrypt('sabre_directory = '+str(dct_args['sabre-directory'])).decode('utf8') )
            if dct_args['worldspan-directory'] is not None :
                lst_return_msg.append( self.encrypt('worldspan_directory = '+str(dct_args['worldspan-directory'])).decode('utf8') )
            str_return_msg = '^$$^'.join(lst_return_msg)
            return {
                        "str_status": "Encrypted: "+str(str_return_msg)
                   }
        
    def encrypt( self, raw ):
        secrect_key = '2KNucTr@@CS'
        key = hashlib.sha256(secrect_key.encode('utf-8')).digest()
        block_size = 16
        pad = lambda s: s + (block_size - len(s) % block_size) * chr(block_size - len(s) % block_size)
        raw = pad(raw)
        iv = Random.new().read( AES.block_size )
        cipher = AES.new( key, AES.MODE_CBC, iv )
        return base64.b64encode( iv + cipher.encrypt( raw.encode('utf8') ) )

    def decrypt( self, enc ):
        secrect_key = '2KNucTr@@CS'
        key = hashlib.sha256(secrect_key.encode('utf-8')).digest()
        unpad = lambda s : s[0:-s[-1]]
        enc = base64.b64decode(enc)
        iv = enc[:16]
        cipher = AES.new(key, AES.MODE_CBC, iv )
        return unpad(cipher.decrypt( enc[16:] ))
    

try:
    from ticketCapture.lib import generalMethods
    from ticketCapture.lib import generalMethodsWave
except:
    from lib import generalMethods
    from lib import generalMethodsWave

try:
    ins_general_methods = generalMethods.GeneralMethods()
    if ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS':
        ins_general_methods = generalMethodsWave.GeneralMethods()
        try:
            import amadeusCapture as amadeusCapture
        except Exception as msg:
            print ('Amadeus File import error.. ' + str(msg))

        try:
            import galileoCapture as galileoCapture
        except Exception as msg:
            print ('Galileo File import error.. ' + str(msg))

        try:
            import sabreCapture as sabreCapture
        except Exception as msg:
            print ('Sabre File import error.. ' + str(msg))

        try:
            import worldspanCapture as worldspanCapture
        except Exception as msg:
            print ('Worldspan File import error.. ' + str(msg))
        
except Exception as msg:
    print ('Connection Error..',msg)
    raise

@manager.command
def run():
    flask_app.run(host='0.0.0.0', port=ins_general_methods.dct_conf_data['TRAACS_PORT'], debug=True)

if __name__ == '__main__':
    manager.run()
    
