"""
Purpose: General Methods for capturing
Owner  : KISHOR PS
Date   : 01/10/2019
Re. F/M: 

"""
import binascii
import time
import copy
#import mx.DateTime
import re
import os
import json
from requests import Session
import datetime
import jwt
import redis
import psycopg2    
import psycopg2.extras

class GlobalInstance:
    
    #https://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(GlobalInstance, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self):
        self.dct_not_parsed_files = {}
        self.dct_no_ticket_files = {}
        self.dct_pax_no_service_fee = {}
        self.dct_seg_no_service_fee = {}
        self.dct_seg_no_voucher_supplier_amount = {}
        self.dct_seg_no_voucher_selling_price = {}
        self.dct_pax_no_selling_price = {}
        self.dct_seg_no_selling_price = {}
        self.dct_seg_no_voucher_tax = {}
        self.dct_seg_no_voucher_supplier = {}
        self.dct_seg_no_voucher_discount = {}
        self.dct_service_fee_sector_wise = {}
        self.dct_child_service_fee_sector_wise = {}
        self.dct_infant_service_fee_sector_wise = {}
        self.dct_extra_earning_sector_wise = {}
        self.dct_child_extra_earning_sector_wise = {}
        self.dct_infant_extra_earning_sector_wise = {}
        self.dct_published_fare_sector_wise = {}
        self.dct_child_published_fare_sector_wise = {}
        self.dct_infant_published_fare_sector_wise = {}
        self.dct_emd_connection_ticket_and_sector = {}
        self.lst_process_list = []
        self.lst_process_list_void = []
    
class NonIataCaptureBase:
    def __init__(self):
        self.str_supplier = ''
        self.str_url = ''
        self.str_user_name = ''
        self.str_password = ''
        self.bln_sign_in_code = False
        self.bln_office_id = False
        self.bln_counter_staff = False
        self.bln_customer = False
        self.lst_sign_in_code = []
        self.lst_office_id = []
        self.lst_customer = []
        self.lst_counter_staff = []
        
class OfficeIdInvoice :
    def __init__(self):
        
        self.str_gds = ''
        self.str_type = ''
        self.str_office_id = ''
        self.int_customer_id = None
        self.str_customer_code = ''
        self.int_counter_staff_id = None
        self.int_cost_center_id = None
        self.int_department_id = None
        self.bln_capture_service_fee = True
        self.str_amad_code = ''
        self.str_amad_num_code = ''
        self.str_gal_code = ''
        self.str_gal_num_code = ''
        self.str_sab_code = ''
        self.str_sab_num_code = ''
        self.str_ws_code = ''
        self.str_ws_num_code = ''
        self.str_far_code = ''
        self.str_far_num_code = ''
        self.bln_auto_invoice = True
        
class AutoInvoice:
    def __init__(self):
        
        self.bln_staff_num_code_match = False
        self.bln_staff_officeid_match = False
        self.bln_voucher_capture = False
        self.bln_xo_capture = False
        self.bln_auto_invoice = False
        self.bln_auto_invoice_all_ticket = False
        self.bln_auto_refund_all_ticket = False
        self.bln_auto_refund = False
        self.bln_capture_client_set_in_tick_agent = False
        self.bln_capture_client_set_in_book_agent = False
        self.bln_capture_use_client_set_in_tick_agent = False
        self.bln_capture_use_client_set_in_book_agent = False
        self.dct_office_id_data = {}
        self.str_input_vat_code = ''
        self.dct_office_id_data_refund = {}

class FolderBase(object):
    
    def __init__(self, *args):
        self.str_directory = ''
        self.str_parsed_dir = ''
        self.str_parsed_dir_monthly = ''
        self.str_no_ticket_files_dir = ''
        self.str_no_ticket_files_dir_monthly = ''
        self.str_not_parsed_dir = ''
        self.int_not_parsed_folder_files_controler = 0
        
class CaptureBase(object):
    
    #https://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CaptureBase, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self, *args):
        self.db = None
        self.str_directory = ''
        self.str_branch_code = ''
        self.bln_multi_currency = True
        self.int_sleep_time = 5
        self.str_parsed_dir = ''
        self.str_parsed_dir_monthly = ''
        self.str_no_ticket_files_dir = ''
        self.str_no_ticket_files_dir_monthly = ''
        self.str_not_parsed_dir = ''
        self.int_not_parsed_folder_files_controler = 0
        self.chr_field_seperator = ''
        self.bln_diif_fp_section = False
        self.bln_capture_client_set_in_tick_agent = False
        self.bln_capture_client_set_in_book_agent = False
        self.int_mask_start_amadeus = 0
        self.int_mask_char_count_amadeus = 0
        self.str_mask_char_amadeus = ''
        self.int_mask_start_galileo = 0
        self.int_mask_char_count_galileo = 0
        self.str_mask_char_galileo = ''
        self.int_mask_start_sabre = 0
        self.int_mask_char_count_sabre = 0
        self.str_mask_char_sabre = ''
        self.str_consider_nf_key_word_in_amdeus = 'False'
        self.bln_consider_dana_hotel_voucher_capture = 'False'
        self.ticketing_agent_char_code_for_ins_voucher = ''
        self.ticketing_agent_numeric_code_for_ins_voucher = ''   
        self.str_traacs_version = ''  
        
class GeneralMethods:
    
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(GeneralMethods, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self):
        self.read_configuration_file()
#        self.connect_db()
        
        
        pass
#        
#        self.str_base_currency = self.get_admin_settings('STR_CURRENCY_BASE_1')
#        self.bln_set_cc_chrge_as_profit = self.get_admin_settings('BLN_CREDIT_CARD_CHARGES_AS_PROFIT')
#        if not self.str_base_currency :
#            raise Exception("Base currency not Found")
#        self.str_currency_conversion_method = self.get_admin_settings('ROE_FORMAT')
#        
#        self.int_rounding_digits = 3 
#        self.set_capturing_settings_dictionary()
#        self.set_optional_field_data()
#        self.set_airplus_field_data()
#        self.get_meals_plan_id()
#        self.str_reverse_calculate_dis_extra_amt = self.get_admin_settings('ADJUST_SELLING_PRICE_DIFFERENCE_TO')
#        self.str_consider_higher_travel_class = self.get_admin_settings('BLN_HIGHER_TRAVEL_CLASS')
#        
#        self.lst_non_iata_instance = []
#        self.dct_non_iata_capture_settings = {}
#        
#        self.ins_auto_inv = AutoInvoice()
#        self.ins_capture_base = CaptureBase()
##        self.set_capture_settings_data()
#        self.create_configuration_file_instance()
#        self.ins_global = GlobalInstance()
##        self.set_non_iata_capture_details()
#        self.bln_enable_non_iata_capture = False #need to remove
##        self.set_auto_invoice_refund_details()
#        self.int_min_location_id = self.get_min_location_id()
#        self.int_min_department_id = self.get_min_department_id() 
#        
#        self.bln_capture_input_vat_only_for_dom_tickets =  self.get_admin_settings('BLN_CAPTURE_INPUT_VAT_ONLY_FOR_DOM_TKTS')
        

    def reload_data(self,):
        
        
#        print("Reloading data")
        
#        self.get_tenant_data_from_redis()
        self.ins_auto_inv = AutoInvoice()
        self.ins_capture_base = CaptureBase()
        self.ins_global = GlobalInstance()
        self.create_configuration_file_instance()
        
        self.str_base_currency = self.get_admin_settings('STR_CURRENCY_BASE_1')
        self.bln_set_cc_chrge_as_profit = self.get_admin_settings('BLN_CREDIT_CARD_CHARGES_AS_PROFIT')
        self.str_currency_conversion_method = self.get_admin_settings('ROE_FORMAT')
        
        self.set_capturing_settings_dictionary()
        self.set_optional_field_data()
        self.get_meals_plan_id()
        self.str_reverse_calculate_dis_extra_amt = self.get_admin_settings('ADJUST_SELLING_PRICE_DIFFERENCE_TO')
        self.str_consider_higher_travel_class = self.get_admin_settings('BLN_HIGHER_TRAVEL_CLASS')
        
        self.lst_non_iata_instance = []
        self.dct_non_iata_capture_settings = {}
        self.dct_airplus_capturing_settings = {}
#        self.set_non_iata_capture_details()
        self.bln_enable_non_iata_capture = False #need to remove
#        self.set_auto_invoice_refund_details()
        self.int_min_location_id = self.get_min_location_id()
        self.int_min_department_id = self.get_min_department_id()
        self.ins_auto_inv.str_input_vat_code = self.get_admin_settings('STR_INPUT_VAT_GST_COMPONENTS')
        self.bln_capture_input_vat_only_for_dom_tickets =  self.get_admin_settings('BLN_CAPTURE_INPUT_VAT_ONLY_FOR_DOM_TKTS')
        self.get_voucher_configuration_and_masking_settings()
        
    def read_configuration_file(self, *args):
        #configuration file reading
        #As per the configuration extenilization process configurations files will be in different location.
        str_path = "/app/configurations/traacs-saas-capturing/"
        str_conf_path = str_path + "capture.conf"  
#        str_service_conf_path = str_path+ "voucherCapture.conf"  
        if not os.path.exists(str_conf_path) :
            str_conf_path = os.path.realpath(__file__)
            index = str_conf_path.rindex("/")
            str_conf_path = str_conf_path[:index-3] + "capture.conf"  
#            str_service_conf_path = str_conf_path[:index-3]+ "voucherCapture.conf"  

        fd = open(str_conf_path,'r')
        
        self.dct_conf_data = {}
        for str_line in fd.readlines():
            str_line = str_line.strip()
            if str_line != "" and str_line[0] != "#":
                self.dct_conf_data[str_line.split("=")[0].strip()] = str_line.split("=")[1].strip()
                pass
            pass
        fd.close()
#        
#        #voucher configuration file reading
#        str_key = ""
#        self.dct_voucher_conf_data = {}
#        fd = open(str_service_conf_path,'r')
#        for str_line in fd.readlines():
#            str_line = str_line.strip()
#            if str_line != "" and str_line[0] != "#":
#                if str_line.startswith("**"):
#                    str_key = str_line.strip().strip("*").strip()
#                    self.dct_voucher_conf_data[str_key] = {}
#                else :
#                    self.dct_voucher_conf_data[str_key][str_line.split(":")[0].strip().upper()] = str_line.split(":")[1].strip().upper()+':'
#                
#        fd.close()
    
    def connect_db(self, dct_tenant_data):
        
        """Database Connection
        """
        if all(key in self.dct_conf_data for key in ('database','host','user','password')): 
            dct_tenant_data['db_name'] = self.dct_conf_data['database']
            dct_tenant_data['db_host'] = self.dct_conf_data['host']
            dct_tenant_data['db_port'] = '5432'
            dct_tenant_data['db_user'] = self.dct_conf_data['user']
            dct_tenant_data['db_password'] = self.dct_conf_data['password']
        try:
            self.ins_db = psycopg2.connect("""dbname=%s user=%s password=%s host=%s port=%s"""%
                                                    (dct_tenant_data['db_name'],
                                                     dct_tenant_data['db_user'],
                                                     dct_tenant_data['db_password'],
                                                     dct_tenant_data['db_host'],
                                                     dct_tenant_data['db_port']))
            
        except Exception as msg:
            print(msg)
            raise
        
        try:
            cr1 = self.create_cursor()
            cr1.execute("""SET datestyle = 'DMY'""")
        except Exception as msg:
            self.ins_db.rollback()
            cr1.close()
            raise
        else:
            cr1.close()
            self.ins_db.commit()
            pass
    
    def create_cursor(self, *args):
        cr = self.ins_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cr
    
    def create_configuration_file_instance(self,):
        
        str_current_date_time = datetime.datetime.now().strftime("%B%Y")
        
        try:
            self.ins_capture_base.db = self.ins_db
        
        except Exception as msg:
            print(msg)
            raise Exception("Unable to connect to Database")
        
        self.ins_capture_base.int_sleep_time = int(self.dct_conf_data.get('sleep_time',5))
        self.ins_capture_base.chr_field_seperator = self.dct_conf_data.get('field_seperator', '')
        self.ins_capture_base.str_consider_nf_key_word_in_amdeus = self.dct_conf_data.get('CONSIDER_NF_KEYWORD_IN_AMADEUS','False')
        if self.dct_conf_data.get('CONSIDER_DANA_HOTEL_VOUCHER_CAPTURE','').upper() == 'TRUE': # refer 34430
            self.ins_capture_base.bln_consider_dana_hotel_voucher_capture = True
        else :
            self.ins_capture_base.bln_consider_dana_hotel_voucher_capture = False
        
        self.ins_capture_base.bln_multi_currency = True
            
        if self.dct_conf_data.get('AMADEUS_DIFF_FP_SECTION' , '').upper() == 'TRUE':
            iself.ns_capture_base.bln_diif_fp_section = True
        else :
            self.ins_capture_base.bln_diif_fp_section = False
        
        self.ins_capture_base.ticketing_agent_numeric_code_for_ins_voucher = self.dct_conf_data.get('TICKETING_AGENT_NUMERIC_CODE_FOR_INS_VOUCHER','').upper()
        self.ins_capture_base.ticketing_agent_char_code_for_ins_voucher = self.dct_conf_data.get('TICKETING_AGENT_CHAR_CODE_FOR_INS_VOUCHER','').upper()
        self.ins_capture_base.str_traacs_version = self.dct_conf_data.get('TRAACS_VERSION','SAAS').upper()
        
        self.ins_capture_base.dct_conf_data = self.dct_conf_data
        #self.ins_capture_base.dct_voucher_conf_data = self.dct_voucher_conf_data
    
    
    def create_folder_structre(self,str_gds_name):
        ins_folder_base = FolderBase()
        str_current_date_time = datetime.datetime.now().strftime("%B%Y")
        
        if str_gds_name == 'Amadeus' and self.dct_conf_data.get('amadeus_directory','AIR'):
            ins_folder_base.str_directory = self.dct_conf_data.get('amadeus_directory','AIR')
            ins_folder_base.str_parsed_dir_monthly = os.path.join(ins_folder_base.str_directory, 'parsed')
            ins_folder_base.str_parsed_dir = os.path.join(ins_folder_base.str_directory, 'parsed',str_current_date_time)
            ins_folder_base.str_not_parsed_dir = os.path.join(ins_folder_base.str_directory, 'notParsed')
            ins_folder_base.str_no_ticket_files_dir_monthly = os.path.join(ins_folder_base.str_directory, 'noTicketFiles')
            ins_folder_base.str_no_ticket_files_dir = os.path.join(ins_folder_base.str_directory, 'noTicketFiles',str_current_date_time)
            ins_folder_base.str_skipped_imr_files = os.path.join(ins_folder_base.str_directory, 'skippedIMRFiles')
        
        elif str_gds_name == 'Galileo' and self.dct_conf_data.get('galileo_directory','MIR'):
            ins_folder_base.str_directory = self.dct_conf_data.get('galileo_directory','MIR')
            ins_folder_base.str_parsed_dir_monthly = os.path.join(ins_folder_base.str_directory, 'parsed')
            ins_folder_base.str_parsed_dir = os.path.join(ins_folder_base.str_directory, 'parsed',str_current_date_time)
            ins_folder_base.str_not_parsed_dir = os.path.join(ins_folder_base.str_directory, 'notParsed')
            ins_folder_base.str_no_ticket_files_dir_monthly = os.path.join(ins_folder_base.str_directory, 'noTicketFiles')
            ins_folder_base.str_no_ticket_files_dir = os.path.join(ins_folder_base.str_directory, 'noTicketFiles',str_current_date_time)
            ins_folder_base.str_skipped_imr_files = os.path.join(ins_folder_base.str_directory, 'skippedIMRFiles')
        
        elif str_gds_name == 'Sabre' and self.dct_conf_data.get('sabre_directory','PNR'):
            ins_folder_base.str_directory = self.dct_conf_data.get('sabre_directory','PNR')
            ins_folder_base.str_parsed_dir_monthly = os.path.join(ins_folder_base.str_directory, 'parsed')
            ins_folder_base.str_parsed_dir = os.path.join(ins_folder_base.str_directory, 'parsed',str_current_date_time)
            ins_folder_base.str_not_parsed_dir = os.path.join(ins_folder_base.str_directory, 'notParsed')
            ins_folder_base.str_no_ticket_files_dir_monthly = os.path.join(ins_folder_base.str_directory, 'noTicketFiles')
            ins_folder_base.str_no_ticket_files_dir = os.path.join(ins_folder_base.str_directory, 'noTicketFiles',str_current_date_time)
            ins_folder_base.str_skipped_imr_files = os.path.join(ins_folder_base.str_directory, 'skippedIMRFiles')
        
        elif str_gds_name == 'Worldspan' and self.dct_conf_data.get('worldspan_directory','PRT'):
            ins_folder_base.str_directory = self.dct_conf_data.get('worldspan_directory','PRT')
            ins_folder_base.str_parsed_dir_monthly = os.path.join(ins_folder_base.str_directory, 'parsed')
            ins_folder_base.str_parsed_dir = os.path.join(ins_folder_base.str_directory, 'parsed',str_current_date_time)
            ins_folder_base.str_not_parsed_dir = os.path.join(ins_folder_base.str_directory, 'notParsed')
            ins_folder_base.str_no_ticket_files_dir_monthly = os.path.join(ins_folder_base.str_directory, 'noTicketFiles')
            ins_folder_base.str_no_ticket_files_dir = os.path.join(ins_folder_base.str_directory, 'noTicketFiles',str_current_date_time)
            ins_folder_base.str_skipped_imr_files = os.path.join(ins_folder_base.str_directory, 'skippedIMRFiles')
        
        
        return ins_folder_base


    def verify_sso_token_and_connect_tenant_database(self,str_sso_token,str_token_type):
        if str_sso_token :
            str_tenant_id =  ''
#           {'strUserName': 'mohamedharisinqa@gmail.com',
#            'intSessionID': '131',
#            'strTenancyId': '6e2ab21c-8fb5-4bf8-925c-0e2d30c02161',
#            'arrApplicationRole': {'59073316-ab99-4198-97e1-2557d435bf25': {'strApplicationRoleCode': 'BOFFICE','intApplcationLoginId': '3'}},
#            'iat': 1601542806,
#            'exp': 1601546406,
#            'aud': 'Mozilla/5.0 (X11; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0',
#            'sub': '::ffff:10.240.0.42/1601542806561'}
            try:
                dct_temp_token = {}
                dct_temp_token = jwt.decode(str_sso_token, verify=False, algorithm='RS256') #decode the token
            except Exception as msg:
                print ('INVALID_TOKEN_PROVIDED..'+str(msg))
                return 'INVALID_TOKEN_PROVIDED'+str(msg)
            
            if all(key in dct_temp_token for key in ('intSessionID', 'arrApplicationRole','strTenancyId','exp','iat','aud')): 
                str_tenant_id = dct_temp_token['strTenancyId']
            else: 
                print ('INTIAL_TOKEN_DECODE_FAILED..')
                return 'INVALID_TOKEN_PROVIDED_INTIAL_TOKEN_DECODE_FAILED'

                
            dct_tenant_data = self.get_tenant_data_from_redis(str_tenant_id)
#            print ('*******',dct_tenant_data,'**************')
            if not dct_tenant_data:
                print ('TENANT_DATA_MISSING')
                return 'INVALID_TOKEN_PROVIDED_TENANT_DATA_MISSING'
            
            ####################
#            dct_tenant_data = {}
#            self.connect_db(dct_tenant_data)
#            return 'VALID_TOKEN'
            ####################
            #check saas application is permitted or not
            if not dct_tenant_data['str_application_id'] in dct_temp_token['arrApplicationRole']:
                print ('APPLICATION_NOT_ALLOWED')
                return 'INVALID_TOKEN_PROVIDED_APPLICATION_NOT_ALLOWED'
            dct_options = {
                        'verify_signature': True,
                        'verify_exp': True,
                        'verify_nbf': False,
                        'verify_iat': True,
                        'verify_aud': False,
                        'require_exp': False,
                        'require_iat': False,
                        'require_nbf': False
                        }
#            dct_options = {
#                        'verify_signature': False,
#                        'verify_exp': False,
#                        'verify_nbf': False,
#                        'verify_iat': False,
#                        'verify_aud': False,
#                        'require_exp': False,
#                        'require_iat': False,
#                        'require_nbf': False
#                        }
            try:
                if str_token_type == 'ACCESS':
                    dct_token = jwt.decode(str_sso_token, key=dct_tenant_data['str_access_public_secret_key'], algorithm='RS256',options=dct_options) #verify access token
                else:
                    dct_token = jwt.decode(str_sso_token, key=dct_tenant_data['str_refresh_public_secret_key'], algorithm='RS256',options=dct_options) #verify refresh token
            except Exception as msg:
                print ('INVALID_TOKEN_PROVIDED..'+str(msg))
                return 'INVALID_TOKEN_PROVIDED'+str(msg)
            else:
                #connect tenant to its database
                self.connect_db(dct_tenant_data)
                return 'VALID_TOKEN'
            
        else:
            print ('NO_TOKEN_FIND')
            return 'INVALID_TOKEN_PROVIDED_NO_TOKEN_FIND'
    
    def get_tenant_data_from_redis(self,str_tenant_id):
        #connect to Redis and get the tenat data
        dct_redis_data = {}
        r = redis.StrictRedis(host=self.dct_conf_data['REDIS_HOST'], port=self.dct_conf_data['REDIS_PORT'])
        str_application_id = r.get('application_guid:LITE').decode('utf8')
        dct_redis_data = {
                    'str_application_id'            : str_application_id,
                    'str_access_public_secret_key'  : r.get('tenant_uuid:'+str_tenant_id+':access_rsa_pubkey').decode('utf8'),
                    'str_refresh_public_secret_key' : r.get('tenant_uuid:'+str_tenant_id+':refresh_rsa_pubkey').decode('utf8'),
                    'db_name'                       : r.get('tenant_uuid:'+str_tenant_id+':app:'+str_application_id+':db').decode('utf8'),
                    'db_host'                       : r.get('tenant_uuid:'+str_tenant_id+':app:'+str_application_id+':db_host').decode('utf8'),
                    'db_port'                       : r.get('tenant_uuid:'+str_tenant_id+':app:'+str_application_id+':db_port').decode('utf8'),
                    'db_user'                       : r.get('tenant_uuid:'+str_tenant_id+':app:'+str_application_id+':db_user').decode('utf8'),
                    'db_password'                   : r.get('tenant_uuid:'+str_tenant_id+':app:'+str_application_id+':db_pass').decode('utf8')
                    }
        
        return dct_redis_data
    
    def get_voucher_configuration_and_masking_settings(self,):
        dct_masking_conf_data = {}
        str_voucher_settings = self.get_admin_settings('VOUCHER_CAPTURE_SETTINGS')
        str_masking_settings = self.get_admin_settings('MASKING_SETTINGS')
        
        self.ins_auto_inv.bln_voucher_capture = True
        self.ins_capture_base.dct_voucher_conf_data = json.loads(str_voucher_settings.replace('\n','').replace('\t',''))
        
        dct_masking_conf_data = json.loads(str_masking_settings.replace('\n','').replace('\t',''))
        self.ins_capture_base.int_mask_start_amadeus = int(dct_masking_conf_data['AMADEUS'].get('MASK_START_AM', 0))
        self.ins_capture_base.int_mask_char_count_amadeus = int(dct_masking_conf_data['AMADEUS'].get('MASK_CHAR_COUNT_AM', 0))
        self.ins_capture_base.str_mask_char_amadeus = dct_masking_conf_data['AMADEUS'].get('MASK_CHAR_AM', 0)
        
        self.ins_capture_base.int_mask_start_galileo = int(dct_masking_conf_data['GALILEO'].get('MASK_START_GAL', 0))
        self.ins_capture_base.int_mask_char_count_galileo = int(dct_masking_conf_data['GALILEO'].get('MASK_CHAR_COUNT_GAL', 0))
        self.ins_capture_base.str_mask_char_galileo = dct_masking_conf_data['GALILEO'].get('MASK_CHAR_GAL', 0)
        
        self.ins_capture_base.int_mask_start_sabre = int(dct_masking_conf_data['SABRE'].get('MASK_START', 0))
        self.ins_capture_base.int_mask_char_count_sabre = int(dct_masking_conf_data['SABRE'].get('MASK_CHAR_COUNT', 0))
        self.ins_capture_base.str_mask_char_sabre = dct_masking_conf_data['SABRE'].get('MASK_CHAR', 0)
        
        
    def get_airline_data(self, str_ticketing_airline_numeric_code, str_ticketing_airline_character_code):
        cr = self.create_cursor()
        str_where = ''
        if str_ticketing_airline_character_code :
            str_where = " AND vchr_airline_chr_code = '%s' "%(str_ticketing_airline_character_code)
            if str_ticketing_airline_numeric_code :
                str_where += " AND vchr_airline_numeric_code = '%s'  "%(str_ticketing_airline_numeric_code)        
        elif str_ticketing_airline_numeric_code:
            str_where = " AND vchr_airline_numeric_code = '%s' "%(str_ticketing_airline_numeric_code)
        else:
            return None        
        cr.execute("""SELECT pk_bint_airline_id,
                            fk_bint_airline_account_id

                     FROM tbl_airline
                     WHERE chr_document_status = 'N' %s"""%(str_where))

        rst = cr.fetchone()
        cr.close()
        return rst
    
    def get_original_issue_tickets(self, str_original_issue):
        lst_original_ticket_details = []
        cr = self.create_cursor()

        if str_original_issue:
            while str_original_issue:
                cr.execute("""SELECT vchr_original_issue ,
                                         dat_refund
                                        FROM tbl_ticket
                                        WHERE vchr_ticket_number = %s
                                        AND vchr_ticket_number <> vchr_original_issue """ ,(str_original_issue,))

                rst_data = cr.fetchone()
                str_temp_original_issue = str_original_issue
                str_original_issue = ''
                dat_refud = None
                if rst_data:
                    dat_refud = rst_data['dat_refund']
                    if not dat_refud:
                        lst_original_ticket_details.append(str_temp_original_issue)
                        
                    if rst_data['vchr_original_issue'] and rst_data['vchr_original_issue'] not in lst_original_ticket_details :
                        str_original_issue = rst_data['vchr_original_issue']
        cr.close()
        return lst_original_ticket_details


    def get_ticket_details(self, str_ticket_number):
        cr = self.create_cursor()

        cr.execute("""SELECT *
                     FROM tbl_ticket
                     WHERE vchr_ticket_number = %s""",(str_ticket_number, ))

        rst = cr.fetchall()
        cr.close()
        return rst
    
    def get_hotel_voucher_details(self, str_voucher_number):
        ## Refer 13823
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_hotel_voucher_id
                     FROM tbl_hotel_voucher
                     WHERE vchr_voucher_number = %s""",(str_voucher_number, ))

        rst = cr.fetchone()
        if rst :
            cr.close()
            return rst['pk_bint_hotel_voucher_id']
      
        else :
            return None
        
    def get_hotel_voucher_sales_id(self, str_voucher_number):
        ## Refer 13823
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_hotel_voucher_sales_id
                     FROM tbl_hotel_voucher_sales
                     WHERE vchr_voucher_number = %s""",(str_voucher_number, ))

        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_hotel_voucher_sales_id']
        else :
            return None

##    def get_car_voucher_details(self, str_voucher_number):
##        cr = self.create_cursor()
##
##        cr.execute("""SELECT pk_bint_car_voucher_id
#                     FROM tbl_car_voucher
#                     WHERE vchr_voucher_number = %s#""",(str_voucher_number, ))
##
##        rst = cr.fetchone()
##        cr.close()
##        if rst :
##            return rst['pk_bint_car_voucher_id']
##        else :
##            return None

    def get_other_voucher_details(self, str_voucher_number):
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_other_voucher_id
                     FROM tbl_other_voucher
                     WHERE vchr_voucher_number = %s""",(str_voucher_number, ))

        rst = cr.fetchone()
        if rst :
            cr.close()
            return rst['pk_bint_other_voucher_id']
        else : 
            return None
        
    def get_region_details(self):
        cr = self.create_cursor()
        dct_region = {}
        cr.execute("""SELECT pk_bint_airports_regions_id,
                             vchr_region_code,
                             vchr_list_of_airports
                      FROM tbl_airports_regions
                      WHERE chr_document_status = 'N' """)
        rst = cr.fetchall()
        cr.close()
        
        for record in rst:
            dct_region[record['vchr_region_code']] = record['vchr_list_of_airports'] or ''
            
        return dct_region    
        
    
    
    def get_account_type(self,master_id): # refer 20879
        int_account_type = None
        account_code = master_id
        cr = self.create_cursor()
        str_query = """SELECT  int_account_type
                        FROM tbl_account
                        WHERE pk_bint_account_id = '%s'
                        AND chr_document_status = 'N'
                        """%(account_code)
        cr.execute(str_query)
        rst = cr.fetchone()
        cr.close()
        if rst:
             int_account_type = rst['int_account_type']
             pass
        return  int_account_type
    
    def get_credit_card_pos_id(self, str_credit_card_pos_id): # refer 20879
        
        int_credit_card_pos_id = None
        cr = self.create_cursor()
        if str_credit_card_pos_id:
            
            str_query = """SELECT pk_bint_credit_card_pos_id
                        FROM tbl_credit_card_pos
                        WHERE 
                                vchr_credit_card_pos_code = '%s'
                            AND chr_document_status = 'N'
                        """%(str_credit_card_pos_id)
            
            
            cr.execute(str_query)
            rst = cr.fetchone()
            if rst:
                int_credit_card_pos_id = rst['pk_bint_credit_card_pos_id']
                
                pass
            pass
        cr.close()
        return int_credit_card_pos_id 
    
    def get_supplier_account_id_from_iata_num(self,str_iata_num) :
        """ refs #17717"""
        cr = self.create_cursor()
        
        cr.execute(""" SELECT ac.pk_bint_account_id
                                FROM tbl_account  ac 
                                INNER JOIN tbl_partner pt 
                                        ON ac.pk_bint_account_id = pt.fk_bint_partner_account_id
                                        AND pt.chr_document_status = 'N'
                                INNER JOIN tbl_supplier_info si
                                        ON pt.pk_bint_partner_id = si.fk_bint_partner_id
                                WHERE substring(si.vchr_iata_no,1,7) = %s
                                        AND ac.chr_document_status = 'N' """,
                       
                         (str_iata_num[0:7],))
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_account_id']
        
        return None
    
    
    def generate_valid_date(self, str_date_string, str_issue_date = None,str_date_with_year = ''):
        dct_month = {'JAN': 1, 'FEB': 2, 'MAR': 3,'APR': 4,
                     'MAY': 5,'JUN': 6, 'JUL': 7,'AUG': 8,
                     'SEP': 9,'OCT': 10,'NOV': 11,'DEC': 12}

        str_date = None

        int_current_year = time.localtime().tm_year
        int_current_month = time.localtime().tm_mon
        int_current_day = time.localtime().tm_mday  ## Refs 17035
        if str_issue_date:
            try:
                int_current_year = int(str_issue_date.split('/')[2])
                int_current_month = int(str_issue_date.split('/')[1])
                int_current_day = int(str_issue_date.split('/')[0])
            except:
                pass
            pass
        
        try:
            if str_date_with_year: #format - ddmmyy
                int_current_year = str(int_current_year)[:2] + str_date_with_year[5:7]
                if not str_date_string[5:7]:
                    int_current_year = int(int_current_year[:4])
        except:
            pass
        
        try:
            if str_date_string[2:] in dct_month:
                int_day = int(str_date_string[:2])
                int_month = dct_month[str_date_string[2:]]
                if not str_date_with_year and ((int_month < int_current_month) or (int_month == int_current_month and int_current_day > int_day)):
                    int_year = int_current_year + 1
                else:
                    int_year = int_current_year
                    pass
            else:
                int_day = str_date_string[4:6]
                int_month = str_date_string[2:4]
                int_year = str(int_current_year)[:2] + str_date_string[:2]
                pass
            str_date = str(int_day).zfill(2) + "/" + str(int_month).zfill(2) + "/" + str(int_year).zfill(4)
        except:
            pass

        try:
            tm = time.strptime(str_date, "%d/%m/%Y")
        except:
            str_date = None
            pass

        return str_date
    
    def generate_valid_booking_date(self,str_booking_date):
        str_year  = str_booking_date[:2]
        str_month = str_booking_date[2:4]
        str_day   = str_booking_date[4:]
        return str_day+'/'+str_month+'/20'+str_year
    
    
        
    ############################################################################################
    def get_roe_of_currency_for_a_date(self,str_currency_code, str_date):
        
        
        if str_currency_code == self.str_base_currency :
            return 1
                
        cr = self.create_cursor()
        
        cr.execute("""SELECT dbl_rate_of_exchange AS dbl_roe 
                            FROM tbl_rate_of_exchange tr
                                INNER JOIN tbl_currency tc
                                ON tc.pk_bint_currency_id = tr.fk_bint_currency_id
                                AND tc.chr_document_status = 'N'
                            WHERE tr.dat_effective <= %s
                                AND tc.vchr_currency_code = %s
                            ORDER BY tr.dat_effective DESC
                            """,
                             (str_date,str_currency_code))
        rst = cr.fetchone()
        cr.close()
        if rst and rst['dbl_roe']:
            return rst['dbl_roe']
        else:
            msg = 'Please Set ROE of %s for %s'%(str_currency_code,str_date)
            print(msg)
            raise Exception(msg)
    
    def get_admin_settings(self,str_settings):
        """ Pass the settings string into this function and get the settings value"""
        
        cr = self.create_cursor()
        
        cr.execute("""SELECT  vchr_settings_value
                            FROM tbl_settings 
                            WHERE chr_document_status = 'N'
                            AND vchr_settings_name = %s""",(str_settings,))
                             
        rst = cr.fetchone()
        cr.close()
        if rst and rst['vchr_settings_value']:
            return rst['vchr_settings_value']
        else:
            if str_settings == 'AMADEUSC3XOPT1':
                return 'Location Code'
            elif str_settings == 'AMADEUSC3XOPT2':
                return 'Salesman Code'
            return ''
    
        
    def set_capturing_settings_dictionary(self):
        # // SET CAPTURING FIELDS
        self.dct_capturing_settings = {}
        cr = self.create_cursor()

        cr.execute("""SELECT vchr_system_attribute_name,
                             vchr_amadeus_code,
                             vchr_galileo_code,
                             vchr_sabre_code,
                             vchr_worldspan_code,
                             bln_mandatory
                        FROM tbl_capturing_fields_settings
                        WHERE int_entry_type IN (0,1)
                        AND chr_status = 'N' """)

        rst = cr.fetchall()
        cr.close()
        for record in rst :
            str_attribute_name  = record['vchr_system_attribute_name']
            str_amadeus_code  = record['vchr_amadeus_code']
            str_galileo_code = record['vchr_galileo_code']
            str_sabre_code = record['vchr_sabre_code']
            str_worldspan_code = record['vchr_worldspan_code']
            bln_mandatory = record['bln_mandatory']

            self.dct_capturing_settings[str_attribute_name] = [str_amadeus_code,
                                                                str_galileo_code,
                                                                    str_sabre_code,
                                                                    str_worldspan_code,
                                                                            bln_mandatory]
            pass

        pass
    
    def set_optional_field_data(self):
        #set dynamically adding user derined remark
        dct_opt_data = {}
        
        
        self.dct_amadeus_optional_fields = {}
        self.dct_galileo_optional_fields = {}
        self.dct_worldspan_optional_fields = {}
        self.dct_sabre_optional_fields = {}
        self.dct_fare_logix_optional_fields = {}
        
        cr = self.create_cursor()
        cr.execute("""SELECT vchr_system_attribute_name,
                             vchr_amadeus_code,
                             vchr_galileo_code,
                             vchr_sabre_code,
                             vchr_worldspan_code,
                             vchr_farelogix_code
                      FROM tbl_capturing_fields_settings
                      WHERE int_entry_type = 2
                      AND chr_status = 'N' """)
        rst_opt_fields = cr.fetchall()

        for record in rst_opt_fields:
            if record['vchr_amadeus_code'] or record['vchr_galileo_code'] or record['vchr_sabre_code'] or record['vchr_worldspan_code'] or record['vchr_farelogix_code']:
                self.dct_amadeus_optional_fields[record['vchr_amadeus_code']] = record['vchr_system_attribute_name']
                self.dct_galileo_optional_fields[record['vchr_galileo_code']] = record['vchr_system_attribute_name']
                self.dct_worldspan_optional_fields[record['vchr_worldspan_code']] = record['vchr_system_attribute_name']
                self.dct_sabre_optional_fields[record['vchr_sabre_code']] = record['vchr_system_attribute_name']
                self.dct_fare_logix_optional_fields[record['vchr_farelogix_code']] = record['vchr_system_attribute_name']
        cr.close()
    
    def set_airplus_field_data(self):
        
        self.dct_airplus_capturing_settings = {
                                        'CUSTOMER CODE'  : ['RM CUST','RM CUST','RM CUST','RM CUST',False],
                                        'COST CENTRE'    : ['RM COST CENTER','RM COST CENTER','RM COST CENTER','RM COST CENTER',False],
                                        'PROJECT CODE'   : ['RM PROJECT CODE','RM PROJECT CODE','RM PROJECT CODE','RM PROJECT CODE',False],
                                        'EMPLOYEE ID'    : ['RM EMPID','RM EMPID','RM EMPID','RM EMPID',False],
                                        'DEPARTMENT CODE': ['RM DEPTCODE','RM DEPTCODE','RM DEPTCODE','RM DEPTCODE',True],
                                        'ACTION NO'       : ['RM ACTION NO','RM ACTION NO','RM ACTION NO','RM ACTION NO',True],
                                        'INTERNAL ACCOUNT NO' : ['RM INTACCNO','RM INTACCNO','RM INTACCNO','RM INTACCNO',False],
                                        'SECRETARY' : ['RM SECRETARY','RM SECRETARY','RM SECRETARY','RM SECRETARY',False],
                                        'TRIP REASON' : ['RM TRIP REASON','RM TRIP REASON','RM TRIP REASON','RM TRIP REASON',False],
                                        'CARD EXPIRY DATE' : ['RM CARD EXP DATE','RM CARD EXP DATE','RM CARD EXP DATE','RM CARD EXP DATE',False],
                                        'RESOURCE CODE' : ['RM RESOURCE CODE','RM RESOURCE CODE','RM RESOURCE CODE','RM RESOURCE CODE',False],
                                        'LPO NO' : ['RM LPO NO','RM LPO NO','RM LPO NO','RM LPO NO',True],
                                        'PA' : ['RM PA','RM PA','RM PA','RM PA',False],
                                        'SERVICE FEE' : ['RM SERVICE FEE','RM SERVICE FEE','RM SERVICE FEE','RM SERVICE FEE',False],
                                        'CARD TYPE' : ['RM *CARDTYPE','RM *CARDTYPE','RM *CARDTYPE','RM *CARDTYPE',False],
                                        'ACCOUNTING UNIT' : ['RM ACCOUNTING UNIT','RM ACCOUNTING UNIT','RM ACCOUNTING UNIT','RM ACCOUNTING UNIT',False],
                                        'DEPARTURE DATE' : ['RM DEPARTURE DATE','RM DEPARTURE DATE','RM DEPARTURE DATE','RM DEPARTURE DATE',False],
                                        'ORDER NO' : ['RM ORDER NO','RM ORDER NO','RM ORDER NO','RM ORDER NO',False]
                                      }
        
        
        cr = self.create_cursor()
        cr.execute("""SELECT vchr_system_attribute_name,
                             vchr_amadeus_code,
                             vchr_galileo_code,
                             vchr_sabre_code,
                             vchr_worldspan_code
                      FROM tbl_capturing_fields_settings
                      WHERE int_entry_type = 2 """)
        rst = cr.fetchall()

        for record in rst :
            str_attribute_name  = record['vchr_system_attribute_name']
            str_amadeus_code  = record['vchr_amadeus_code']
            str_galileo_code = record['vchr_galileo_code']
            str_sabre_code = record['vchr_sabre_code']
            str_worldspan_code = record['vchr_worldspan_code']
            
            self.dct_airplus_capturing_settings[str_attribute_name] = [str_amadeus_code,
                                                                str_galileo_code,
                                                                    str_sabre_code,
                                                                    str_worldspan_code
                                                                            ]
        cr.close()
    
    
    #37364
    def get_airline_data_using_id(self, int_airline_id):
        """ Pass airline id  into this function and get the type in the airline details"""
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_airline_id,
                            fk_bint_airline_account_id,
                            vchr_airline_numeric_code,
                            vchr_airline_name,
                            vchr_airline_chr_code

                     FROM tbl_airline
                     WHERE chr_document_status = 'N'
                    AND pk_bint_airline_id = %s """,(int_airline_id,))

        rst = cr.fetchone()
        cr.close()
        if rst:
            return rst
        else:
            return None 
    
    def get_ticket_type_id(self,str_ticket_type):
        """ Pass ticket type into this function and get the id of that type in the tbl_ticket_type"""
        
        try:
            cr = self.create_cursor()

            cr.execute("""SELECT  pk_bint_ticket_type
                                FROM tbl_ticket_type 
                                WHERE vchr_ticket_type = %s
                                AND chr_document_status = 'N' """,(str_ticket_type,))

            rst = cr.fetchone()

            if rst and rst['pk_bint_ticket_type']:
                cr.close()
                return rst['pk_bint_ticket_type']
            else:
                str_ticket_type = str_ticket_type.upper()
                str_current_date_time = self.get_current_date_time()
                int_ticket_type_id = self.get_max_value('tbl_ticket_type','pk_bint_ticket_type')

                cr.execute(""" INSERT INTO tbl_ticket_type
                        ( pk_bint_ticket_type,
                        vchr_ticket_type,
                        chr_document_status,
                        fk_bint_created_user_id,
                        tim_created) 
                    VALUES (%s,%s,%s,%s,%s)""",
                    (int_ticket_type_id,str_ticket_type,'N',2,str_current_date_time))

                cr.execute("""SELECT setval('tbl_ticket_type_pk_bint_ticket_type_seq'::regclass, %s)""",(int_ticket_type_id,))

                cr.execute("""SELECT  pk_bint_ticket_type
                                    FROM tbl_ticket_type 
                                    WHERE vchr_ticket_type = %s
                                    AND chr_document_status = 'N' """,(str_ticket_type,))

                rst = cr.fetchone()
                cr.close()
                if rst and rst['pk_bint_ticket_type']:
                    return rst['pk_bint_ticket_type']

            cr.close()
            return None
        except Exception as msg:
            cr.close()
            print (msg)
            raise
        
    def get_max_value(self, str_table_name, str_field_name):
        """
            This function returns maximum value + 1
            of the field of the table.
            Use this function for asigning the next value
            in this table.
        """
        cr = self.create_cursor()

        cr.execute("""SELECT MAX(%s) AS int_max FROM %s""" %(str_field_name, str_table_name))

        rst = cr.fetchone()
        cr.close()
        if rst and rst['int_max']:
            return rst['int_max'] + 1
        else:
            return 1
        pass

    
    
    def get_region_id(self,str_region_code):
        """ Pass region code into this function and get the region id """
        cr = self.create_cursor()
        
        cr.execute("""SELECT  pk_bint_airports_regions_id
                            FROM tbl_airports_regions 
                            WHERE vchr_region_code = %s
                            AND chr_document_status = 'N'""",(str_region_code,))
                            
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_airports_regions_id']:
            return rst['pk_bint_airports_regions_id']
        else:
            return None
    def get_region_code(self,int_region_id):
        cr = self.create_cursor()
        
        cr.execute("""SELECT  vchr_region_code
                            FROM tbl_airports_regions 
                            WHERE pk_bint_airports_regions_id = %s
                            AND chr_document_status = 'N'""",(int_region_id,))
                            
        rst = cr.fetchone()
        cr.close()
        if rst and rst['vchr_region_code']:
            return rst['vchr_region_code']
        else:
            return ''
        
    def get_ticket_id(self,str_ticket_number):
        """ Pass ticket number into this function and get ticket id """
        cr = self.create_cursor()
        
        cr.execute("""SELECT  pk_bint_ticket_id
                            FROM tbl_ticket 
                            WHERE vchr_ticket_number 	 = %s""",(str_ticket_number,))
                            
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_ticket_id']:
            return rst['pk_bint_ticket_id']
        else:
            return None
        
    
    def get_class_id(self,str_class,int_airline_id= None):
        """ Pass class into this function and get the class id """
        if not str_class.strip() :
            return None,''
        cr = self.create_cursor()
        
        cr.execute("""SELECT  pk_bint_booking_class_id,
                                vchr_booking_class_name 	
        
                            FROM tbl_booking_class 
                            WHERE vchr_list_of_booking_class_types ILIKE %s 
                            AND chr_document_status = 'N'
                            AND (
                                 ( fk_bint_airline_id = %s AND dat_effective <= now()::DATE )
                                 OR NOT EXISTS      (
                                                    SELECT pk_bint_booking_class_id 
                                                    FROM tbl_booking_class
                                                    WHERE fk_bint_airline_id = %s
                                                    AND chr_document_status = 'N'
                                                    AND vchr_list_of_booking_class_types ILIKE %s 
                                                    )
                                    )
                                    
                            ORDER BY now()::DATE-COALESCE(dat_effective,now()::DATE)
                            
                            """,(str_class,int_airline_id,int_airline_id,str_class))
                            
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_booking_class_id']:
            return rst['pk_bint_booking_class_id'],rst['vchr_booking_class_name']
        else:
            return None,''
    

    
    def get_customer_id_using_profile_code(self,str_profile_code):
        cr = self.create_cursor()
        
        cr.execute("""
                    SELECT tp.fk_bint_related_account_id ,
                            tp.pk_bint_profile_id 	, 
                            ta.pk_bint_account_id
                            
                            FROM tbl_account ta
                                LEFT JOIN tbl_profile tp
                                
                                ON ta.pk_bint_account_id = tp.fk_bint_profile_account_id
                                    AND ta.chr_document_status = 'N'
                                    AND tp.chr_document_status = 'N'
                            WHERE ta.vchr_account_code = %s
        
                """,(str_profile_code,))
        
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['fk_bint_related_account_id'],rst['pk_bint_account_id']
        else :
            return None,None
    
    
    def get_country_id(self,str_country):
        
        if not str_country :
            return None
        cr = self.create_cursor()
        
        
        str_query = """ SELECT pk_bint_country_id 
                                FROM tbl_country
                                WHERE UPPER(vchr_country) = '%s'
                                AND chr_document_status	= 'N' """%str_country
                                
        cr.execute(str_query)
        rst = cr.fetchone()
        cr.close()
        if rst not in [[],None,[[]],[None]] :
            return rst['pk_bint_country_id']
        else :
            return None
        pass
    
    def get_city_id(self,str_city_name):
        cr = self.create_cursor()
        
        
        str_query = """SELECT pk_bint_city_id ,
                        fk_bint_country_id
                    FROM tbl_city 
                    WHERE (upper(vchr_city_name) = '%s' AND chr_document_status = 'N') 
                            OR ( upper(vchr_city_name) ILIKE '%s' 
                                AND chr_document_status = 'N'
                                AND NOT EXISTS (SELECT pk_bint_city_id
                                                FROM tbl_city 
                                                WHERE (upper(vchr_city_name) = '%s'
                                                AND chr_document_status = 'N'
                                                )))
                    """%(str_city_name,str_city_name+'%',str_city_name)
        cr.execute(str_query)
                    
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_city_id'] ,rst['fk_bint_country_id']
        else :
            return None,None
        pass
    
    def get_hotel_master_id(self,str_hotel_name , int_city_id):
        
        cr = self.create_cursor()
        
        cr.execute("""
                SELECT pk_bint_hotel_master_id
                    FROM tbl_hotel_master
                    
                    WHERE UPPER(vchr_hotel_name) = %s
                    AND fk_bint_hotel_city_id = %s
                    AND chr_document_status = 'N'
                
                """,(str_hotel_name.upper() , int_city_id))
                
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_hotel_master_id']
        else :
            return None
        
        pass
    
    def get_meals_plan_id(self):
        
        self.dct_meals_plan_id = {}
        cr = self.create_cursor()
        
        cr.execute("""
                    SELECT vchr_meals_plan ,
                    pk_bint_hotel_meals_plan_id
                    FROM tbl_hotel_meals_plan
                    WHERE chr_document_status = 'N'
                        """)
        
        rst = cr.fetchall()
        p = re.compile('[A-Z]')
        cr.close()
        for record in rst :
            
            lst_meals_plan = p.findall(record['vchr_meals_plan'])
            
            if lst_meals_plan :
                self.dct_meals_plan_id[''.join(lst_meals_plan)] = [record['pk_bint_hotel_meals_plan_id'],record['vchr_meals_plan']]
                pass
            
    def get_room_type_id(self,str_room_type):
        
        if not str_room_type :
            return None
        cr = self.create_cursor()
        cr.execute(""" SELECT pk_bint_hotel_room_type_id
                            FROM tbl_hotel_room_type
                            WHERE UPPER(vchr_hotel_room_type) = %s
                            AND chr_document_status = 'N'
                            """,(str_room_type,))
        
        rst = cr.fetchone()
        if rst not in [[],None,[[]],[None]] :
            cr.close()
            return rst['pk_bint_hotel_room_type_id']
        
        else :
            
            try :
                cr.execute("""INSERT INTO tbl_hotel_room_type
                               ( vchr_hotel_room_type,
                                    fk_bint_created_user_id,
                                    tim_created
                               )
                               VALUES
                               (%s,%s,%s)

                                """,(str_room_type,2,self.get_current_date_time()))
            except :
                self.ins_db.rollback()
            else :
                self.ins_db.commit()
                cr.execute(""" SELECT pk_bint_hotel_room_type_id
                            FROM tbl_hotel_room_type
                            WHERE UPPER(vchr_hotel_room_type) = %s
                            AND chr_document_status = 'N'
                            """,(str_room_type,))
        
                rst = cr.fetchone()
                if rst not in [[],None,[[]],[None]] :
                    cr.close()
                    return rst['pk_bint_hotel_room_type_id']
            
            
        cr.close()    
        return None
            
    
    def get_counter_staff_id(self,str_gds,str_counter_staff,str_counter_staff_numeric_code = '',
                    bln_check_agent_numeric_code = False,bln_check_office_id = False,str_office_id = '',bln_ticketing = False): 
        
        if not str_counter_staff :
            return None,'',None,None,None,None,'',''
        
        # Numeric code not used now.
        cr = self.create_cursor()
        str_numeric_code = ''
        if str_gds == 'Amadeus' :
            str_gds = "vchr_amadeus_code"
            str_numeric_code = "vchr_amadeus_numeric_code"
        elif str_gds == 'Sabre' :
            str_gds = "vchr_sabre_code"
            str_numeric_code = "vchr_sabre_numeric_code"
        elif str_gds == 'Galileo' :
            str_gds = "vchr_galileo_code"
            str_numeric_code = "vchr_galileo_numeric_code"
        elif str_gds == 'Worldspan' :
            str_gds = "vchr_world_span_code"
            str_numeric_code = "vchr_worldspan_numeric_code"
        elif str_gds == 'Farelogix' :
            str_gds = "vchr_farelogix_code"
            str_numeric_code = "vchr_farelogix_numeric_code"

        str_con_query = ''
        
        if bln_check_agent_numeric_code and str_counter_staff_numeric_code:
            str_con_query += " AND %s = '%s'"%(str_numeric_code,str_counter_staff_numeric_code)
        if bln_check_office_id and str_office_id:

            if bln_ticketing :
                str_con_query += " AND COALESCE((string_to_array(cs.vchr_sabre_office_id,','))[2],(string_to_array(cs.vchr_sabre_office_id,','))[1]) = '%s'"%str_office_id
            else :
                str_con_query+= " AND (string_to_array(cs.vchr_sabre_office_id,','))[1] = '%s'"%str_office_id
        
        str_query = """
                    SELECT pk_bint_counter_staff_id ,
                            %s AS vchr_counter_staff_numeric_code,
                            COALESCE(%s,'') AS vchr_numeric_code ,
                            CASE WHEN COALESCE(%s,'') = '%s'
                                THEN 1
                                ELSE 0
                                END AS int_sort_order,
                            
                            a.vchr_account_code 	,
                            d.fk_bint_branch_id ,
                            
                            cs.fk_bint_branch_id AS fk_bint_cost_center_id_cs
                            
                        FROM tbl_counter_staff cs
                        LEFT JOIN tbl_partner d
                            ON cs.fk_bint_customer_account_id = d.fk_bint_partner_account_id
                            AND d.chr_document_status = 'N'
                        LEFT JOIN tbl_account a
                            ON cs.fk_bint_customer_account_id =  a.pk_bint_account_id
                            AND a.chr_document_status = 'N'
                            
                        
                        WHERE %s = '%s'
                        %s
                        AND cs.chr_document_status = 'N'
                        AND cs.bln_inactive = False
                        ORDER BY int_sort_order DESC
                        
                    """%(str_numeric_code,
                            str_numeric_code,
                            str_numeric_code,
                            str_counter_staff_numeric_code, 
                            str_gds ,
                            str_counter_staff,
                            str_con_query)
        
        cr.execute(str_query)
                    
        rst = cr.fetchone()
        if rst :
            ### Refer #13142
            if str_gds == 'vchr_amadeus_code' and str_counter_staff_numeric_code and not rst['vchr_numeric_code'] :
                
                try :
                    str_update_query = """
                                        UPDATE tbl_counter_staff 
                                            SET vchr_amadeus_numeric_code = '%s'
                                            WHERE pk_bint_counter_staff_id = %s
                                        """%(str_counter_staff_numeric_code,rst['pk_bint_counter_staff_id'])
                    cr.execute(str_update_query)
                    pass
                except Exception:
                    self.ins_db.rollback()
                else :
                    self.ins_db.commit()
                    pass
            cr.close()    
            return ( rst['pk_bint_counter_staff_id'],
                        rst['vchr_account_code'] or '',
                            None ,
                                rst['fk_bint_branch_id'],
                                    rst['fk_bint_cost_center_id_cs'],
                                        None,
                                        rst['vchr_counter_staff_numeric_code'] or '',
                                        None)
        else :

            cr.close()
            return None,'',None,None,None,None,'',''
        
        
    ## Intensionally let not deleted.
#    def set_counter_staff_id(self,): 
#        self.dct_amadeus_counter_staff = {}
#        self.dct_sabre_counter_staff = {}
#        self.dct_galileo_counter_staff = {}
#        self.dct_worldspan_counter_staff = {}
#        
#        
#        cr = self.create_cursor()
#        
#        str_query = """
#                    SELECT pk_bint_counter_staff_id ,
#                            vchr_amadeus_numeric_code ,
#                            vchr_galileo_numeric_code  ,
#                            vchr_sabre_numeric_code ,
#                            vchr_worldspan_numeric_code ,
#                            vchr_amadeus_code ,
#                            vchr_galileo_code ,
#                            vchr_sabre_code ,
#                            vchr_world_span_code 
#                            
#                        FROM tbl_counter_staff
#                        WHERE chr_document_status = 'N'
#                        
#                    """
#        
#        cr.execute(str_query)
#                    
#        rst = cr.fetchall()
#        for record in rst :
#            if record['vchr_amadeus_code'] :
#                self.dct_amadeus_counter_staff[(record['vchr_amadeus_code'],record['vchr_amadeus_numeric_code'])] = record['pk_bint_counter_staff_id']
#                self.dct_amadeus_counter_staff[record['vchr_amadeus_code']] = record['pk_bint_counter_staff_id']
#                
#            if record['vchr_sabre_code'] :
#                self.dct_sabre_counter_staff[(record['vchr_sabre_code'],record['vchr_sabre_numeric_code'])] = record['pk_bint_counter_staff_id']
#                self.dct_sabre_counter_staff[record['vchr_sabre_code']] = record['pk_bint_counter_staff_id']
#            
#            if record['vchr_galileo_code'] :
#                self.dct_galileo_counter_staff[(record['vchr_galileo_code'],record['vchr_galileo_numeric_code'])] = record['pk_bint_counter_staff_id']
#                self.dct_galileo_counter_staff[record['vchr_galileo_code']] = record['pk_bint_counter_staff_id']
#                
#            if record['vchr_world_span_code'] :    
#                self.dct_worldspan_counter_staff[(record['vchr_world_span_code'],record['vchr_worldspan_numeric_code'])] = record['pk_bint_counter_staff_id']
#                self.dct_worldspan_counter_staff[record['vchr_world_span_code']] = record['pk_bint_counter_staff_id']
#            
#    
    def get_system_mapping_account_id(self,str_mapping_code):
        
        cr = self.create_cursor()
        
        cr.execute("""
                    SELECT fk_bint_account_id 
                            FROM tbl_system_mapping_accounts tsm
                            WHERE tsm.vchr_system_mapping_code = %s
                            AND tsm.chr_document_status = 'N'

                    """,(str_mapping_code,))

        rst = cr.fetchone()
        cr.close()    
        if rst :
            return rst['fk_bint_account_id']
        else :
            return None
    
    
    def get_service_id(self,str_service_type):
        
        cr = self.create_cursor()
        
        cr.execute("""
                        SELECT pk_bint_service_id
                            FROM tbl_service 
                            WHERE (UPPER(vchr_service_code) = %s OR UPPER(vchr_service_name) = %s)
                            AND chr_document_status = 'N'
                        """,(str_service_type.upper(),str_service_type.upper()))
                        
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_service_id']
        else :
            return None
        
        pass
    
    def get_sector_for_emd_tickets(self,str_connection_ticket_number):
        """  Definition to get sector  of a ticket invoiced  #35976"""
        
        cr = self.create_cursor()
        cr.execute("""
                        SELECT  vchr_sector_inv,
                                fk_bint_airports_regions_id
                            FROM tbl_ticket 
                            WHERE vchr_ticket_number = '%s'
                           
                        """%(str_connection_ticket_number))
                        
        rst = cr.fetchone()
        cr.close()
        lst_emd_ticket_details = [] #38326
        if rst:
            str_emd_region_code = self.get_region_code(rst['fk_bint_airports_regions_id'])
            lst_emd_ticket_details = [rst['vchr_sector_inv'],str_emd_region_code]       
        return lst_emd_ticket_details
        

        
    def convert_amount_data(self, flt_amount, flt_roe = 1, bln_cust_currency = False,flt_cust_currency_roe = 1,int_currency_precision = 2):

        flt_converted_amt = float(flt_amount or 0.0)*float(flt_roe)
        if bln_cust_currency:
            flt_converted_amt = float(flt_converted_amt) / float(flt_cust_currency_roe)

        return round(flt_converted_amt,int_currency_precision)
    
    def convert_foreign_to_based_currency_amt(self, flt_foreign_amt, flt_foreign_roe, int_roe_round = 0):
        if not flt_foreign_roe:
            flt_foreign_roe = 1
            
        flt_base_amt = 0
        if self.str_currency_conversion_method == 'BASE TO FOREIGN':
            flt_base_amt = float(flt_foreign_amt)/float(flt_foreign_roe)
        else: # // 'FOREIGN TO BASE':
            flt_base_amt = float(flt_foreign_amt)*float(flt_foreign_roe)

#        if int_roe_round == 1:
#            flt_base_amt = round(flt_base_amt, ins_system.int_no_of_decimal)
#        elif int_roe_round == 2:
#            flt_base_amt = ins_system.str_decimal_format%flt_base_amt
#            pass

        return flt_base_amt

    def convert_based_to_foreign_currency_amt(self, flt_base_amt, flt_foreign_roe, int_roe_round = 0):
        if not flt_foreign_roe:
            flt_foreign_roe = 1

        flt_foreign_amt = 0
        if ins_dictionary.dct_select_traacs_settings['ROE_FORMAT'][1] == 'BASE TO FOREIGN':
            flt_foreign_amt = float(flt_base_amt)*float(flt_foreign_roe)
        else: # // 'FOREIGN TO BASE':
            flt_foreign_amt = float(flt_base_amt)/float(flt_foreign_roe)

#        if int_roe_round == 1:
#            flt_foreign_amt = round(flt_foreign_amt, ins_system.int_no_of_decimal)
#        elif int_roe_round == 2:
#            flt_foreign_amt = ins_system.str_decimal_format%flt_foreign_amt
#            pass

        return flt_foreign_amt
    
    def get_supplier_id_by_code(self,str_supplier_code):
        cr = self.create_cursor()
        
        cr.execute("""SELECT pk_bint_account_id
                           FROM tbl_account
                           WHERE vchr_account_code = %s
                           AND chr_document_status = 'N' """,
                             (str_supplier_code,))
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_account_id']:
            return rst['pk_bint_account_id']
        else:
            return None
        
        pass
    
    def get_account_id_by_name(self,str_account_name):
        cr = self.create_cursor()
        
        cr.execute("""SELECT pk_bint_account_id
                           FROM tbl_account
                           WHERE vchr_account_name = %s
                           AND chr_document_status = 'N' """,
                             (str_account_name,))
        rst = cr.fetchone()
        cr.close()    
        if rst and rst['pk_bint_account_id']:
            return rst['pk_bint_account_id']
        else:
            return None
    
    def verify_location_details(self,str_location_code):
        cr = self.create_cursor()
        
        cr.execute("""SELECT  	 pk_bint_branch_id ,
                                fk_bint_company_id
                            FROM tbl_branch 
                            WHERE vchr_branch_code = %s
                            AND chr_document_status = 'N' """,(str_location_code,))
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_branch_id']:
            return rst['pk_bint_branch_id'],(rst['fk_bint_company_id'] or 1 )
        else:
            return None,1
        pass
    
    def verify_department_details(self,str_branch_code):
        return None
        
#        cr = self.create_cursor()
#        
#        cr.execute("""SELECT pk_bint_department_id 
#                            FROM tbl_department 
#                            WHERE vchr_department_code = %s
#                            AND chr_document_status = 'N' #""",(str_branch_code,))
#        rst = cr.fetchone()
#        cr.close()
#        if rst and rst['pk_bint_department_id']:
#            return rst['pk_bint_department_id']
#        else:
#            return None
#        pass
    
    
    def get_destination_airport_code(self,str_sector):
        """ Fucntion to find the destination airport code """
        lst_sector = str_sector.split('/')
        lst_temp = []
        bln_return = False      #40613
        if lst_sector[0] and lst_sector[0] == lst_sector[-1]:
            bln_return = True
        for str_air_code in lst_sector :
            if str_air_code :
                if str_air_code  in lst_temp :
                    return lst_temp[-1], bln_return
                else:
                    lst_temp.append(str_air_code)
        if lst_temp :
            return lst_temp[-1], bln_return
        else : return '', bln_return
    
    
    
    def get_current_date_time2(self, sql_cursor = None):
        """get_current_date_time() -> DateTime
           returns the current DateTime object
        
        class DateTime(corelib.general.datetime.DateTime):
            def get_current_date_time(self):
                cr = self.ins_database.cursor()
                return corelib.general.datetime.DateTime.get_current_date_time(self, cr)
        """
        cr = self.create_cursor()
        #cr.execute("""select CAST ( timeofday() AS timestamp)""")
        cr.execute("""select now()""")
        tim_date = cr.fetchone()[0]
        cr.close()
        return tim_date
    
    def get_corporate_card_id(self,str_card_number,str_gds_name = ''):
        """Get corp card id and type"""
        
        str_card_number = str_card_number.strip() #43988
        if not str_card_number :
            return None,None
        
        if str_gds_name.upper() == 'AMADEUS' :
            int_mask_start = self.ins_capture_base.int_mask_start_amadeus
            int_mask_char_count = self.ins_capture_base.int_mask_char_count_amadeus
            str_mask_char =  self.ins_capture_base.str_mask_char_amadeus
        elif str_gds_name.upper() == 'GALILEO' :
            int_mask_start = self.ins_capture_base.int_mask_start_galileo
            int_mask_char_count = self.ins_capture_base.int_mask_char_count_galileo
            str_mask_char =  self.ins_capture_base.str_mask_char_galileo
        elif str_gds_name.upper() == 'SABRE' :
            int_mask_start = self.ins_capture_base.int_mask_start_sabre
            int_mask_char_count = self.ins_capture_base.int_mask_char_count_sabre
            str_mask_char =  self.ins_capture_base.str_mask_char_sabre
            
        cr = self.create_cursor()
        cr.execute("""  SELECT fk_bint_corporate_card_account_id,
                                int_corporate_card_type
                                
                            FROM tbl_corporate_card
                            WHERE vchr_corporate_card_no = %s
                            AND chr_document_status = 'N'
        
                        """,(str_card_number,))
        rst = cr.fetchone()
        
        if not rst and int_mask_start and int_mask_char_count and str_mask_char:
            
            cr.execute("""  SELECT fk_bint_corporate_card_account_id,
                                int_corporate_card_type
                                
                            FROM tbl_corporate_card
                            WHERE substring(vchr_corporate_card_no from 1 for %s ) = %s
                                AND substring(vchr_corporate_card_no from %s) = %s
                            
                            
                            AND chr_document_status = 'N'
        
                        """,(int_mask_start-1,str_card_number[:int_mask_start-1],int_mask_start+int_mask_char_count,str_card_number[int_mask_start+int_mask_char_count-1:].strip()))
                        
                        
#                        substring(mcard from 1 for 6 )
            rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['fk_bint_corporate_card_account_id'],rst['int_corporate_card_type']
        return None,None # to be removed
    
####        else:
####            ## refer #15466 &#39593
####            cr.execute("""
###                            SELECT fk_bint_customer_card_account_id 
###                            FROM tbl_customer_corporate_card
###                            WHERE vchr_card_no = %s
###                            AND chr_document_status = 'N'
###                            #""",(str_card_number,))
####            rst = cr.fetchone()
####            
####            if not rst and int_mask_start and int_mask_char_count and str_mask_char:
####                cr.execute("""
###                            SELECT fk_bint_customer_card_account_id 
###                            FROM tbl_customer_corporate_card
###                            WHERE substring(vchr_card_no from 1 for %s ) = %s
###                                AND substring(vchr_card_no from %s) = %s
###                            
###                            AND chr_document_status = 'N'
##                            """,(int_mask_start-1,str_card_number[:int_mask_start-1],int_mask_start+int_mask_char_count,str_card_number[int_mask_start+int_mask_char_count-1:]))
#                rst = cr.fetchone()
#            
#            
#            cr.close()
#            if rst :
#                return rst['fk_bint_customer_card_account_id'],1
#            else :
#                return None,None
    
    def get_corp_card_id(self,str_account_code):
        cr = self.create_cursor()
    
        cr.execute("""  SELECT pk_bint_account_id
                                
                            FROM tbl_account
                            WHERE vchr_account_code = %s
                            AND chr_document_status = 'N'
                            AND int_account_type = 6
        
                        """,(str_account_code,))
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_account_id']
        else :
            return None
        
    def get_customer_id_using_card_no(self,str_card_number,str_gds_name = ''): 
        return None # need to remove
        # Refer 13997
        if not str_card_number.strip() :
            return None
        
        if str_gds_name.upper() == 'AMADEUS' :
            int_mask_start = self.ins_capture_base.int_mask_start_amadeus
            int_mask_char_count = self.ins_capture_base.int_mask_char_count_amadeus
            str_mask_char =  self.ins_capture_base.str_mask_char_amadeus
        elif str_gds_name.upper() == 'GALILEO' :
            int_mask_start = self.ins_capture_base.int_mask_start_galileo
            int_mask_char_count = self.ins_capture_base.int_mask_char_count_galileo
            str_mask_char =  self.ins_capture_base.str_mask_char_galileo
        elif str_gds_name.upper() == 'SABRE' :
            int_mask_start = self.ins_capture_base.int_mask_start_sabre
            int_mask_char_count = self.ins_capture_base.int_mask_char_count_sabre
            str_mask_char =  self.ins_capture_base.str_mask_char_sabre
            
        cr = self.create_cursor()
        
        #refer #39593 changes related to Airplus
        cr.execute("""
                        SELECT dt.fk_bint_debtor_account_id
                        FROM tbl_debtor dt
                        INNER JOIN tbl_customer_corporate_card cc
                            ON cc.fk_bint_debtor_id = dt.pk_bint_debtor_id
                        WHERE cc.vchr_card_no = %s
                            AND cc.chr_document_status = 'N'
                            AND dt.chr_document_status = 'N'
                        """,(str_card_number,))
        rst = cr.fetchone()

        if not rst and int_mask_start and int_mask_char_count and str_mask_char:
            cr.execute("""
                        SELECT dt.fk_bint_debtor_account_id
                        FROM tbl_debtor dt
                        INNER JOIN tbl_customer_corporate_card cc
                            ON cc.fk_bint_debtor_id = dt.pk_bint_debtor_id
                        WHERE substring(cc.vchr_card_no from 1 for %s ) = %s
                            AND substring(cc.vchr_card_no from %s) = %s
                            AND cc.chr_document_status = 'N'
                            AND dt.chr_document_status = 'N'
                        """,(int_mask_start-1,str_card_number[:int_mask_start-1],int_mask_start+int_mask_char_count,str_card_number[int_mask_start+int_mask_char_count-1:]))

            rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['fk_bint_debtor_account_id']
        else :
            return None
            

    def get_current_date_time(self,):
        cr = self.create_cursor()
        cr.execute("""select timeofday()""")
        tim_date = cr.fetchone()[0] # 'Fri Nov 30 16:57:11.605984 2012 IST'
        # // Process
        lst_date = tim_date.split(' ')
        lst_time = lst_date[3].split(':')
        str_date_time = lst_date[2] + '/' + lst_date[1] + '/' + lst_date[4] + ' ' + lst_time[0] + ':' + lst_time[1] + ':' + lst_time[2].split('.')[0]
        tim_date = datetime.datetime.strptime(str_date_time, '%d/%b/%Y %H:%M:%S')
        cr.close()
        return tim_date
    
#    def strptime(self, str_date, str_format="%d/%m/%Y"):
#        tm = time.strptime(str_date, str_format)
#        print ('ERRORRRRRRRRRRR')
#        return 0
#        return mx.DateTime.DateTime(tm.tm_year,
#                                    tm.tm_mon,
#                                    tm.tm_mday,
#                                    tm.tm_hour,
#                                    tm.tm_min,
#                                    tm.tm_sec)
                                    
    def get_product_master_id(self,str_product_code ):
        
        if not str_product_code.strip() :
            return None
        
        cr = self.create_cursor()
        cr.execute("""SELECT  	pk_bint_product_master_id 
                                FROM tbl_product_master
                                WHERE vchr_product_code = %s""",(str_product_code,))
                                
        rst = cr.fetchone()
        cr.close()
        if rst:
            return rst['pk_bint_product_master_id']
        else:
            return None
        
    ## Non IATA capture get details from settings table
    
    def set_non_iata_capture_details(self,):
        
        cr = self.create_cursor()
        
        self.bln_enable_non_iata_capture = False
        self.dct_non_iata_capture_settings = {}
        self.lst_non_iata_instance = []
        
        
        cr.execute(""" SELECT pk_bint_non_iata_capturing_settings_id,
                                vchr_supplier_code,
                                vchr_url,
                                int_port,
                                vchr_user_name,
                                vchr_password,
                                bln_sign_in_code,
                                bln_office_id,
                                bln_counter_staff,
                                bln_customer,
                                vchr_gds ,
                                vchr_sign_in_code_numeric, 
                                vchr_sign_in_code_charachter,
                                vchr_office_id,
                                fk_bint_counter_staff_id,
                                fk_bint_customer_id,
                                int_record_type,
                                vchr_ticketing_office_id
                                
                        FROM tbl_non_iata_capturing_settings 
                            INNER JOIN tbl_non_iata_capturing_settings_details
                                ON pk_bint_non_iata_capturing_settings_id = fk_bint_non_iata_capturing_settings_id
                                
                        WHERE chr_document_status ='N'
                        
                        ORDER BY pk_bint_non_iata_capturing_settings_id ASC
                        
                                
                                    """)
        
        rst = cr.fetchall()
        cr.close()
        if rst and rst not in [[[]],[None]] :
            self.bln_enable_non_iata_capture = True
            int_non_iata_capturing_settings_id = None
            for record in rst :
                
                if not int_non_iata_capturing_settings_id or int_non_iata_capturing_settings_id != record['pk_bint_non_iata_capturing_settings_id'] :
                    ins_non_iata_capture = NonIataCaptureBase()
                else :
                    ins_non_iata_capture = self.lst_non_iata_instance[-1]

                ins_non_iata_capture.str_supplier = record['vchr_supplier_code'] or ''
                ins_non_iata_capture.str_url = (record['vchr_url'] or '') + ':' + str(record['int_port'])+ "/nucorelib/common_api_integration/create_or_update_ticket"
                ins_non_iata_capture.str_user_name = record['vchr_user_name'] or ''
                ins_non_iata_capture.str_password = record['vchr_password'] or ''
                ins_non_iata_capture.int_non_iata_capturing_settings_id = record['pk_bint_non_iata_capturing_settings_id']
                ins_non_iata_capture.bln_sign_in_code = record['bln_sign_in_code']
                ins_non_iata_capture.bln_office_id = record['bln_office_id']
                ins_non_iata_capture.bln_counter_staff = record['bln_counter_staff']
                ins_non_iata_capture.bln_customer = record['bln_customer']
                
                if record['fk_bint_customer_id'] :
                    self.dct_non_iata_capture_settings[('CUSTOMER',record['fk_bint_customer_id'])] = ins_non_iata_capture
                    ins_non_iata_capture.lst_customer.append(('CUSTOMER',record['fk_bint_customer_id']))

                if record['fk_bint_counter_staff_id'] :
                    self.dct_non_iata_capture_settings[('COUNTERSTAFF',record['fk_bint_counter_staff_id'])] = ins_non_iata_capture
                    ins_non_iata_capture.lst_counter_staff.append(('COUNTERSTAFF',record['fk_bint_counter_staff_id']))

                if record['vchr_office_id'] :
                    self.dct_non_iata_capture_settings[('OFFICEID',(record['vchr_gds'] or '').upper(),record['vchr_office_id'])] = ins_non_iata_capture
                    ins_non_iata_capture.lst_office_id.append(('OFFICEID',(record['vchr_gds'] or '').upper(),record['vchr_office_id']))

                if record['vchr_ticketing_office_id']:
                    self.dct_non_iata_capture_settings[('TOFFICEID',(record['vchr_gds'] or '').upper(),record['vchr_ticketing_office_id'])] = ins_non_iata_capture
                    ins_non_iata_capture.lst_office_id.append(('TOFFICEID',(record['vchr_gds'] or '').upper(),record['vchr_ticketing_office_id']))

                if record['vchr_sign_in_code_charachter'] :
                    self.dct_non_iata_capture_settings[('SIGNCODE',(record['vchr_gds'] or '').upper(),(record['vchr_sign_in_code_numeric'] or '').upper(),record['vchr_sign_in_code_charachter'])] = ins_non_iata_capture
                    ins_non_iata_capture.lst_sign_in_code.append(('SIGNCODE',(record['vchr_gds'] or '').upper(),(record['vchr_sign_in_code_numeric'] or '').upper(),(record['vchr_sign_in_code_charachter'] or '').upper()))
                    
                if not int_non_iata_capturing_settings_id or int_non_iata_capturing_settings_id != record['pk_bint_non_iata_capturing_settings_id'] :
                    self.lst_non_iata_instance.append(ins_non_iata_capture)
                    
                int_non_iata_capturing_settings_id = record['pk_bint_non_iata_capturing_settings_id']
                
    def get_non_iata_instance(self,ins_ticket_base):
        
        ins_non_iata_settings = None
        
        for ins_capture_sett in self.lst_non_iata_instance :
            
            bln_return = False
            
            if ins_capture_sett.bln_sign_in_code :
                if not ('SIGNCODE',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agent_numeric_code.upper(),\
                        ins_ticket_base.str_booking_agent_code) in ins_capture_sett.lst_sign_in_code :
                    bln_return = False
                    continue
                bln_return = True
            elif ('SIGNCODE',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agent_numeric_code.upper(),\
                        ins_ticket_base.str_booking_agent_code) in ins_capture_sett.lst_sign_in_code :
                bln_return = True
            
            if ins_capture_sett.bln_office_id :
                if not ('OFFICEID',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agency_office_id) in ins_capture_sett.lst_office_id and not \
                        ('TOFFICEID',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_ticketing_agency_office_id) in ins_capture_sett.lst_office_id :
                    bln_return = False
                    continue
                bln_return = True
            elif ('OFFICEID',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agency_office_id) in ins_capture_sett.lst_office_id or \
                    ('TOFFICEID',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_ticketing_agency_office_id) in ins_capture_sett.lst_office_id:
                bln_return = True

            if ins_capture_sett.bln_counter_staff :
                if not ('COUNTERSTAFF',ins_ticket_base.int_booking_agent_counter_staff_id) in ins_capture_sett.lst_counter_staff :
                    bln_return = False
                    continue
                bln_return = True
            elif ('COUNTERSTAFF',ins_ticket_base.int_booking_agent_counter_staff_id) in ins_capture_sett.lst_counter_staff :
                bln_return = True
            
            if ins_capture_sett.bln_customer :
                if not ('CUSTOMER',ins_ticket_base.int_account_master_id) in ins_capture_sett.lst_customer :
                    bln_return = False
                    continue
                bln_return = True
            elif ('CUSTOMER',ins_ticket_base.int_account_master_id) in ins_capture_sett.lst_customer :
                bln_return = True
                
                
            if bln_return :
                return ins_capture_sett
        
        else :
            return None
        
        return None
#        if self.dct_non_iata_capture_settings.has_key(('CUSTOMER',ins_ticket_base.int_account_master_id)) :
#            ins_non_iata_settings = self.dct_non_iata_capture_settings[('CUSTOMER',ins_ticket_base.int_account_master_id)]
#            
#        elif self.dct_non_iata_capture_settings.has_key(('COUNTERSTAFF',ins_ticket_base.int_booking_agent_counter_staff_id)) :
#            ins_non_iata_settings = self.dct_non_iata_capture_settings[('COUNTERSTAFF',ins_ticket_base.int_booking_agent_counter_staff_id)]
#            
#        elif self.dct_non_iata_capture_settings.has_key(('OFFICEID',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agency_office_id)) :
#            ins_non_iata_settings = self.dct_non_iata_capture_settings[('OFFICEID',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agency_office_id)]
#            
#        elif self.dct_non_iata_capture_settings.has_key(('SIGNCODE',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agent_numeric_code.upper(),ins_ticket_base.str_booking_agent_code)) :
#            ins_non_iata_settings = self.dct_non_iata_capture_settings[('SIGNCODE',ins_ticket_base.str_crs_company.upper(),ins_ticket_base.str_booking_agent_numeric_code.upper(),ins_ticket_base.str_booking_agent_code)]
#            
#        
#        return ins_non_iata_settings
    
    
    
        
    def write_error_message(self,int_agency_settings_id,
                                        int_ticket_id,
                                        str_error_message,
                                            str_type):
        
        cr = self.create_cursor()
        
        try:
            cr.execute("""INSERT INTO tbl_non_iata_capture_log
                        (fk_bint_non_iata_capturing_settings_id,
                            fk_bint_ticket_id,
                            vchr_type,
                            vchr_status,
                            vchr_failed_reason,
                            bln_pending)
                        VALUES
                        (%s, %s, %s, %s, %s, %s)
                        """,(int_agency_settings_id,
                                int_ticket_id,
                                    str_type,
                                        'FAIL',
                                            str_error_message,
                                                True))
        except Exception as msg:
            self.ins_db.rollback()
            cr.close()
            raise Exception("Inserting error message failed ." + str(msg))
        else:
            self.ins_db.commit()
            pass
        cr.close()
        
    def convert_amount(self,flt_amount,flt_currency_roe) :
        """Convert amount to base curency """

        return self.convert_foreign_to_based_currency_amt(flt_amount, flt_currency_roe, int_roe_round = 0)
        pass
    
    def get_original_issue_ticket_segment_count(self,str_original_issue_ticket):
        cr = self.create_cursor()
        
        cr.execute(""" SELECT int_no_of_segments_inv
                            FROM tbl_ticket
                            WHERE vchr_ticket_number = %s
                            """,(str_original_issue_ticket,))
        rst = cr.fetchone()
        cr.close()
        if rst not in [[],None,[[]],[None]] :
            return rst['int_no_of_segments_inv'] or 0
        return 0
    
    
    def get_account_data(self, str_customer_code,str_account_type = ''):
                
        int_account_master_id = None
        int_location_id = None
        int_branch_id = None
        chr_account_type = ''
        str_account_code_inv = ''
        str_account_name_inv = ''
        str_auto_inv_grouping = ''
        int_auto_invoice = 0 #  update tbl_debtor set int_auto_invoice = 0 where int_auto_invoice <> 0

        cr = self.create_cursor()
        
        str_account_type_filter = ''

        if str_account_type :
            str_account_type_filter = " AND int_account_type NOT IN (0,1,2,3,16,17,19) "

        if str_customer_code:
            str_customer_code = str_customer_code.replace("'",'').strip() #refs 38439
            str_query = """SELECT pk_bint_account_id ,
                                  vchr_account_code,
                                  vchr_account_name
                        FROM tbl_account
                        
                            
                        WHERE vchr_account_code = '%s'
                        AND chr_document_status  = 'N'
                        %s
                        """%(str_customer_code,str_account_type_filter)
            
            
            cr.execute(str_query)
            rst = cr.fetchone()
            
            if rst:
                int_account_master_id = rst['pk_bint_account_id']
                str_account_code_inv = rst['vchr_account_code']
                str_account_name_inv = rst['vchr_account_name']#refs #26999
                
        cr.close()
        return (int_account_master_id,
                    chr_account_type,
                    str_account_code_inv,
                    str_account_name_inv,
                    int_location_id,
                    int_branch_id,
                    int_auto_invoice,
                    str_auto_inv_grouping
                    )
                    
                    
    def get_min_location_id(self,):
                    
        cr = self.create_cursor()
                    
        cr.execute("""SELECT  	 min(pk_bint_branch_id) AS pk_bint_branch_id 
                            FROM tbl_branch 
                            WHERE chr_document_status = 'N' """)
        rst = cr.fetchone()
        cr.close()
        if rst:
            return rst['pk_bint_branch_id']
        else:
            return None
                    
    def validate_and_convert_format_of_date(self,str_date):
        
        if not str_date:
            return None
        #refer #42159 & #46466
        str_new_date = None
        str_date = str_date.replace(' ','').strip()
        lst_date_format = ['%d-%m-%Y','%Y-%m-%d','%d-%m-%y','%d%b%y','%d%b%Y','%Y/%m/%d','%d/%m/%Y','%d/%m/%y']
        for str_format in lst_date_format:
            try:
                str_new_date = datetime.datetime.strptime(str_date, str_format)
            except:
                pass
            else:
                return str_new_date
        return None
    
    def get_min_department_id(self,):
        return None
#        cr = self.create_cursor()
        
#        cr.execute("""SELECT min(pk_bint_department_id) AS pk_bint_department_id_min 
#                            FROM tbl_department 
#                            WHERE chr_document_status = 'N' 
#                            """)
#        rst = cr.fetchone()
#        cr.close()
#        if rst:
#            return rst['pk_bint_department_id_min']
#        else:
#            return None
                    
    def get_rounding_value(self,str_currency):
                    
        cr = self.create_cursor()
        cr.execute("""SELECT  bint_precision
                        FROM    tbl_currency   
                            WHERE
                                vchr_currency_code = %s
                                AND chr_document_status = 'N'
                                                        """,
                             (str_currency,))
        rst = cr.fetchone()
        cr.close()
        if rst and rst['bint_precision']:
            return rst['bint_precision']
        return 2
    
    def get_conjuntion_ticket_ticket(self, str_conjection_ticket_number):
        str_ticket_no = ''
        cr = self.create_cursor()
        
        if str_conjection_ticket_number:
            str_query =  """SELECT vchr_ticket_number 
                                        FROM tbl_ticket
                                        WHERE substring(vchr_last_conjunction_ticket_number FROM '[0-9]+')::bigint >= 0 
                                        AND substring(vchr_last_conjunction_ticket_number FROM '[0-9]+')::bigint >= '%s' 
                                        AND substring(vchr_ticket_number FROM '[0-9]+')::bigint < '%s'
                                        AND bln_conjunction_ticket = FALSE
                                        """ %(str_conjection_ticket_number,str_conjection_ticket_number)
                                        
            cr.execute(str_query)
            rst = cr.fetchone()
            cr.close()
            if rst and rst['vchr_ticket_number']:
                str_ticket_no = rst['vchr_ticket_number']
        return str_ticket_no
                
        
    def set_auto_invoice_refund_details(self,):
        cr = self.create_cursor()
        str_query = """
        SELECT 
            
            fk_bint_customer_id,
            fk_bint_counter_staff_id,
            fk_bint_cost_center_id,
            fk_bint_department_id,
            dbl_agency_charge,
            bln_agency_charge

        FROM tbl_auto_refund_settings_details
        """
        cr.execute(str_query)
        rst = cr.fetchall()
        cr.close()
        if rst and rst not in [[],[[]],[None],None]:
            for record in rst :
                ins_office = OfficeIdInvoice()
                ins_office.int_customer_id = record['fk_bint_customer_id']
                ins_office.int_cost_center_id = record['fk_bint_cost_center_id']
                ins_office.int_department_id = record['fk_bint_department_id']
                ins_office.bln_capture_agency_charge = record['bln_agency_charge']
                ins_office.flt_capture_agency_charge = record['dbl_agency_charge']
                self.ins_auto_inv.dct_office_id_data_refund[record['fk_bint_counter_staff_id']] = ins_office
                    
        pass
    
    
        

    
    
    #36781
    def check_ticket_already_exists_in_tbl_ticket_refund_or_not(self,str_ticket_number):
        cr = self.create_cursor()
        cr.execute("""SELECT vchr_ticket_number
                        FROM tbl_ticket_refund
                        WHERE vchr_ticket_number = %s 
                        AND chr_document_status = 'N' """,
                        (str_ticket_number,))
        rst = cr.fetchone()
        cr.close()
        if rst:
            return True
        else:
            return False        
        pass
    
    #refer #40299
    
            
    def get_car_voucher_details(self, str_voucher_number):#40359
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_car_voucher_id
                     FROM tbl_car_voucher
                     WHERE vchr_voucher_number = %s""",(str_voucher_number, ))

        rst = cr.fetchone()
        if rst :
            cr.close()
            return rst['pk_bint_car_voucher_id']
      
        else :
            return None

    def get_car_voucher_sales_id(self, str_voucher_number): #40359
        
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_car_voucher_sales_id
                     FROM tbl_car_voucher_sales
                     WHERE vchr_voucher_number = %s""",(str_voucher_number, ))

        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_car_voucher_sales_id']
        else :
            return None
    
    #40602    
    def get_rental_company_id(self,str_rental_code):
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_car_rental_company_id 
                        FROM tbl_car_rental_company 
                        WHERE vchr_rental_company_code = %s
                            AND chr_document_status = 'N' """,(str_rental_code, ))

        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['pk_bint_car_rental_company_id']
        else :
            return None
        
    #38619    
    def get_ticket_table_id(self,lst_ticket_number,bln_xo_ticket = False):
        """To get the table id using ticket number """
        if bln_xo_ticket :
            str_pk_bint_field = 'pk_bint_booking_details_id'
            str_table_name = 'tbl_booking_details'
        else:
            str_pk_bint_field = 'pk_bint_ticket_id'
            str_table_name = 'tbl_ticket'
        cr = self.create_cursor()
        str_query = """
                    SELECT {str_pk_bint_field} AS int_ticket_id ,
                        vchr_ticket_number,
                        vchr_last_conjunction_ticket_number
                    FROM {str_table_name}
                    WHERE vchr_ticket_number IN (%s)
        """.format(str_pk_bint_field = str_pk_bint_field,str_table_name = str_table_name)%','.join(["'%s'"]*len(lst_ticket_number))%tuple(lst_ticket_number)
        
        cr.execute(str_query)
        
        rst = cr.fetchall()
        if rst not in [[],None,[[]],[None]]:
            
            dct_tkt = {}
            for record in rst :
                dct_tkt[record['vchr_ticket_number']] = record['int_ticket_id']
                
                #to add the conjection ticket to the dictionary
                if record['vchr_last_conjunction_ticket_number'] \
                    and int(record['vchr_ticket_number']) < int(record['vchr_last_conjunction_ticket_number']):
                    int_conj_ticket = int(record['vchr_ticket_number']) + 1
                    while (1):
                        if int_conj_ticket <= int(record['vchr_last_conjunction_ticket_number']):
                            dct_tkt[str(int_conj_ticket)] = record['int_ticket_id']
                            int_conj_ticket += 1
                        else:
                            break
               
            return dct_tkt
        else :
            return {}
    
    def detach_original_ticket_of_void_reissue_ticket(self,str_ticket_number,str_original_issue,str_file_name_rfd):
        cr = self.create_cursor()

        dat_refund_original_ticket = None
        str_orig_tkt_status = ''

        if str_original_issue:
            cr.execute(""" UPDATE tbl_ticket
                      SET vchr_original_issue = ''
                         WHERE vchr_ticket_number = %s""", (str_ticket_number,))
        cr.close()
    
    def get_customer_currency_roe(self,int_account_master_id,str_ticket_issue_date,str_base_currency):
        
        cr = self.create_cursor()
        cr.execute("""SELECT  vchr_currency_code
                        FROM tbl_account
                            WHERE
                                pk_bint_account_id = %s """,
                             (int_account_master_id,))
        rst = cr.fetchone()
        cr.close()
        
        if rst and rst['vchr_currency_code']:
            flt_customer_currency_roe = self.get_roe_of_currency_for_a_date(rst['vchr_currency_code'],str_ticket_issue_date)
            str_customer_currency = rst['vchr_currency_code']
           
        else:
            flt_customer_currency_roe = self.get_roe_of_currency_for_a_date(str_base_currency,str_ticket_issue_date)
            str_customer_currency = str_base_currency
         
        return flt_customer_currency_roe,str_customer_currency
        pass
    
    #arguments -> market fare, total tax, tax details and currency code & returns the ait tax component.
    def get_ait_tax_component_using_admin_settings(self,flt_market_fare,flt_total_tax,str_tax_details,str_currency_code):
        
        try:
            flt_ati_amount = 0.0
            dct_tax ={}
            lst_tax_split_up = []
            str_ait_formula = self.get_admin_settings('STR_AIT_TAX_FIELD_IN_INVOICE_AND_REFUND')
                 
            if str_ait_formula.strip():
                str_ait_formula = str_ait_formula.replace(',','+').replace('[','(').replace(']',')').replace('{','(').replace('}',')').replace(' ','').upper() 
                str_ait_formula = str_ait_formula.replace('FARE',str(flt_market_fare)).replace('TAX',str(flt_total_tax))
                
                #Combining same tax codes eg: if E5=10 & E5=5 then E5=15
                for str_tax in str_tax_details.split(','):
                    if not str_tax.split('=')[0].strip() or not str_tax.split('=')[1].strip():
                        continue
                    if str_tax.split('=')[0].strip() in dct_tax:
                        dct_tax[str_tax.split('=')[0].strip()] += float(str_tax.split('=')[1].strip())
                    else:
                        dct_tax[str_tax.split('=')[0].strip()] = float(str_tax.split('=')[1].strip())
                
                for str_code,str_value in dct_tax.items():
                    lst_tax_split_up.append(str_code+'='+str(str_value))
                    
                #replacing tax component values            
                for str_tax in lst_tax_split_up :
                    if str_ait_formula.find(str_tax.split('=')[0].strip()) != -1 :
                        str_ait_formula = str_ait_formula.replace(str_tax.split('=')[0].strip(),str_tax.split('=')[1].strip())
                #converting all other tax components to Zero
                lst_remaining = []
                for int_index in range(len(str_ait_formula)):
                    if str_ait_formula[int_index].isalpha():
                        lst_remaining.append(str_ait_formula[int_index:int_index+2])
                for str_tax in lst_remaining:
                    str_ait_formula = str_ait_formula.replace(str_tax,'0')

                #evaluating formula and rounding value using admin settings
#                flt_ati_amount =  self.get_amount_using_admin_settings_value(eval(str_ait_formula),str_currency_code)
                flt_ati_amount =  round(eval(str_ait_formula))
            return flt_ati_amount
            
        except:
            return 0.0
        
    #45745
    def get_passenger_profile_id(self,str_traveller_id):
#        cr = self.create_cursor()
#        cr.execute("""SELECT pk_bint_passenger_profile_id 
#                        FROM tbl_passenger_profile 
#                        WHERE vchr_traveller_id = %s 
#                            AND chr_document_status = 'N' #""",(str_traveller_id, ))
#        rst = cr.fetchone()
#        cr.close()
#        if rst and rst['pk_bint_passenger_profile_id']:
#            return rst['pk_bint_passenger_profile_id']
#        else:
            return None
        
