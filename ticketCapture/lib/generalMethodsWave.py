"""
Purpose: General Methods for capturing
Owner  : KISHOR PS
Date   : 01/10/2019
Re. F/M: 

"""
import binascii
import time
import copy
import re
import os
import json
from requests import Session
import datetime
import random
import base64
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import unpad


try:
    from pyPgSQL import PgSQL
    str_dba_name = 'pypgsql'
except:
    import psycopg2    
    import psycopg2.extras
    str_dba_name = 'psycopg'

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
        self.bln_skip_tkt_with_tour_code = False   #refer #42644

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
#        self.str_base_currency = self.get_admin_settings('STR_CURRENCY_BASE_1')
#        self.bln_set_cc_chrge_as_profit = self.get_admin_settings('BLN_CREDIT_CARD_CHARGES_AS_PROFIT')
#        if not self.str_base_currency :
#            raise Exception("Base currency not Found")
#        self.str_currency_conversion_method = self.get_admin_settings('ROE_FORMAT')
#        
#        self.int_rounding_digits = 3 #pdb 
#        self.set_capturing_settings_dictionary()
#        self.set_optional_field_data()
#        self.set_airplus_field_data()
#        self.get_meals_plan_id()
#        self.str_reverse_calculate_dis_extra_amt = self.get_admin_settings('ADJUST_SELLING_PRICE_DIFFERENCE_TO')
#        self.str_consider_higher_travel_class = self.get_admin_settings('BLN_HIGHER_TRAVEL_CLASS')
#        self.int_airplus_card_id = self.get_airplus_card_id('TP')
#        self.lst_non_iata_instance = []
#        self.dct_non_iata_capture_settings = {}
#        
#        self.ins_auto_inv = AutoInvoice()
#        self.ins_capture_base = CaptureBase()
#        self.set_capture_settings_data()
#        self.create_configuration_file_instance()
#        self.ins_global = GlobalInstance()
#        self.set_non_iata_capture_details()
#        self.set_auto_invoice_refund_details()
#        self.int_min_location_id = self.get_min_location_id()
#        self.int_min_department_id = self.get_min_department_id() 
#        
#        self.bln_capture_input_vat_only_for_dom_tickets =  self.get_admin_settings('BLN_CAPTURE_INPUT_VAT_ONLY_FOR_DOM_TKTS')
        

    def reload_data(self,):
        
        
        print("Reloading data")
#        self.connect_db()
        self.str_base_currency = self.get_admin_settings('STR_CURRENCY_BASE_1')
        self.bln_set_cc_chrge_as_profit = self.get_admin_settings('BLN_CREDIT_CARD_CHARGES_AS_PROFIT')
        self.str_currency_conversion_method = self.get_admin_settings('ROE_FORMAT')
        
        self.set_capturing_settings_dictionary()
        self.set_optional_field_data()
        self.set_airplus_field_data()
        self.get_meals_plan_id()
        self.str_reverse_calculate_dis_extra_amt = self.get_admin_settings('ADJUST_SELLING_PRICE_DIFFERENCE_TO')
        self.str_consider_higher_travel_class = self.get_admin_settings('BLN_HIGHER_TRAVEL_CLASS')
        self.int_airplus_card_id = self.get_airplus_card_id('TP')
        self.lst_non_iata_instance = []
        self.dct_non_iata_capture_settings = {}
        
        self.ins_auto_inv = AutoInvoice()
        self.ins_capture_base = CaptureBase()
        self.set_capture_settings_data()
        self.create_configuration_file_instance()
        self.ins_global = GlobalInstance()
        self.set_non_iata_capture_details()
        self.set_auto_invoice_refund_details()
        self.int_min_location_id = self.get_min_location_id()
        self.int_min_department_id = self.get_min_department_id()
        self.bln_capture_input_vat_only_for_dom_tickets =  self.get_admin_settings('BLN_CAPTURE_INPUT_VAT_ONLY_FOR_DOM_TKTS')
        self.bln_enable_input_vat_in_all_transactions = self.get_admin_settings('BLN_ENABLE_INPUT_VAT_IN_ALL_TRANSACTIONS') #42613
        self.str_client_server_authentication = self.get_admin_settings('TICKET_CAPTURE_AUTHENTICATION')
        self.bln_enable_multiple_discount_details = self.get_admin_settings('BLN_ENABLE_MULTI_DISCOUNT')
        
    def read_configuration_file(self, *args):
        #configuration file reading
        #As per the configuration extenilization process configurations files will be in different location.
        str_path = "/app/configurations/traacs-saas-capturing/"
        str_conf_path = str_path + "capture.conf"  
        str_service_conf_path = str_path+ "voucherCapture.conf"  
        if not os.path.exists(str_conf_path) :
            str_conf_path = os.path.realpath(__file__)
            index = str_conf_path.rindex("/")
            str_conf_path = str_conf_path[:index-3] + "capture.conf"  
            str_service_conf_path = str_conf_path[:index-3]+ "voucherCapture.conf"  

        fd = open(str_conf_path,'r')
        
        self.dct_conf_data = {}
        for str_line in fd.readlines():
            str_line = str_line.strip()
            if str_line != "" and str_line[0] != "#":
                self.dct_conf_data[str_line.split("=")[0].strip()] = str_line.split("=")[1].strip()
                pass
            pass
        fd.close()
        
        #voucher configuration file reading
        str_key = ""
        self.dct_voucher_conf_data = {}
        fd = open(str_service_conf_path,'r')
        for str_line in fd.readlines():
            str_line = str_line.strip()
            if str_line != "" and str_line[0] != "#":
                if str_line.startswith("**"):
                    str_key = str_line.strip().strip("*").strip()
                    self.dct_voucher_conf_data[str_key] = {}
                else :
                    self.dct_voucher_conf_data[str_key][str_line.split(":")[0].strip().upper()] = str_line.split(":")[1].strip().upper()+':'
                
        fd.close()
    
    def connect_db(self, *args):
        
        """Database Connection
        """
        dct_tenant_data = {}
        dct_tenant_data['db_port'] = '5432'
        if all(key in self.dct_conf_data for key in ('database','host','user','password')):
            dct_tenant_data['db_name'] = self.dct_conf_data['database']
            dct_tenant_data['db_host'] = self.dct_conf_data['host']
            dct_tenant_data['db_user'] = self.dct_conf_data['user']
            dct_tenant_data['db_password'] = self.dct_conf_data['password']
            
        #---------------------------------------------------------------------
        #41159- If there is an entry 'TRAACS_WAVE_CONF_PATH' in configuration file,
        #then the database details are taken from Traacs wave application.ini file.
        #---------------------------------------------------------------------
        #Developer->And if there is no traacs connection, then comment 'TRAACS_WAVE_CONF_PATH' in configuration file
        #and add the missing details of database connection.
        #---------------------------------------------------------------------
        if 'TRAACS_WAVE_CONF_PATH' in self.dct_conf_data:
            str_traacs_path = self.dct_conf_data['TRAACS_WAVE_CONF_PATH']
            str_traacs_path += '/application/configs/application.ini'
            fd_traacs = open(str_traacs_path,'r')
            for str_line in fd_traacs.readlines():
                str_line = str_line.strip()
                if str_line.startswith('resources.db.params.host ='):
                    str_temp_line = str_line.split('=')[1].strip()
                    if str_temp_line.find(';'):
                        dct_tenant_data['db_host'] = self.get_decryption_data(str_temp_line.split(';')[0])
                    else:
                        dct_tenant_data['db_host'] = self.get_decryption_data(str_temp_line)                    
                    
                elif str_line.startswith('resources.db.params.dbname ='):
                    str_temp_line = str_line.split('=')[1].strip()
                    if str_temp_line.find(';'):
                        dct_tenant_data['db_name'] = self.get_decryption_data(str_temp_line.split(';')[0])
                    else:
                        dct_tenant_data['db_name'] = self.get_decryption_data(str_temp_line)
                    
                elif str_line.startswith('resources.db.params.username ='):
                    str_temp_line = str_line.split('=')[1].strip()
                    if str_temp_line.find(';'):
                        dct_tenant_data['db_user'] = self.get_decryption_data(str_temp_line.split(';')[0])
                    else:
                        dct_tenant_data['db_user'] = self.get_decryption_data(str_temp_line)
                    
                elif str_line.startswith('resources.db.params.password ='):
                    str_temp_line = str_line.split('=')[1].strip()
                    if str_temp_line.find(';'):
                        dct_tenant_data['db_password'] = self.get_decryption_data(str_temp_line.split(';')[0])
                    else:
                        dct_tenant_data['db_password'] = self.get_decryption_data(str_temp_line)
                        
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
    
    #decryption function can decrypt the data from Traacs Wave side encryption and as well as above encryption
    def get_decryption_data(self,str_cipher,padlen=32):
        plaintext = ''
        str_cipher = str_cipher.replace('*','=')
        if str_cipher and str_cipher.find('@Tr@@CS') != -1:
            str_data = str_cipher.split('@Tr@@CS')[0]
            str_key = str_cipher.split('@Tr@@CS')[1]
        else:
            str_data = str_cipher.split('@@cs')[0]
            str_data = self.pad_text(str_data,24,'=')
            str_key = str_cipher.split('@@cs')[1]
            str_key = self.pad_text(str_key,24,'=')
            str_key = self.pad_text(str_key,padlen)
        str_iv = base64.b64decode(str_key)
        obj2 = AES.new(str_key.encode(), AES.MODE_CBC, str_iv)
        plaintext = obj2.decrypt(base64.b64decode(str_data))
        if str_cipher and str_cipher.find('@Tr@@CS') == -1:
            plaintext = unpad(plaintext, AES.block_size)
        plaintext = plaintext.decode()
        return plaintext
    
    #Function to add the padding according to the required length
    def pad_text(self,str_pad,padlen,padChar ='\0' ):
        for int_count in range(len((padlen - len(str_pad) % padlen) * chr(padlen - len(str_pad) % padlen))):
            str_pad = str_pad+padChar
        return str_pad
    
    def create_cursor(self, *args):
        if str_dba_name == 'psycopg':
            cr = self.ins_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            cr = self.ins_db.cursor()
        return cr
    
    def create_configuration_file_instance(self,):
        
        str_current_date_time = datetime.datetime.now().strftime("%B%Y")
        
        try:
            self.ins_capture_base.db = self.ins_db

        except Exception as msg:
            print(msg)
            raise Exception("Unable to connect to Database")
        
        self.ins_capture_base.int_sleep_time = int(self.dct_conf_data.get('sleep_time',5))
        
        self.ins_capture_base.int_mask_start_amadeus = int(self.dct_conf_data.get('MASK_START_AM', 0))
        self.ins_capture_base.int_mask_char_count_amadeus = int(self.dct_conf_data.get('MASK_CHAR_COUNT_AM', 0))
        self.ins_capture_base.str_mask_char_amadeus = self.dct_conf_data.get('MASK_CHAR_AM', 0)
        
        self.ins_capture_base.int_mask_start_galileo = int(self.dct_conf_data.get('MASK_START_GAL', 0))
        self.ins_capture_base.int_mask_char_count_galileo = int(self.dct_conf_data.get('MASK_CHAR_COUNT_GAL', 0))
        self.ins_capture_base.str_mask_char_galileo = self.dct_conf_data.get('MASK_CHAR_GAL', 0)
        
        self.ins_capture_base.int_mask_start_sabre = int(self.dct_conf_data.get('MASK_START', 0))
        self.ins_capture_base.int_mask_char_count_sabre = int(self.dct_conf_data.get('MASK_CHAR_COUNT', 0))
        self.ins_capture_base.str_mask_char_sabre = self.dct_conf_data.get('MASK_CHAR', 0)
        
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
        self.ins_capture_base.dct_voucher_conf_data = self.dct_voucher_conf_data
    
    
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
        
        
#        ins_capture_base1.int_sleep_time = int(self.dct_conf_data.get('sleep_time',5))
#        ins_capture_base1.int_mask_start = int(self.dct_conf_data.get('MASK_START_AM', 0))
#        ins_capture_base1.int_mask_char_count = int(self.dct_conf_data.get('MASK_CHAR_COUNT_AM', 0))
#        ins_capture_base1.str_mask_char = self.dct_conf_data.get('MASK_CHAR_AM', 0)
#        ins_capture_base1.str_consider_nf_key_word_in_amdeus = self.dct_conf_data.get('CONSIDER_NF_KEYWORD_IN_AMADEUS','False')
#        if self.dct_conf_data.get('CONSIDER_DANA_HOTEL_VOUCHER_CAPTURE','').upper() == 'TRUE': # refer 34430
#            ins_capture_base1.bln_consider_dana_hotel_voucher_capture = True
#        else :
#            ins_capture_base1.bln_consider_dana_hotel_voucher_capture = False
#        
#        ins_capture_base1.bln_multi_currency = True
#            
#        if self.dct_conf_data.get('AMADEUS_DIFF_FP_SECTION' , '').upper() == 'TRUE':
#            ins_capture_base1.bln_diif_fp_section = True
#        else :
#            ins_capture_base1.bln_diif_fp_section = False
        
#        ins_capture_base1.ticketing_agent_numeric_code_for_ins_voucher = self.dct_conf_data.get('TICKETING_AGENT_NUMERIC_CODE_FOR_INS_VOUCHER','').upper()
#        ins_capture_base1.ticketing_agent_char_code_for_ins_voucher = self.dct_conf_data.get('TICKETING_AGENT_CHAR_CODE_FOR_INS_VOUCHER','').upper()
        
#        ins_capture_base1.dct_conf_data = self.dct_conf_data
#        ins_capture_base1.dct_voucher_conf_data = self.dct_voucher_conf_data
        
        return ins_folder_base

        
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
                                        AND vchr_ticket_number != vchr_original_issue """ ,(str_original_issue,))

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
            
            cr.execute("""SELECT pk_bint_xo_purchase_master_id 
                     FROM tbl_xo_purchase_master
                     WHERE vchr_document_number = %s
                        AND chr_document_status = 'N'""", (str_voucher_number,))
            rst = cr.fetchone()
            cr.close()
            if rst:
                return rst['pk_bint_xo_purchase_master_id']
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

#    def get_car_voucher_details(self, str_voucher_number):
#        cr = self.create_cursor()
#
#        cr.execute("""SELECT pk_bint_car_voucher_id
#                     FROM tbl_car_voucher
#                     WHERE vchr_voucher_number = %s#""",(str_voucher_number, ))
#
#        rst = cr.fetchone()
#        cr.close()
#        if rst :
#            return rst['pk_bint_car_voucher_id']
#        else :
#            return None

    def get_other_voucher_details(self, str_voucher_number):
        cr = self.create_cursor()

        cr.execute("""SELECT pk_bint_other_service_id
                     FROM tbl_other_service
                     WHERE vchr_supporting_document_number = %s""",(str_voucher_number, ))

        rst = cr.fetchone()
        if rst :
            cr.close()
            return rst['pk_bint_other_service_id']
        else : 
            cr.execute("""SELECT pk_bint_xo_purchase_master_id 
                     FROM tbl_xo_purchase_master
                     WHERE vchr_document_number = %s
                          AND chr_document_status = 'N'""", (str_voucher_number,))
            rst = cr.fetchone()
            cr.close()
            if rst:
                return rst['pk_bint_xo_purchase_master_id']
            return None
        
    def get_region_details(self):
        cr = self.create_cursor()
        dct_region = {}
        cr.execute("""SELECT pk_bint_set_regions_airports_id,
                             vchr_region_code,
                             vchr_list_of_airports
                      FROM tbl_set_regions_airports
                      WHERE chr_document_status = 'N' """)
        rst = cr.fetchall()
        cr.close()
        
        for record in rst:
            dct_region[record['vchr_region_code']] = record['vchr_list_of_airports']
            
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
        
        cr.execute(""" SELECT tc.fk_bint_creditor_account_id
                       FROM tbl_creditor tc
                       INNER JOIN tbl_account ta
                        ON tc.fk_bint_creditor_account_id = ta.pk_bint_account_id
                        AND ta.chr_document_status = 'N'
                        AND tc.chr_document_status = 'N'
                       WHERE substring(tc.vchr_iata_number,1,7) = %s
                       AND tc.chr_document_status = 'N' """,
                       
                         (str_iata_num[0:7],))
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['fk_bint_creditor_account_id']
        
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
        """ Set value same as in ins_dictionary.dct_capturing_settings"""
        
        #// When ever a new rm section is aded add it into the below list
        
#        self.dct_capturing_settings = {
#                                        'AGENT CODE' : ['RM AC','RM AC','M9*SGAC','RM AC',False],
#                                        'C3X OPTION1' : ['RM OPT1','RM OPT1','RM OPT1','RM OPT1',False],
#                                        'C3X OPTION2' : ['RM OPT2','RM OPT2','RM OPT2','RM OPT2',False],
#                                        'CASH' : ['RM CASH','RM CASH','RM CASH','RM CASH',False],
#                                        'COUNT' : ['RM COUNT','RM COUNT','RM COUNT','RM COUNT',True],
#                                        'CUSTOMER CODE' : ['RM CUST','A14FT-CUS','M9*SGCUST','RM CUST',True],
#                                        'CUSTOMER EMAIL' : ['RM EMAIL','RM EMAIL','RM EMAIL','RM EMAIL',False],
#                                        'CUSTOMER PHONE' : ['RM PHONE','RM PHONE','RM PHONE','RM PHONE',False],
#                                        'EMPLOYEE EMAIL' : ['RM EMPMAIL','RM EMPMAIL','RM EMPMAIL','RM EMPMAIL',False],
#                                        'EMPLOYEE NUMBER' : ['RM EMP','A14FT-EMP','M9*SGEMP','.Z EMP',False],
#                                        'EXTRA COLLECTION' : ['RM EXTRA','RM EXTRA','RM EXTRA','RM EXTRA',False],
#                                        'INVOICE COST CENTER' : ['RM CC','A14FT-CC','M9*SGCC','.Z CC',True],
#                                        'JOB CODE' : ['RM JC','A14FT-JC','M9*SGJC','.Z JC',False],
#                                        'LOST' : ['RM LOST','RM LOST','RM LOST','RM LOST',False],
#                                        'LPO NUMBER' : ['RM LPO','RM LPO','RM LPO','RM LPO',False],
#                                        'PURPOSE' : ['RM PURPOSE','RM PURPOSE','RM PURPOSE','RM PURPOSE',False],
#                                        'REASON' : ['RM REASON','RM REASON','RM REASON','RM REASON',False],
#                                        'REMARKS' : ['RM COMMENT','RM COMMENT','RM COMMENT','RM COMMENT',False],
#                                        'SELLING PRICE' : ['RM COLL','RM COLL','RM COLL','RM COLL',False],
#                                        'SERVICE FEE' : ['RM SVF','A14FT-SVF','M9*SGSVF','.Z SVF',False],
#                                        'STAFF' : ['RM STAFF','RM STAFF','RM STAFF','RM STAFF',False],
#                                        'SUBTYPE' : ['RM SUBTYPE','RM SUBTYPE','RM SUBTYPE','RM SUBTYPE',False],
#                                        'TICKET AGENT CODE' : ['RM TKTAGNT','RM TKTAGNT','RM TKTAGNT','RM TKTAGNT',False],
#                                        'TRAVEL REG NUMBER' : ['RM TRN','A14FT-TRN','M9*SGTRN','.Z TRN',False],
#                                        'ADDITIONAL SERVICE FEE' : ['RM*MARGE','RM*MARGE','RM*MARGE','RM*MARGE',False],
#                                        'BRANCH CODE' : ['RM SA','A14FTSA','M9*SA','.Z SA',False],
#                                        'INVOICE BRANCH' : ['RM BR','A14FT-BR','M9*SGBR','.Z BR',False],
#                                        'ORIGINAL FARE' : ['RM*ORGFARE' ,'A14FT-ORGFARE','M9*SGORGFARE','.Z ORGFARE',False],
#
##                                      }
#                                      
#                                      
        self.dct_capturing_settings = {
                                        
                                        'PARTY_CODE' : ['RM*CUST:','A14FT-CUST:','M9*SG-CUST:','.Z CUST:',True],
                                        'CUST_PAX_EMAIL' : ['RM*PAXEMAIL:','A14FT-PAXEMAIL:','M9*SG-PAXEMAIL:','.Z PAXEMAIL:',False],
                                        'CUST_PAX_MOBILE' : ['RM*PAXMOB:','A14FT-PAXMOB:','M9*SG-PAXMOB:','.Z PAXMOB:',False],
                                        ##'EMPLOYEE EMAIL' : ['RM EMPMAIL','RM EMPMAIL','RM EMPMAIL','RM EMPMAIL',False],
                                        'CUST_EMPLOYEE_NO':['RM*ENO:','A14FT-ENO:','M9*SG-ENO:','.Z ENO:',False],
                                        ##'EXTRA COLLECTION' : ['RM EXTRA','RM EXTRA','RM EXTRA','RM EXTRA',False],
                                        'AGENCY_COST CENTRE_CODE' : ['RM CC','A14FT-CC','M9*SGCC','.Z CC',True],
                                        'CUST_JOB_CODE' : ['RM*CJC:','A14FT-CJC:','M9*SG-CJC:','.Z CJC:',False],
                                        #'LOST' : ['RM LOST','RM LOST','RM LOST','RM LOST',False],
                                        'PARTY_LPO_NO' : ['RM*LPO:','A14FT-LPO:','M9*SG-LPO:','.Z LPO:',False],
                                        'CUST_PURPOSE_OF_TRAVEL' : ['RM*POT:','A14FT-POT:','M9*SG-POT:','.Z POT:',False],
                                        #'REASON' : ['RM REASON','RM REASON','RM REASON','RM REASON',False],
                                        'AGENCY_INTERNAL_REMARKS' : ['RM*AIR:','A14FT-AIR:','M9*SG-AIR:','.Z AIR:',False],
                                        'FARE_SELLING_PRICE' : ['RM*SP:','A14FT-SP:','M9*SG-SP:','.Z SP:',False],
                                        'FARE_SERVICE_FEE' : ['RM*SF:','A14FT-SF:','M9*SG-SF:','.Z SF:',False],
                                        #'STAFF' : ['RM STAFF','RM STAFF','RM STAFF','RM STAFF',False],
                                        #'SUBTYPE' : ['RM SUBTYPE','RM SUBTYPE','RM SUBTYPE','RM SUBTYPE',False],
                                        'AGENCY_TICKETING_STAFF' : ['RM*STAFF:','A14FT-STAFF:','M9*SG-STAFF:','.Z STAFF:',False],
                                        #'TRAVEL REG NUMBER' : ['RM TRN','A14FT-TRN','M9*SGTRN','.Z TRN',False],
                                        #'ADDITIONAL SERVICE FEE' : ['RM*MARGE','RM*MARGE','RM*MARGE','RM*MARGE',False],
                                        'AGENCY_DEPARTMENT_CODE' : ['RM*DEPT:','A14FT-DEPT:','M9*SG-DEPT:','.Z DEPT:',False],
#                                        'ORIGINAL FARE' : ['RM*ORGFARE' ,'A14FT-ORGFARE','M9*SGORGFARE','.Z ORGFARE',False] ,
                                        
                                        
                                        
                                        'CUST_ENGAGEMENT_CODE':['RM*CEC:','A14FT-CEC:','M9*SG-CEC:','.Z CEC:',False] ,
                                        'CUST_RESOURCE_CODE':['RM*RES:','A14FT-RES:','M9*SG-RES:','.Z RES:',False],
                                        'CUST_COMMITMENT_NO':['RM*PA:','A14FT-PA:','M9*SG-PA:','.Z PA:',False],
                                        'CUST_ACCOUNTING_UNIT':['RM*CAU:','A14FT-CAU:','M9*SG-CAU:','.Z CAU:',False],
                                        'CUST_INTERNAL_AC_NO':['RM*CIAN:','A14FT-CIAN:','M9*SG-CIAN:','.Z CIAN:',False],
                                        'CUST_PROJECT_CODE':['RM*CPC:','A14FT-CPC:','M9*SG-CPC:','.Z CPC:',False],
                                        'CUST_ACTION_NO':['RM*CAN:','A14FT-CAN:','M9*SG-CAN:','.Z CAN:',False],
                                        'CUST_APPROVER_NAME':['RM*TAPR:','A14FT-TAPR:','M9*SG-TAPR:','.Z TAPR:',False],
                                        'CUST_APPROVER_EMAIL':['RM*AEM:','A14FT-AEM:','M9*SG-AEM:','.Z AEM:',False],
                                        'CUST_SUB_CUSTOMER_CODE':['RM*SUBCUST:','RM*SUBCUST:','RM*SUBCUST:','RM*SUBCUST:',False],
                                        
                                        'CUST_EMPLOYEE_GRADE':['RM*GRADE:','A14FT-GRADE:','M9*SG-GRADE:','.Z GRADE:',False],
                                        'CUST_COST_CENTRE':['RM*CLOC:','A14FT-CLOC:','M9*SG-CLOC:','.Z CLOC:',False],
                                        'CUST_DEPARTMENT':['RM*CDEPT:','A14FT-CDEPT:','M9*SG-CDEPT:','.Z CDEPT:',False],
                                        
                                        'PARTY_MULTIPLE_FOP_YES_NO':['RM*MFOP:','A14FT-MFOP:','M9*SG-MFOP:','.Z MFOP:',False] ,
                                        'PARTY_ADDITIONAL_AR':['RM*AR:','A14FT-AR:','M9*SG-AR:','.Z AR:',False] ,
                                        'PARTY_MAXIMUM_TICKETS':['RM*COUNT:','A14FT-COUNT:','M9*SG-COUNT:','.Z COUNT:',False] ,
                                        'PARTY_FILE_JOB_CARD_NO':['RM*FILE:','A14FT-FILE:','M9*SG-FILE:','.Z FILE:',False] ,
                                        
                                        'AGENCY_SALES_MAN':['RM*SMAN:','A14FT-SMAN:','M9*SG-SMAN:','.Z SMAN:',False] ,
                                        'AGENCY_TRAACS_USER':['RM*USER:','A14FT-USER:','M9*SG-USER:','.Z USER:',False] ,
                                        'AGENCY_ADV_RECEIPT_NO':['RM*ARV:','A14FT-ARV:','M9*SG-ARV:','.Z ARV:',False] ,
                                        'AGENCY_PRODUCT_CODE':['RM*PROD:','A14FT-PROD:','M9*SG-PROD:','.Z PROD:',False] ,
                                        'AGENCY_SUB_PRODUCT_CODE':['RM*SUBPROD:','A14FT-SUBPROD:','M9*SG-SUBPROD:','.Z SUBPROD:',False] ,
                                        'AGENCY_AUTO_INVOICE_YES_NO':['RM*AUTO:','A14FT-AUTO:','M9*SG-AUTO:','.Z AUTO:',False] ,
                                        'AGENCY_PAYBACK_ACCOUNT':['RM*PBAC:','A14FT-PBAC:','M9*SG-PBAC:','.Z PBAC:',False] ,
                                        'COMPLIANCE':['RM*COMPLIANCE:','A14FT-COMPLIANCE:','M9*SG-COMPLIANCE:','.Z COMPLIANCE:',False] ,
                                        'CORPORATE_CARD_CODE':['RM*CCCODE:','A14FT-CCCODE:','M9*SG-CCCODE:','.Z CCCODE:',False] ,
                                        'AGAINST_DOCUMENT_NO':['RM*AGTDOC:','A14FT-AGTDOC:','M9*SG-AGTDOC:','.Z AGTDOC:',False] ,
                                        'PNR_TYPE':['RM*PNRTYPE:','A14FT-PNRTYPE:','M9*SG-PNRTYPE:','.Z PNRTYPE:',False] ,
                                        
                                        'FARE_PUBLISHED':['RM*PF:','A14FT-PF:','M9*SG-PF:','.Z PF:',False] ,
                                        'FARE_PUBLISHED_CHILD':['RM*CPF:','A14FT-CPF:','M9*SG-CPF:','.Z CPF:',False] ,
                                        'FARE_PUBLISHED_INFANT':['RM*IPF:','A14FT-IPF:','M9*SG-IPF:','.Z IPF:',False] ,
                                        'FARE_ORIGINAL':['RM*OF','A14FT-OF','M9*SG-OF','.Z OF',False] ,
                                        'FARE_PRINTING':['RM*FQ:','A14FT-FQ:','M9*SG-FQ:','.Z FQ:',False] ,
                                        'FARE_EXTRA_EARNING':['RM*EE:','A14FT-EE:','M9*SG-EE:','.Z EE:',False] ,
                                        'FARE_EXTRA_EARNING_CHILD':['RM*CEE:','A14FT-CEE:','M9*SG-CEE:','.Z CEE:',False] ,
                                        'FARE_EXTRA_EARNING_INFANT':['RM*IEE:','A14FT-IEE:','M9*SG-IEE:','.Z IEE:',False] ,
                                        'FARE_PAYBACK_COMMISSION':['RM*PBC:','A14FT-PBC:','M9*SG-PBC:','.Z PBC:',False] ,
                                        'FARE_CC_CHARGE_COLLECTED':['RM*CCC:','A14FT-CCC:','M9*SG-CCC:','.Z CCC:',False] ,
                                        'FARE_DISCOUNT_GIVEN':['RM*DG:','A14FT-DG:','M9*SG-DG:','.Z DG:',False] ,
                                        'FARE_DISCOUNT_GIVEN_CHILD':['RM*CDG:','A14FT-CDG:','M9*SG-CDG:','.Z CWDG:',False] ,
                                        'FARE_DISCOUNT_GIVEN_INFANT':['RM*IDG:','A14FT-IDG:','M9*SG-IDG:','.Z IDG:',False] ,
                                        'FARE_LOWEST_OFFERED':['RM*LFO:','A14FT-LFO:','M9*SG-LFO:','.Z LFO:',False] ,
                                        'FARE_REASON_FOR_CHOOSE_HIGHER':['RM*RHF:','A14FT-RHF:','M9*SG-RHF:','.Z RHF:',False] ,
                                        'FARE_ACCEPTED_OR_PAID': ['RM*FAP:','A14FT-FAP:','M9*SG-FAP:','.Z FAP:',False ] ,
                                        'FARE_LOST_AMOUNT': ['RM*LFA:','A14FT-LFA:','M9*SG-LFA:','.Z LFA:',False ],
                                        'FARE_SERVICE_FEE_CHILD': ['RM*CSF:','RM*CSF:','RM*CSF:','RM*CSF:',False ],
                                        'FARE_SERVICE_FEE_INFANT': ['RM*ISF:','RM*ISF:','RM*ISF:','RM*ISF:',False ],
                                        'FARE_DEAL_SHARING': ['RM*RATIO:','RM*RATIO:','RM*RATIO:','.RM*RATIO:',False ],
                                        
                                        'OPTION_1': ['RM*OPTION1:','RM*OPTION1:','RM*OPTION1:','.RM*OPTION1:',False ],
                                        'OPTION_2': ['RM*OPTION2:','RM*OPTION2:','RM*OPTION2:','.RM*OPTION2:',False ],
                                        
                                        'CUST_POS_ID': ['RM*POS ID:','A14FT-POS ID:','M9*SG-POS ID:','.Z POS ID:',False], # Refer 20879
                                        'CUST_CC_NUMBER': ['RM*CC NO:','A14FT-CC NO:','M9*SG-CC NO:','.Z CC NO:',False], # Refer 20879
                                        'VOUCHER_TAX': ['RX TAX:','RX TAX:','RX TAX:','RX TAX:',False], # Refer 20879
                                        'VOUCHER_DISCOUNT': ['RX DISCOUNT:','RX DISCOUNT:','RX DISCOUNT:','RX DISCOUNT:',False], # Refer 20879
                                        'VOUCHER_SUPPLIER_AMOUNT': ['RX NR:','RX NR:','RX NR:','RX NR:',False], # Refer 20879
                                        'VOUCHER_SELLING_PRICE': ['RX RQ:','RX RQ:','RX RQ:','RX RQ:',False], # Refer 20879
                                        'VOUCHER_SUPPLIER': ['RX VENDOR:','RX VENDOR:','RX VENDOR:','RX VENDOR:',False], # Refer 20879
                                        'CRM_REFERENCE': ['RM*CRM REF:','A14FT-CRM REF:','M9*SG-CRM REF:','.Z CRM REF:',False], # Refer 23236
                                        'FARE_INF_SELLING_PRICE': ['RM*INFSP:','A14FT-INFSP:','M9*INFSP','RM*INFSP:',False], # Refer 20879
                                        'FARE_CHD_SELLING_PRICE': ['RM*CHDSP:','A14FT-CHDSP:','M9*CHDSP','RM*CHDSP:',False], # Refer 20879
                                        
                                        'MASTER_REFERENCE':['RM*MASTER REF:','A14FT-MASTER REF:','M9*SG-MASTER REF:','.Z MASTER REF:',False],# ref 28647
                                        'MASTER_NARRATION':['RM*MASTER NARR:','A14FT-MASTER NARR:','M9*SG-MASTER NARR:','.Z MASTER NARR:',False],# ref 28647
                                        'LPO_DATE':['RM*LPO DATE:','A14FT-LPO DATE:','M9*SG-LPO DATE:','.Z LPO DATE:',False],# ref 28647
                                        'PASSIVE_SEGMENTS':['RM*PS:','A14FT-PS:','M9*PS:','.Z PS:',False],  #43745
                                        'FARE_CC_CHARGE_COLLECTED_CHILD':['RM*CCCC:' ,'A14FT-CCCC:','M9*CCCC:','.Z CCCC:', False], #45305
                                        'FARE_CC_CHARGE_COLLECTED_INFANT':['RM*ICCC:' ,'A14FT-ICCC:','M9*ICCC:','.Z ICCC:', False]
                                        
                                      }

        self.dct_capturing_settings_default = copy.deepcopy(self.dct_capturing_settings)

        # // SET CAPTURING FIELDS
        cr = self.create_cursor()

        cr.execute("""SELECT vchr_system_attribute_name,
                             vchr_amadeus_code,
                             vchr_galileo_code,
                             vchr_sabre_code,
                             vchr_worldspan_code,
                             bln_mandatory
                        FROM tbl_capturing_fields_settings
                        WHERE int_entry_type = 0
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
        
        
        dct_opt_data = {
                        'FIELD 1' : 'vchr_field_1',
                        'FIELD 2' : 'vchr_field_2',
                        'FIELD 3' : 'vchr_field_3',
                        'FIELD 4' : 'vchr_field_4',
                        'FIELD 5' : 'vchr_field_5',
                        'FIELD 6' : 'vchr_field_6',
                        'FIELD 7' : 'vchr_field_7',
                        'FIELD 8' : 'vchr_field_8',
                        'FIELD 9' : 'vchr_field_9',
                        'FIELD 10' : 'vchr_field_10',
                        'FIELD 11' : 'vchr_field_11',
                        'FIELD 12' : 'vchr_field_12',
                        'FIELD 13' : 'vchr_field_13',
                        'FIELD 14' : 'vchr_field_14',
                        'FIELD 15' : 'vchr_field_15',
                        'FIELD 16' : 'vchr_field_16',
                        'FIELD 17' : 'vchr_field_17',
                        'FIELD 18' : 'vchr_field_18',
                        'FIELD 19' : 'vchr_field_19',
                        'FIELD 20' : 'vchr_field_20'
                            
                        
                        }
        
        
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
                             vchr_farelogix_code,
                             CASE WHEN int_entry_type = 1
                             THEN 'NOT_JSON' ELSE 'JSON' END AS vchr_type
                      FROM tbl_capturing_fields_settings
                      WHERE int_entry_type IN( 1,2)
                      AND chr_status = 'N' """)
        rst_opt_fields = cr.fetchall()

        for record in rst_opt_fields:
            if record['vchr_amadeus_code'] or record['vchr_galileo_code'] or record['vchr_worldspan_code'] or record['vchr_sabre_code'] or record['vchr_farelogix_code']:  #42165
                if record['vchr_type'] == 'JSON':
                    self.dct_amadeus_optional_fields[record['vchr_amadeus_code']] = [record['vchr_type'],record['vchr_system_attribute_name']]
                    self.dct_galileo_optional_fields[record['vchr_galileo_code']] = [record['vchr_type'],record['vchr_system_attribute_name']]
                    self.dct_worldspan_optional_fields[record['vchr_worldspan_code']] = [record['vchr_type'],record['vchr_system_attribute_name']]
                    self.dct_sabre_optional_fields[record['vchr_sabre_code']] = [record['vchr_type'],record['vchr_system_attribute_name']]
                    self.dct_fare_logix_optional_fields[record['vchr_farelogix_code']] = [record['vchr_type'],record['vchr_system_attribute_name']]
                else:
                    self.dct_amadeus_optional_fields[record['vchr_amadeus_code']] = dct_opt_data[record['vchr_system_attribute_name']]
                    self.dct_galileo_optional_fields[record['vchr_galileo_code']] = dct_opt_data[record['vchr_system_attribute_name']]
                    self.dct_worldspan_optional_fields[record['vchr_worldspan_code']] = dct_opt_data[record['vchr_system_attribute_name']]
                    self.dct_sabre_optional_fields[record['vchr_sabre_code']] = dct_opt_data[record['vchr_system_attribute_name']]
                    self.dct_fare_logix_optional_fields[record['vchr_farelogix_code']] = dct_opt_data[record['vchr_system_attribute_name']]
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
                    fk_bint_created_id,
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
        
        cr.execute("""SELECT  pk_bint_set_regions_airports_id
                            FROM tbl_set_regions_airports 
                            WHERE vchr_region_code = %s
                            AND chr_document_status = 'N'""",(str_region_code,))
                            
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_set_regions_airports_id']:
            return rst['pk_bint_set_regions_airports_id']
        else:
            return None
    def get_region_code(self,int_region_id):
        cr = self.create_cursor()
        
        cr.execute("""SELECT  vchr_region_code
                            FROM tbl_set_regions_airports 
                            WHERE pk_bint_set_regions_airports_id = %s
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
                                 ( fk_bint_account_id = %s AND dat_effective <= now()::DATE )
                                 OR NOT EXISTS      (
                                                    SELECT pk_bint_booking_class_id 
                                                    FROM tbl_booking_class
                                                    WHERE fk_bint_account_id = %s
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
    
    def get_city_id(self,str_city_name,str_airport_code = ''):
        cr = self.create_cursor()
        
        if str_airport_code:
            str_query = """SELECT pk_bint_city_id,fk_bint_country_id 
                            FROM tbl_city
                            WHERE vchr_city_name ILIKE (SELECT vchr_airport_name 
                                                        FROM tbl_airport
                                                        WHERE vchr_airport_code  = '%s'
                                                        AND chr_document_status = 'N' )
                                AND chr_document_status = 'N'"""%(str_airport_code,)
        else:
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
                            d.fk_bint_department_id,
                            d.fk_bint_default_cost_centre_id ,
                            
                            cs.fk_bint_cost_center_id AS fk_bint_cost_center_id_cs,
                            cs.fk_bint_department_id AS fk_bint_department_id_cs,
                            ta2.vchr_account_code AS vchr_cust_cash_account_code
                            
                        FROM tbl_counter_staff cs
                        LEFT JOIN tbl_debtor d
                            ON fk_bint_customer_account_id = d.fk_bint_debtor_account_id
                            AND d.chr_document_status = 'N'
                        LEFT JOIN tbl_account a
                            ON cs.fk_bint_customer_account_id =  a.pk_bint_account_id
                            AND a.chr_document_status = 'N'
                            
                        LEFT JOIN tbl_account ta2
                            ON cs.fk_bint_staff_cash_acc_id = ta2.pk_bint_account_id
                                AND ta2.chr_document_status = 'N'
                        WHERE %s = '%s'
                        %s
                        AND cs.chr_document_status = 'N'
                        AND cs.chr_status = 'A'
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
                            rst['fk_bint_department_id'] ,
                                rst['fk_bint_default_cost_centre_id'],
                                    rst['fk_bint_cost_center_id_cs'],
                                        rst['fk_bint_department_id_cs'],
                                        rst['vchr_counter_staff_numeric_code'] or '',
                                        rst['vchr_cust_cash_account_code'])
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
                                fk_bint_region_id
                            FROM tbl_ticket 
                            WHERE vchr_ticket_number = '%s'
                           
                        """%(str_connection_ticket_number))
                        
        rst = cr.fetchone()
        cr.close()
        lst_emd_ticket_details = [] #38326
        if rst:
            str_emd_region_code = self.get_region_code(rst['fk_bint_region_id'])
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
        
        cr.execute("""SELECT  	 pk_bint_cost_center_id ,
                                fk_bint_company_id
                            FROM tbl_cost_center 
                            WHERE vchr_cost_center_code = %s
                            AND chr_document_status = 'N' """,(str_location_code,))
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_cost_center_id']:
            return rst['pk_bint_cost_center_id'],(rst['fk_bint_company_id'] or 1 )
        else:
            return None,1
        pass
    
    def verify_department_details(self,str_branch_code):
        cr = self.create_cursor()
        
        cr.execute("""SELECT pk_bint_department_id 
                            FROM tbl_department 
                            WHERE vchr_department_code = %s
                            AND chr_document_status = 'N' """,(str_branch_code,))
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_department_id']:
            return rst['pk_bint_department_id']
        else:
            return None
        pass
    
    
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
        
        if rst :
            cr.close()
            return rst['fk_bint_corporate_card_account_id'],rst['int_corporate_card_type']
        else:
            ## refer #15466 &#39593
            cr.execute("""
                            SELECT fk_bint_customer_card_account_id 
                            FROM tbl_customer_corporate_card
                            WHERE vchr_card_no = %s
                            AND chr_document_status = 'N'
                            """,(str_card_number,))
            rst = cr.fetchone()
            
            if not rst and int_mask_start and int_mask_char_count and str_mask_char:
                cr.execute("""
                            SELECT fk_bint_customer_card_account_id 
                            FROM tbl_customer_corporate_card
                            WHERE substring(vchr_card_no from 1 for %s ) = %s
                                AND substring(vchr_card_no from %s) = %s
                            
                            AND chr_document_status = 'N'
                            """,(int_mask_start-1,str_card_number[:int_mask_start-1],int_mask_start+int_mask_char_count,str_card_number[int_mask_start+int_mask_char_count-1:].strip()))
                rst = cr.fetchone()
            
            
            cr.close()
            if rst :
                return rst['fk_bint_customer_card_account_id'],1
            else :
                return None,None
    
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
        
        cr.execute("""
                    SELECT fk_bint_customer_account_id_inv 	
                                FROM tbl_customer
                                WHERE vchr_credit_card_number = %s
                                AND chr_document_status = 'N'
                        """,(str_card_number,))
        
        rst = cr.fetchone()
        
        if not rst and int_mask_start and int_mask_char_count and str_mask_char:
            cr.execute("""
                    SELECT fk_bint_customer_account_id_inv 	
                                FROM tbl_customer
                                WHERE substring(vchr_credit_card_number from 1 for %s ) = %s
                                AND substring(vchr_credit_card_number from %s) = %s
                                AND chr_document_status = 'N'
                        """,(int_mask_start-1,str_card_number[:int_mask_start-1],int_mask_start+int_mask_char_count,str_card_number[int_mask_start+int_mask_char_count-1:]))
        
            rst = cr.fetchone()
        
        if rst :
            cr.close()
            return rst['fk_bint_customer_account_id_inv']
        else :
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
            rst = cr.fetchall()    #41052
            
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
            
                rst = cr.fetchall()
            cr.close()
            if rst and rst not in [[[]],[None]] and len(rst) == 1:    #41052 & #41611
                for record in rst :
                    return record['fk_bint_debtor_account_id']
            else :
                return None
            
    def get_airplus_card_id(self,str_card_type):
        ## Refer #16207
        if not str_card_type.strip() :
            return None
        cr = self.create_cursor()
        
        cr.execute("""
                    SELECT tas.fk_bint_corporate_card_account_id
                        FROM tbl_airplus_settings tas
                        INNER JOIN tbl_corporate_card tcc
                            ON tas.fk_bint_corporate_card_account_id = tcc.fk_bint_corporate_card_account_id
                            AND tcc.vchr_corporate_card_type = 'AIRPLUS'
                            AND tas.vchr_credit_card_indicator = %s
                            AND tcc.int_corporate_card_type = 1
                            AND tcc.chr_document_status = 'N'
                            

                        """,(str_card_type,))
        
        rst = cr.fetchone()
        cr.close()
        if rst :
            return rst['fk_bint_corporate_card_account_id']
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
        int_auto_invoice = 0 #  update tbl_debtor set int_auto_invoice = 0 where int_auto_invoice != 0

        cr = self.create_cursor()
        
        str_account_type_filter = ''

        if str_account_type :
            str_account_type_filter = " AND int_account_type NOT IN (0,1,2,3,16,17,19) "

        if str_customer_code:
            str_customer_code = str_customer_code.replace("'",'').strip() #refs 38439
            str_query = """SELECT ta.pk_bint_account_id ,
                                  ta.vchr_account_code,
                                  ta.vchr_account_name,
                                  d.fk_bint_department_id,
                                  d.fk_bint_default_cost_centre_id,
                                  d.int_auto_invoice,
                                  d.vchr_invoice_grouping
                        FROM tbl_account ta
                        LEFT JOIN tbl_debtor d
                            ON ta.pk_bint_account_id = d.fk_bint_debtor_account_id
                            AND d.chr_document_status = 'N'
                        
                            
                        WHERE ta.vchr_account_code = '%s'
                        AND ta.chr_document_status  = 'N'
                        %s
                        """%(str_customer_code,str_account_type_filter)
            
            
            cr.execute(str_query)
            rst = cr.fetchone()
            
            if rst:
                int_account_master_id = rst['pk_bint_account_id']
                str_account_code_inv = rst['vchr_account_code']
                str_account_name_inv = rst['vchr_account_name']
                int_location_id = rst['fk_bint_default_cost_centre_id']
                int_branch_id = rst['fk_bint_department_id']
                int_auto_invoice = rst['int_auto_invoice'] or 0
                str_auto_inv_grouping = rst['vchr_invoice_grouping'] or ''#refs #26999
                
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
                    
        cr.execute("""SELECT  	 min(pk_bint_cost_center_id) AS pk_bint_cost_center_id_min 
                            FROM tbl_cost_center 
                            WHERE chr_document_status = 'N' """)
        rst = cr.fetchone()
        cr.close()
        if rst:
            return rst['pk_bint_cost_center_id_min']
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
        
        cr = self.create_cursor()
        
        cr.execute("""SELECT min(pk_bint_department_id) AS pk_bint_department_id_min 
                            FROM tbl_department 
                            WHERE chr_document_status = 'N' 
                            """)
        rst = cr.fetchone()
        cr.close()
        if rst:
            return rst['pk_bint_department_id_min']
        else:
            return None
                    
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
                    
    def set_capture_settings_data(self,):
                    
        str_query = """
        SELECT tam.bln_staff_num_code_match,
            tam.bln_staff_officeid_match,
            tam.bln_voucher_capture,
            tam.bln_xo_capture,
            tam.int_auto_invoice,
            tam.int_auto_refund,
            tam.vchr_staffwise_auto_invoice,
            tam.vchr_input_vat_code,
            tam.bln_staff_customer,
            tam.bln_skip_tkt_with_tour_code,
                    
            tad.vchr_gds_name,
            tad.vchr_officeid_type,
            tad.vchr_officeid,
            tad.fk_bint_customer_id,
            tad.fk_bint_counter_staff_id,
            tad.fk_bint_cost_center_id,
            tad.fk_bint_department_id,
            tad.bln_capture_servicefee,
            
            ta.vchr_account_code ,
            tc.vchr_cost_center_code,
            tc.fk_bint_company_id,
            
            vchr_amadeus_numeric_code,
            vchr_galileo_numeric_code,
            vchr_sabre_numeric_code,
            vchr_worldspan_numeric_code,
            vchr_amadeus_code,
            vchr_galileo_code,
            vchr_sabre_code ,
            vchr_world_span_code,
            vchr_farelogix_numeric_code,
            vchr_farelogix_code,
            bln_auto_invoice
            
            
            
            
        FROM tbl_autoinvoice_settings_master tam
            LEFT JOIN tbl_autoinvoice_settings_details tad
                ON fk_bint_autoinvoice_settings_master_id = pk_bint_autoinvoice_settings_master_id
                
            LEFT JOIN tbl_account ta 
                ON tad.fk_bint_customer_id = ta.pk_bint_account_id
                    AND ta.chr_document_status ='N'
                    
            LEFT JOIN tbl_cost_center tc
                ON tad.fk_bint_cost_center_id = pk_bint_cost_center_id
                    AND tc.chr_document_status = 'N'
                    
            LEFT JOIN tbl_counter_staff tco
                ON tad.fk_bint_counter_staff_id = pk_bint_counter_staff_id
                    AND tco.chr_document_status = 'N'
                    
            
                
            
            
        WHERE tam.chr_document_status = 'N'
    """
        
        cr = self.create_cursor()
        cr.execute(str_query)
        rst = cr.fetchall()
        cr.close()

        if rst and rst not in [[],[[]],[None],None] :
            self.ins_auto_inv.bln_staff_num_code_match = rst[0]['bln_staff_num_code_match']
            self.ins_auto_inv.bln_staff_officeid_match = rst[0]['bln_staff_officeid_match']
            self.ins_auto_inv.bln_voucher_capture = rst[0]['bln_voucher_capture']
            self.ins_auto_inv.bln_xo_capture = rst[0]['bln_xo_capture']
            self.ins_auto_inv.str_input_vat_code = rst[0]['vchr_input_vat_code']
            self.ins_auto_inv.bln_skip_tkt_with_tour_code = rst[0]['bln_skip_tkt_with_tour_code'] #refer #42644
            
            if rst[0]['int_auto_invoice'] == 1:
                self.ins_auto_inv.bln_auto_invoice = True
                self.ins_auto_inv.bln_auto_invoice_all_ticket = True
            elif rst[0]['int_auto_invoice'] == 2:
                self.ins_auto_inv.bln_auto_invoice = True
                self.ins_auto_inv.bln_auto_invoice_all_ticket = False
            else :
                self.ins_auto_inv.bln_auto_invoice = False
                self.ins_auto_inv.bln_auto_invoice_all_ticket = False

            if rst[0]['int_auto_refund'] == 1:
                self.ins_auto_inv.bln_auto_refund = True
                self.ins_auto_inv.bln_auto_refund_all_ticket = True
            elif rst[0]['int_auto_refund'] == 2:
                self.ins_auto_inv.bln_auto_refund = True
                self.ins_auto_inv.bln_auto_refund_all_ticket = False
            else :
                self.ins_auto_inv.bln_auto_refund = False
                self.ins_auto_inv.bln_auto_refund_all_ticket = False
            
            if rst[0]['vchr_staffwise_auto_invoice'] == 'ISSUING' :
                self.ins_auto_inv.bln_capture_use_client_set_in_tick_agent = True
                if rst[0]['bln_staff_customer']:
                    self.ins_auto_inv.bln_capture_client_set_in_tick_agent = True
            elif rst[0]['vchr_staffwise_auto_invoice'] == 'BOOKING' :
                self.ins_auto_inv.bln_capture_use_client_set_in_book_agent = True
                if rst[0]['bln_staff_customer']:
                    self.ins_auto_inv.bln_capture_client_set_in_book_agent = True
            else :
                self.ins_auto_inv.bln_capture_client_set_in_tick_agent = False
                self.ins_auto_inv.bln_capture_client_set_in_book_agent = False
                self.ins_auto_inv.bln_capture_use_client_set_in_tick_agent = False
                self.ins_auto_inv.bln_capture_use_client_set_in_book_agent = False
        
            for record in rst :
                
                if record['vchr_gds_name'] and record['vchr_officeid_type'] and record['vchr_officeid'] :
                    ins_office = OfficeIdInvoice()
                    self.ins_auto_inv.dct_office_id_data[record['vchr_gds_name']+record['vchr_officeid_type']+record['vchr_officeid']] = ins_office
                    
                    ins_office.int_customer_id = record['fk_bint_customer_id']
                    ins_office.str_customer_code = record['vchr_account_code']
                    ins_office.int_counter_staff_id = record['fk_bint_counter_staff_id']
                    ins_office.int_cost_center_id = record['fk_bint_cost_center_id']
                    ins_office.int_company_id = record['fk_bint_company_id']
                    ins_office.int_department_id = record['fk_bint_department_id']
                    ins_office.bln_capture_service_fee = record['bln_capture_servicefee']
                    
                    ins_office.str_amad_code = record['vchr_amadeus_code']
                    ins_office.str_amad_num_code = record['vchr_amadeus_numeric_code']
                    ins_office.str_gal_code = record['vchr_galileo_code']
                    ins_office.str_gal_num_code = record['vchr_galileo_numeric_code']
                    ins_office.str_sab_code = record['vchr_sabre_code']
                    ins_office.str_sab_num_code = record['vchr_sabre_numeric_code']
                    ins_office.str_ws_code = record['vchr_world_span_code']
                    ins_office.str_ws_num_code = record['vchr_worldspan_numeric_code']
                    ins_office.str_far_code = record['vchr_farelogix_code']
                    ins_office.str_far_num_code = record['vchr_farelogix_numeric_code']
                    
                    ins_office.bln_auto_invoice = record['bln_auto_invoice']
                   
            
    """
    const INT_ACCOUT_TYPE_OTHER = 0;
    const INT_ACCOUT_TYPE_GROUP = 1;
    const INT_ACCOUT_TYPE_MAIN_LEDGER = 2;
    const INT_ACCOUT_TYPE_LEDGER_LEAF = 3;
    const INT_ACCOUT_TYPE_BANK = 4;
    const INT_ACCOUT_TYPE_CREDIT_CARD = 5;
    const INT_ACCOUT_TYPE_DC_CARD = 6;
    const INT_ACCOUT_TYPE_CASH_ACCOUNTS = 7;
    const INT_ACCOUT_TYPE_CURRENT_CHEQUE = 8;
    const INT_ACCOUT_TYPE_PDC_RECEIVED = 9;
    const INT_ACCOUT_TYPE_PDC_ISSUED = 10;
    const INT_ACCOUT_TYPE_EMPLOYEE = 13;
    const INT_ACCOUT_TYPE_DEBTOR = 14;
    const INT_ACCOUT_TYPE_CREDITOR = 15;
    const INT_ACCOUT_TYPE_PROFILE = 16;
    const INT_ACCOUT_TYPE_AIRLINE = 17;
    const INT_ACCOUT_TYPE_ITEM = 19;
    const INT_ACCOUT_TYPE_COMMISSION_AGENT = 20;
    const INT_ACCOUT_TYPE_COMMISSION_COLLECTOR = 21;
    """
    
    
    
    def get_conjuntion_ticket_ticket(self, str_conjection_ticket_number):
        str_ticket_no = ''
        cr = self.create_cursor()
        
        if str_conjection_ticket_number:
            str_query =  """SELECT vchr_ticket_number 
                                        FROM tbl_ticket
                                        WHERE substring(vchr_last_conjection_ticket_number FROM '[0-9]+')::bigint >= 0 
                                        AND substring(vchr_last_conjection_ticket_number FROM '[0-9]+')::bigint >= '%s' 
                                        AND substring(vchr_ticket_number FROM '[0-9]+')::bigint < '%s'
                                        AND bln_conjuction_ticket = FALSE
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
            
            cr.execute("""SELECT pk_bint_xo_purchase_master_id 
                     FROM tbl_xo_purchase_master
                     WHERE vchr_document_number = %s
                        AND chr_document_status = 'N'""", (str_voucher_number,))
            rst = cr.fetchone()
            cr.close()
            if rst:
                return rst['pk_bint_xo_purchase_master_id']
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
                        WHERE vchr_car_rental_company_code = %s
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
                        vchr_last_conjection_ticket_number
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
                if record['vchr_last_conjection_ticket_number'] \
                    and int(record['vchr_ticket_number']) < int(record['vchr_last_conjection_ticket_number']):
                    int_conj_ticket = int(record['vchr_ticket_number']) + 1
                    while (1):
                        if int_conj_ticket <= int(record['vchr_last_conjection_ticket_number']):
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
    
    def rm_str_to_flt(self,str_line):
        try:
            str_temp = ''
            str_line = str(str_line)
            for i in range(len(str_line)):
                if str_line[i].isdigit() or str_line[i] == '.' :
                    str_temp += str_line[i]
                else:
                    break
            return float(str_temp)
        except:
            return 0.0
        
    def create_remarks_with_pax_association(self,str_system_attribute,str_remark_value):
        """If a values like P1,P2 etc comes in remarks section then it means that the remark is for
            pax 1 and pax 2 

            Possibilities are :
            P1-4  ==> P1,P2,P3,P4
            P3-4,6 ==> P3,P4,P6
            P1,P2 ==> P1,P2
            P1    ==> P1
            P1-P2 ==>P1,P2
        """

        def update_remark_dictionary(str_assosiation):
            lst_temp = []
            try:
                if str_assosiation and str_assosiation.find('-') != -1 : # P1-4 & P1-P2
                    lst_temp = str_assosiation.split('-')
                    if len(lst_temp) == 2 and lst_temp[0].replace('P','').strip().isdigit() and lst_temp[1].replace('P','').strip().isdigit():
                        int_first = int(lst_temp[0].replace('P','').strip())
                        int_last = int(lst_temp[1].replace('P','').strip())
                        for i in range(int_first,int_last+1) :
                            if not str_system_attribute in self.dct_pax_wise_remark :
                                self.dct_pax_wise_remark[str_system_attribute] = {}
                            if not i in self.dct_pax_wise_remark[str_system_attribute] :
                                self.dct_pax_wise_remark[str_system_attribute].update( {i : str_remark_value.split(';')[0].strip()} )

                elif str_assosiation and str_assosiation.replace('P','').strip().isdigit() :
                    int_first = int(str_assosiation.replace('P','').strip())
                    if not str_system_attribute in self.dct_pax_wise_remark :
                        self.dct_pax_wise_remark[str_system_attribute] = {}
                    if not int_first in self.dct_pax_wise_remark[str_system_attribute] :
                        self.dct_pax_wise_remark[str_system_attribute].update( {int_first : str_remark_value.split(';')[0].strip()} )
            except:
                pass
        
        try:
            if str_remark_value.strip():
                str_remark_value = str(str_remark_value.strip())
                if str_remark_value.strip().find(';') != -1 :
                    str_pax_assosiation = str_remark_value.split(';')[-1].strip()
                    if str_pax_assosiation and str_pax_assosiation.startswith('P') : #if not starts 'P' then skip
                        if str_pax_assosiation.strip().find(',') != -1 :
                            for str_assosiation in str_pax_assosiation.strip().split(',') :
                                update_remark_dictionary(str_assosiation.strip())
                        else:
                            update_remark_dictionary(str_pax_assosiation.strip())
                else:
                    if not str_system_attribute in self.dct_pax_wise_remark :
                        self.dct_pax_wise_remark[str_system_attribute] = { 'ALL' : str_remark_value.split(';')[0].strip() }
                    elif not 'ALL' in self.dct_pax_wise_remark[str_system_attribute] :
                        self.dct_pax_wise_remark[str_system_attribute].update({ 'ALL' : str_remark_value.split(';')[0].strip() })
        except:
            pass
            
    def create_sabre_remarks_with_pax_association(self,str_system_attribute,str_remark_value):
        
        def update_sabre_remark_dictionary(str_assosiation):
            try:
                if str_assosiation:
                    int_first = str_assosiation.strip()
                    if not str_system_attribute in self.dct_pax_wise_remark:
                        self.dct_pax_wise_remark[str_system_attribute] = {}
                    if not int_first in self.dct_pax_wise_remark[str_system_attribute]:
                        self.dct_pax_wise_remark[str_system_attribute].update({int_first: str_remark_value.split('-')[0].strip()})
            except:
                pass
            
        try:
            if str_remark_value.strip():
                str_remark_value = str(str_remark_value.strip())
                if str_remark_value.strip().find('-') != -1 :
                    str_pax_assosiation = str_remark_value.split('-')[-1].strip()
                    if str_pax_assosiation :
                        if str_pax_assosiation.strip().find(',') != -1 :
                            for str_assosiation in str_pax_assosiation.strip().split(',') :
                                update_sabre_remark_dictionary(str_assosiation.strip())
                        else:
                            update_sabre_remark_dictionary(str_pax_assosiation.strip())
                else:
                    if not str_system_attribute in self.dct_pax_wise_remark :
                        self.dct_pax_wise_remark[str_system_attribute] = { 'ALL' : str_remark_value.split('-')[0].strip() }
                    elif not 'ALL' in self.dct_pax_wise_remark[str_system_attribute] :
                        self.dct_pax_wise_remark[str_system_attribute].update({ 'ALL' : str_remark_value.split('-')[0].strip() })
        except:
            pass
    def get_pax_no_remarks(self,str_remark_key,int_pax_no,str_type = 'STRING'):
        # for sabre int_pax_no argument will be a string. eg: "1.1"
        if str_remark_key == 'PRINTALL':
            print(self.dct_pax_wise_remark)
            
        str_remark_data = ''
        if str_remark_key  and str_remark_key in self.dct_pax_wise_remark:
            if int_pax_no and int_pax_no in self.dct_pax_wise_remark[str_remark_key]:
                str_remark_data =  self.dct_pax_wise_remark[str_remark_key][int_pax_no]
            elif 'ALL' in self.dct_pax_wise_remark[str_remark_key]:
                str_remark_data =  self.dct_pax_wise_remark[str_remark_key]['ALL']
        
        if str_type == 'FLOAT' :
            str_remark_data = self.rm_str_to_flt(str_remark_data)
        elif str_type == 'DATE' :
            str_remark_data = self.validate_and_convert_format_of_date(str_remark_data)
        elif str_type == 'INT' :
            try:
                str_remark_data = int(str_remark_data)
            except:
                str_remark_data = 0
            
        return str_remark_data
    
    
    def convert_to_customer_currency(self,flt_amount,flt_cust_currency_roe,flt_supplier_currency_roe = 1, str_cust_currency = ''):
        int_precision = self.get_rounding_value(str_cust_currency) #45196
        flt_conv_to_base_amount = float(flt_amount or 0.00 ) * float(flt_supplier_currency_roe)
        flt_conv_amount = float(flt_conv_to_base_amount) / float(flt_cust_currency_roe)
        return round(flt_conv_amount,int_precision)
    
    #45196
    def save_discount_data_to_tbl_multi_discount_details(self, ins_ticket_base, chr_ticket_status, str_ticket_number = '', str_supporting_doc_type = 'T'):
        lst_discount_data = []
        str_ticket_number = str_ticket_number or ins_ticket_base.str_ticket_number
        str_cust_currency = ins_ticket_base.str_cust_currency
        flt_cust_currency_roe = ins_ticket_base.flt_cust_currency_roe
        flt_sup_currency_roe = ins_ticket_base.flt_supplier_currency_roe
        if chr_ticket_status == 'R' or chr_ticket_status == 'V' :
            
            cr = self.create_cursor()
            cr.execute("""SELECT * FROM tbl_multi_discount_details
                                WHERE vchr_supporting_document_no = %s
                                    AND chr_document_status = 'N' """,(str_ticket_number, ))
            rst = cr.fetchall()
            cr.close()
            for record in rst:
                if record['vchr_discount_type'] == 'NORMAL':
                    ins_ticket_base.flt_rm_normal_discount = float(record['dbl_base_currency_amount'] or 0)
                if record['vchr_discount_type'] == 'PLB':
                    ins_ticket_base.flt_rm_plb_discount = float(record['dbl_base_currency_amount'] or 0)
                if record['vchr_discount_type'] == 'DEAL':
                    ins_ticket_base.flt_rm_deal_discount = float(record['dbl_base_currency_amount'] or 0)
                
            str_cust_currency = ins_ticket_base.str_cust_currency_rfd
            flt_cust_currency_roe = ins_ticket_base.flt_cust_currency_roe_rfd
            flt_sup_currency_roe = ins_ticket_base.flt_supplier_currency_roe_rfd

        int_precision = self.get_rounding_value(self.str_base_currency)
        
        if ins_ticket_base.flt_rm_normal_discount:
            lst_discount_data.append((  str_ticket_number,
                                        str_supporting_doc_type,
                                        'NORMAL',
                                        chr_ticket_status,
                                        self.convert_to_customer_currency(ins_ticket_base.flt_rm_normal_discount,flt_cust_currency_roe,flt_sup_currency_roe,str_cust_currency),
                                        round(self.convert_foreign_to_based_currency_amt(ins_ticket_base.flt_rm_normal_discount, flt_sup_currency_roe, int_roe_round = 0),int_precision),
                                        flt_cust_currency_roe,
                                        str_cust_currency,
                                        self.str_base_currency,
                                        'N'
                                    ))
                                    
        if ins_ticket_base.flt_rm_plb_discount:
            lst_discount_data.append((  str_ticket_number,
                                        str_supporting_doc_type,
                                        'PLB',
                                        chr_ticket_status,
                                        self.convert_to_customer_currency(ins_ticket_base.flt_rm_plb_discount,flt_cust_currency_roe,flt_sup_currency_roe,str_cust_currency),
                                        round(self.convert_foreign_to_based_currency_amt(ins_ticket_base.flt_rm_plb_discount, flt_sup_currency_roe, int_roe_round = 0),int_precision),
                                        flt_cust_currency_roe,
                                        str_cust_currency,
                                        self.str_base_currency,
                                        'N'
                                    ))
                                    
        if ins_ticket_base.flt_rm_deal_discount:
            lst_discount_data.append((  str_ticket_number,
                                        str_supporting_doc_type,
                                        'DEAL',
                                        chr_ticket_status,
                                        self.convert_to_customer_currency(ins_ticket_base.flt_rm_deal_discount,flt_cust_currency_roe,flt_sup_currency_roe,str_cust_currency),
                                        round(self.convert_foreign_to_based_currency_amt(ins_ticket_base.flt_rm_deal_discount, flt_sup_currency_roe, int_roe_round = 0),int_precision),
                                        flt_cust_currency_roe,
                                        str_cust_currency,
                                        self.str_base_currency,
                                        'N'
                                    ))
        
        if lst_discount_data:
            cr = self.create_cursor()
            cr.executemany("""INSERT INTO tbl_multi_discount_details
                                (   vchr_supporting_document_no,                            
                                    chr_supporting_document_type,
                                    vchr_discount_type,
                                    chr_supporting_document_status,
                                    dbl_cust_currency_amount ,
                                    dbl_base_currency_amount  ,
                                    dbl_customer_roe ,
                                    vchr_customer_currency ,     
                                    vchr_base_currency,
                                    chr_document_status )
                            VALUES (%s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s)""",lst_discount_data)
            cr.close()
    
    #arguments -> market fare, total tax, tax details and currency code & returns the ait tax component.
    def get_ait_tax_component_using_admin_settings(self,flt_market_fare,flt_total_tax,str_tax_details,str_currency_code):
        
        return 0.0  # need to remove on Bangladesh market merge also rounding AIT value
    
        try:
            flt_ati_amount = 0.0
            dct_tax ={}
            lst_tax_split_up = []
            str_ait_formula = self.get_admin_settings('STR_AIT_TAX_FIELD_IN_INVOICE_AND_REFUND')
                 
            if str_ait_formula:
                str_ait_formula = str_ait_formula.replace(',','+').replace('[','(').replace(']',')').replace('{','(').replace('}',')').replace(' ','').upper() 
                str_ait_formula = str_ait_formula.replace('FARE',str(flt_market_fare)).replace('TAX',str(flt_total_tax))
                
                #Combining same tax codes eg: if E5=10 & E5=5 then E5=15
                for str_tax in str_tax_details.split(','):
                    if str_tax.split('=')[0].strip() in dct_tax:
                        dct_tax[str_tax.split('=')[0].strip()] += float(str_tax.split('=')[1].strip())
                    else:
                        dct_tax[str_tax.split('=')[0].strip()] = float(str_tax.split('=')[1].strip())
                
                for str_code,str_value in dct_tax.items():
                    lst_tax_split_up.append(str_code+'='+str(str_value))
                    
                #replacing tax component values            
                for str_tax in lst_tax_split_up :
                    if str_ait_formula.find(str_tax.split('=')[0].strip())!= -1 :
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
                flt_ati_amount =  eval(str_ait_formula)
            return flt_ati_amount
            
        except:
            return 0.0

    #45745
    def get_passenger_profile_id(self,str_traveller_id):
        cr = self.create_cursor()
        cr.execute("""SELECT pk_bint_passenger_profile_id 
                        FROM tbl_passenger_profile 
                        WHERE vchr_traveller_id = %s 
                            AND chr_document_status = 'N' """,(str_traveller_id, ))
        rst = cr.fetchone()
        cr.close()
        if rst and rst['pk_bint_passenger_profile_id']:
            return rst['pk_bint_passenger_profile_id']
        else:
            return None
        
    #45305
    def get_staff_code_from_email(self, str_email, str_gds = ''):
        cr = self.create_cursor()
        str_char_code = ''
        str_numeric_code = ''
        if str_gds == 'Amadeus' :
            str_char_code = "vchr_amadeus_code"
            str_numeric_code = "vchr_amadeus_numeric_code"
        elif (str_gds == 'Sabre' or str_gds == 'NDC') :
            str_char_code = "vchr_sabre_code"
            str_numeric_code = "vchr_sabre_numeric_code"
        elif str_gds == 'Galileo' :
            str_char_code = "vchr_galileo_code"
            str_numeric_code = "vchr_galileo_numeric_code"
        elif str_gds == 'World Span' :
            str_char_code = "vchr_world_span_code"
            str_numeric_code = "vchr_worldspan_numeric_code"
        elif str_gds == 'Farelogix' :
            str_char_code = "vchr_farelogix_code"
            str_numeric_code = "vchr_farelogix_numeric_code"
            
        str_query = """ SELECT %s AS vchr_numeric_code,
                            %s AS vchr_char_code
                            FROM tbl_counter_staff
                            WHERE vchr_email = '%s'
                            AND chr_document_status = 'N'
                            AND chr_status = 'A'"""%(str_numeric_code,str_char_code,str_email)
        
                            
        cr.execute(str_query)
        rst = cr.fetchone()
        if rst:
            return rst['vchr_char_code'], rst['vchr_numeric_code']