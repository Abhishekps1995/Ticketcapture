"""
Purpose: Wave Amadeus Capture
Owner  : KISHOR PS
Date   : 01/10/2019
Re. F/M: caprure.py , ticketBase.py, Ticket Invoice and Refund
Last Update: 
"""

import re
import binascii
import os
import os.path
import sys
import time
import copy
import random
import datetime
from threading import Thread

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
    else:
        ins_general_methods.reload_data()
except Exception as msg:
    print ('Connection Error..',msg)
    raise

try:
    import ticketCapture.instanceBase as instanceBase
    import ticketCapture.saveOrUpdateData as saveOrUpdateData
    import ticketCapture.saveOrUpdateDataWave as saveOrUpdateDataWave
    import ticketCapture.createTicketBaseInstance as createTicketBaseInstance
except:
    import instanceBase
    import saveOrUpdateData
    import saveOrUpdateDataWave
    import createTicketBaseInstance

ins_folder_base = ins_general_methods.create_folder_structre('Amadeus')

if 'HOME' not in os.environ:
    os.environ['HOME'] = 'C:\\'

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

class SkipTicketException(Exception):
    pass


class DocumentBase(object):
    def __init__(self, *args):
        self.str_document_number = ''
        self.str_ticket_number = ''
        self.str_created_by = ''
        self.str_auto_invoice_location = ''
        self.flt_original_amount = ''
        self.str_dat_document = ''
        self.int_debit_party_id = ''
        self.bln_uccf = False
        self.flt_service_charge = 0.0
        pass

global dct_error_messages
dct_error_messages = {}

global bln_raise
lst_args = sys.argv
if len(lst_args) > 1 and lst_args[1].strip().upper() == 'RAISE':
    bln_raise = True
else :
    bln_raise = False

class Capture:
    def __init__(self, *args):
        self.str_defult_currency_code = ins_general_methods.str_base_currency
        self.bln_refund_void = False
        self.int_first = 1
        

    def move_not_parsed_folder_files_to_parent_folder(self, *args):
        lst_files = os.listdir(ins_folder_base.str_not_parsed_dir)
        for str_file in lst_files:
            if sys.platform == "win32":
                os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_not_parsed_dir, str_file), os.path.join(ins_folder_base.str_directory, str_file)))
            else:
                os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_not_parsed_dir, str_file), os.path.join(ins_folder_base.str_directory, str_file)))
                pass
            pass
        pass

    def ticket_capture(self, *args):
            
#        import amadeusVoucherCapture
#        ins_voucherCapturing = amadeusVoucherCapture.AmadeusVoucherCapture()

            global dct_voucher_status
            dct_voucher_status = {"Success":'Passed',"Refund":'Passed',"Duplication":"False"}
            global dct_voucher_not_parsed_files
            dct_voucher_not_parsed_files = {}
#            ins_general_methods.ins_global.dct_not_parsed_files = {}
#            rst_region = ins_general_methods.get_region_details()
#            self.dct_region = {}
#
#            for record in rst_region:
#                self.dct_region[record['vchr_region_code']] = record['vchr_list_of_airports']
    #        rst = ins_save_or_update_data.get_admin_settings_with_permission('ENABLEVOUCHERCAPTURING')
            bln_enable_voucher_capturing = False
#        if rst:
#            bln_enable_voucher_capturing = bool(rst['bln_admin_settings'])
#        while True:

            self.move_not_parsed_folder_files_to_parent_folder()
            
            lst_files = os.listdir(ins_folder_base.str_directory)
            for str_file in lst_files:
                
                if str_file.find('~') != -1 :
                    continue
                    
                str_new_file = str_file
                str_new_file_tmp = str_file
                
                try:
                    str_new_file_tmp = str_new_file_tmp.replace('.AIR', '').replace('.txt', '')[:70] + '_'  + (datetime.date.today()).strftime("%d%b%Y") + '_' + str(random.randint(0, 999999999)) + '.AIR'

                except:
                    pass

                try:
                    str_directory_file_name = os.path.join(ins_folder_base.str_directory, str_file)
                    
                    # // For solving issue in Kurban as some hidden folders are found inside the capture folder and it is moving in loops
                    if os.path.isdir(str_directory_file_name):
                        continue

                    
                    ## // This portion will activate voucher capturing layer if the adimin setting for voucher capturing is ticked
                    ## // else it will work as normal ticket capturing
                    if bln_enable_voucher_capturing:
                        lst_category = self.get_category_of_file(str_directory_file_name)

                        if lst_category[1] =="V":
                            dct_voucher_status = {"Success":'Passed',"Refund":'Passed',"Duplication":"False"}

                            dct_voucher_status, lst_error_details = ins_voucherCapturing.extract_voucher_data_from_file(str_directory_file_name,dct_voucher_not_parsed_files)
                            if (dct_voucher_status["Success"] == 'Failed' or dct_voucher_status["Refund"] == 'Failed' )and lst_category[0] =="T":
                                  self.extract_ticket_data_from_file(str_directory_file_name)

                            elif lst_category[0] !="T" and  (dct_voucher_status["Success"] != 'Passed' or dct_voucher_status["Refund"] != 'Passed'):
                                try:
                                    str_file_name = os.path.split(str_directory_file_name)[1].split('.')[0]
                                    str_file_name_ticket_num = str_file_name + ':' + str(lst_error_details[0])
                                    if str_file_name_ticket_num not in dct_error_messages:
                                        dct_error_messages[str_file_name_ticket_num] = [str(lst_error_details[0]), lst_error_details[1], str(lst_error_details[2]), str(lst_error_details[3])]
                                except:
                                    pass
#                                self.insert_error_message()
                                # // move file to not parsed directory
                                if sys.platform == "win32":
                                    os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_file)))
                                else:
                                    os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_file)))

                        elif lst_category[1] !="V":
                            dct_voucher_status["Success"] = 'Passed'
                            dct_voucher_status["Refund"] = 'Passed'
                        if lst_category[0] =="T":
                            self.extract_ticket_data_from_file(str_directory_file_name)
                    else:
                        if bln_raise :
                            print(("Processing >> ",str_file))
                        self.extract_ticket_data_from_file(str_directory_file_name)
                        pass
                except IsDirectory as msg:
                    continue
                except InputError as msg:
                    if bln_raise :
                        raise
                    
                    if str_directory_file_name in ins_general_methods.ins_global.dct_no_ticket_files :
                        ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
                        
                    if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:

                        if str_directory_file_name in ins_general_methods.ins_global.dct_no_ticket_files :
                            str_move_to_dir = ins_folder_base.str_no_ticket_files_dir
                            ins_general_methods.ins_global.dct_no_ticket_files.pop(str_directory_file_name)
                        else :
                            str_move_to_dir = ins_folder_base.str_not_parsed_dir

                        if os.access(os.path.join(str_move_to_dir, str_new_file), os.F_OK):
                            str_new_file = str_new_file_tmp
#                        self.insert_error_message()
                        # // move file to not parsed directory
                        if sys.platform == "win32":
                            os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))
                        else:
                            os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))

                        ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                    else:
                        ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
                except DuplicationError as msg:
                    if os.access(os.path.join(ins_folder_base.str_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                    # // move file to parsed directory
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))

                    if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                        ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                    pass
                except OperationalError as msg:
                    if str(msg).strip():
                        print(msg)
                    #if msg.ins_ticket_base.bln_refund == False and msg.ins_ticket_base.str_void_date == '':
                        ## Its a void Ticket but its Issued file is not available.

                    if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:

                        if str_directory_file_name in ins_general_methods.ins_global.dct_no_ticket_files :
                            str_move_to_dir = ins_folder_base.str_no_ticket_files_dir
                            ins_general_methods.ins_global.dct_no_ticket_files.pop(str_directory_file_name)
                        else :
                            str_move_to_dir = ins_folder_base.str_not_parsed_dir


                        if os.access(os.path.join(str_move_to_dir, str_new_file), os.F_OK):
                            str_new_file = str_new_file_tmp

#                        self.insert_error_message()
                        # // move file to not parsed directory
                        if sys.platform == "win32":
                            os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))
                        else:
                            os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))

                        dct_not_parsed_files.pop(str_directory_file_name)
                    else:
                        ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
                except SkipTicketException as msg : 
                    
                    str_move_to_dir = ins_folder_base.str_skipped_imr_files
                    if os.access(os.path.join(str_move_to_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                    ## Move to skipped folder
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))

                except:
                    if bln_raise :
                        raise
                    
                    if str_directory_file_name in ins_general_methods.ins_global.dct_no_ticket_files :
                        ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
                        
                    if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:

                        if str_directory_file_name in ins_general_methods.ins_global.dct_no_ticket_files :
                            str_move_to_dir = ins_folder_base.str_no_ticket_files_dir
                            ins_general_methods.ins_global.dct_no_ticket_files.pop(str_directory_file_name)
                        else :
                            str_move_to_dir = ins_folder_base.str_not_parsed_dir


                        if os.access(os.path.join(str_move_to_dir, str_new_file), os.F_OK):
                            str_new_file = str_new_file_tmp
#                        self.insert_error_message()
                        # // move file to not parsed directory
                        if sys.platform == "win32":
                            os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))
                        else:
                            os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))

                        ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                    else:
                        ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
                else:


                    # // move file to parsed directory
                    # // The execution will reach here only after successfully completing ticket capturing
                    # // but if voucher capturing is enabled we have to check the status of vocher before moving
                    # // the file to parsed folder. The following block of code is used for that
                    if bln_enable_voucher_capturing:
                        if (dct_voucher_status["Success"] == 'Passed' and dct_voucher_status["Refund"] == 'Passed') or dct_voucher_status["Duplication"] == 'True':
                            if sys.platform == "win32":
                                os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_file)))
                            else:
                                os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_file)))


                            if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                                ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                                pass

                        else:
                            if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
#                                self.insert_error_message()
                                # // move file to not parsed directory
                                if sys.platform == "win32":
                                    os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_file)))
                                else:
                                    os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_file)))
                                ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                            else:
                                ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
                    else:
                            if os.access(os.path.join(ins_folder_base.str_parsed_dir, str_new_file), os.F_OK):
                                str_new_file = str_new_file_tmp
                            if sys.platform == "win32":
                                os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                            else:
                                os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))

                            if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                                ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                                pass
                            pass
                pass

            # // Sleep some time
            time.sleep(ins_general_methods.ins_capture_base.int_sleep_time)

            # // Move not parsed folder files to parent folder
            ins_folder_base.int_not_parsed_folder_files_controler += 1

            if ins_folder_base.int_not_parsed_folder_files_controler >= ((3600/ins_general_methods.ins_capture_base.int_sleep_time)*1): # 1 hrs - 3600/5(sleep time) = 720
                self.move_not_parsed_folder_files_to_parent_folder()
                ins_folder_base.int_not_parsed_folder_files_controler = 0

            if self.int_first :
                self.move_not_parsed_folder_files_to_parent_folder()
                self.int_first = 0
                self.ticket_capture()
                
                
            ins_general_methods.ins_db.close()

    def extract_ticket_data_from_file(self, str_file , lst_tsm_number = [], bln_emd_ticket = False, lst_emd_tickets = [],bln_from_web_service = False,str_file_content = '',int_count = 0,bln_start = False):
        str_message = ''
        if bln_from_web_service and int_count:
            ins_general_methods.ins_global.dct_not_parsed_files[str_file] = ''   
        if bln_start :
            ins_general_methods.reload_data()
#            try :
#                ins_general_methods.set_non_iata_capture_details()
#            except :
##                ins_general_methods.connect_db()
#                ins_general_methods.set_non_iata_capture_details()
        
        if ins_general_methods.ins_capture_base.bln_multi_currency:
            str_currency_code = self.set_base_currency(str_file,bln_from_web_service = bln_from_web_service,str_file_content = str_file_content)
            if str_currency_code:
                self.str_defult_currency_code = str_currency_code

        lst_tickets = []
        str_seperator = ''
        # // get file data
        if not bln_emd_ticket:
            lst_tsm_number = []

        fd = None
        bln_has_t_section = False
        if not bln_from_web_service :
            try:
                fd = open(str_file, 'r')
                lst_file_data = fd.readlines()
                fd.close()
            except IOError:
                try:
                    fd.close()
                except:
                    pass
                raise IsDirectory(str_file + ' Is a directory')
            except:
                try:
                    fd.close()
                except:
                    pass
                raise
        else :
            lst_file_data = str_file_content.split('***#|#|#***')

        # // get capture ticket data ins
        ins_capture_ticket_data = instanceBase.CaptureBase()
        ins_capture_emd_ticket_data = instanceBase.CaptureBase()
        ins_capture_ticket_data.str_ins_db_id  = id(ins_general_methods.ins_db)
        if ins_general_methods.dct_conf_data['TRAACS_VERSION'] == 'SAAS':
            ins_save_or_update_data = saveOrUpdateData.captureDB()
        else:
            ins_save_or_update_data = saveOrUpdateDataWave.captureDB()
        ins_create_ticket_base = createTicketBaseInstance.createInstance()

#        if ins_general_methods.ins_capture_base.chr_field_seperator:
#            lst_file_data = lst_file_data[0].split("\r")
        
        ins_capture_ticket_data.str_crs_company  = 'Amadeus'
        ins_capture_emd_ticket_data.str_crs_company  = 'Amadeus'
        ins_capture_ticket_data.str_defult_currency_code = self.str_defult_currency_code
        ins_capture_emd_ticket_data.str_defult_currency_code = self.str_defult_currency_code
        bln_svf_identifier = False
        bln_tst_count = False
        try:
            for str_line in lst_file_data:
                if str_line.split(";")[0][:3] == 'AIR':
                    
                    if str_line.split(";")[1] == 'IM':
                        ins_capture_ticket_data.bln_IMR_file = True
                        ins_capture_emd_ticket_data.bln_IMR_file = True
#                        print "Skipping IMR file " + str_file
#                        raise SkipTicketException,"Skip IMR File "
                    #46542
                    if str_line.split(";")[1] in ('NT','NF','NV','ND'):
                        ins_capture_ticket_data.bln_ndc_content = True
                        ins_capture_emd_ticket_data.bln_ndc_content = True
                    if str_line.split(";")[1] in ('7D','ND'):
                        ins_capture_emd_ticket_data.bln_emd = True
                    pass
                if str_line.split(' ')[0] == "MUC1A":
                    lst_split_line = str_line.split(";")

                    ins_capture_ticket_data.str_pnr_no = str_line.split(" ")[1][:6]
                    ins_capture_ticket_data.str_booking_agency_iata_no = lst_split_line[3]
                    ins_capture_ticket_data.str_ticketing_agency_iata_no = lst_split_line[9]
                    # refer 25579
                    if not ins_capture_emd_ticket_data.bln_emd and not ins_capture_ticket_data.str_ticketing_agency_iata_no and ins_general_methods.ins_auto_inv.bln_xo_capture: #25579 cv change 
                        ins_capture_ticket_data.bln_xo_capturing = True
                        
                    ins_capture_ticket_data.str_pnr_current_owner_iata_no = lst_split_line[7]
                    ins_capture_ticket_data.str_booking_agency_office_id = lst_split_line[2].strip()
                    ins_capture_ticket_data.str_pnr_first_owner_office_id = lst_split_line[4].strip()
                    ins_capture_ticket_data.str_pnr_current_owner_office_id = lst_split_line[6].strip()
                    ins_capture_ticket_data.str_ticketing_agency_office_id = lst_split_line[8].strip()
                    #35761                    
                    ins_capture_ticket_data.str_airline_pnr_number = lst_split_line[-1][3:9].strip() 
                    ins_capture_emd_ticket_data.str_airline_pnr_number = lst_split_line[-1][3:9].strip() 

                    ins_capture_emd_ticket_data.str_pnr_no = ins_capture_ticket_data.str_pnr_no
                    ins_capture_emd_ticket_data.str_booking_agency_iata_no = ins_capture_ticket_data.str_booking_agency_iata_no
                    ins_capture_emd_ticket_data.str_ticketing_agency_iata_no = ins_capture_ticket_data.str_ticketing_agency_iata_no
                    ins_capture_emd_ticket_data.str_pnr_current_owner_iata_no = ins_capture_ticket_data.str_pnr_current_owner_iata_no
                    ins_capture_emd_ticket_data.str_booking_agency_office_id = ins_capture_ticket_data.str_booking_agency_office_id
                    ins_capture_emd_ticket_data.str_pnr_first_owner_office_id = ins_capture_ticket_data.str_pnr_first_owner_office_id
                    ins_capture_emd_ticket_data.str_pnr_current_owner_office_id = ins_capture_ticket_data.str_pnr_current_owner_office_id
                    ins_capture_emd_ticket_data.str_ticketing_agency_office_id = ins_capture_ticket_data.str_ticketing_agency_office_id
                    
                    pass
                elif str_line.split(' ')[0] == "AMD" or str_line.split('R')[0] == "AMD": #// refer #10043
                    ## AMD 0200039533;1/1;VOID02JAN;FLGS
                    lst_split_line = str_line.split(";")

                    try:
                        str_void_portion = lst_split_line[2] # VOID02JAN
                    except:
                        str_void_portion = '                '
                    if str_void_portion[:4] == 'VOID':
                        ins_capture_ticket_data.str_void_date = str_void_portion[4:].strip()
                        
                        if len(ins_capture_ticket_data.str_void_date) > 5 :
                            ins_capture_ticket_data.str_void_date = ins_capture_ticket_data.str_void_date[-5:]
                        ins_capture_ticket_data.str_void_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_void_date,'',ins_capture_ticket_data.str_void_date+str(time.localtime().tm_year)[2:])
                        ins_capture_emd_ticket_data.str_void_date = ins_capture_ticket_data.str_void_date
                            
                elif str_line.split('-')[0] == "A":
                    lst_split_line = str_line.split(";")

                    if len(lst_split_line) > 1:
                        # // Airline Name
                        ins_capture_ticket_data.str_ticketing_airline_name = lst_split_line[0].split('-')[1]
                        # // Airline Character Code
                        ins_capture_ticket_data.str_ticketing_airline_character_code = lst_split_line[1][:2]
                        # // Airline Numeric Code
                        ins_capture_ticket_data.str_ticketing_airline_numeric_code = lst_split_line[1][3:6]

                        ins_capture_emd_ticket_data.str_ticketing_airline_name = ins_capture_ticket_data.str_ticketing_airline_name
                        ins_capture_emd_ticket_data.str_ticketing_airline_character_code = ins_capture_ticket_data.str_ticketing_airline_character_code
                        ins_capture_emd_ticket_data.str_ticketing_airline_numeric_code = ins_capture_ticket_data.str_ticketing_airline_numeric_code
                        pass
 
                elif str_line.split('-')[0] == "B":
                    str_b_line = str_line.split("B-")[1]
                    if ins_capture_ticket_data.bln_ndc_content and str_b_line.startswith('NDC') : #46542
                        ins_capture_ticket_data.int_distribution_type = 1 # 0 -> EDIFACT  1 -> NDC
                        ins_capture_emd_ticket_data.int_distribution_type = 1 # 0 -> EDIFACT  1 -> NDC
                    str_value = ''
                    str_prev = ''
                    bln_pax_data = False
                    lst_b_line = list(str_b_line)
                    lst_pax_nos = []

                    for str_value in lst_b_line :

                        if not str_prev :
                            #if first iteration
                            str_prev = str_value
                            if str_value == 'T' :
                                bln_pax_data = True
                            continue

                        if str_value == 'T' :
                            bln_pax_data = True
                            str_prev = str_value
                            continue
                            
                        if bln_pax_data :

                            if str_value.isdigit() and str_prev != '-' :
                                lst_pax_nos.append(int(str_value))
                            elif str_value.isdigit() and str_prev == '-' :
                                int_start = int(lst_pax_nos[-1]) + 1
                                int_end = int(str_value)
                                while (int_start<= int_end) :
                                    lst_pax_nos.append(int_start)
                                    int_start += 1
                            else :
                                bln_pax_data = False
                                    
                        str_prev = str_value

                    try :
                        #Service Fee identifier here
                        ins_capture_ticket_data.int_tst_count = lst_pax_nos[0]
                        bln_tst_count = True
                    except :
                        ins_capture_ticket_data.int_tst_count = 1


                elif str_line.split('-')[0] == "C":
                    lst_split_line = str_line.split(";")

                    # // Booking Agent Code
                    if len(lst_split_line[0].split('-')[1]) == 14:
                        ins_capture_ticket_data.str_booking_agent_code = lst_split_line[0].split('-')[1][10:12]
                        ins_capture_ticket_data.str_booking_agent_numeric_code = lst_split_line[0].split('-')[1][6:10]
                    elif len(lst_split_line[0].split('-')[1]) == 10:
                        ins_capture_ticket_data.str_booking_agent_code = lst_split_line[0].split('-')[1][6:8]
                        ins_capture_ticket_data.str_booking_agent_numeric_code = ""#lst_split_line[0].split('-')[1][2:6]
                        pass
                    # // Ticketing Agent Code
                    if len(lst_split_line[0].split('-')[2]) == 8:
                        ins_capture_ticket_data.str_ticketing_agent_code = lst_split_line[0].split('-')[2][4:][:2]
                        ins_capture_ticket_data.str_ticketing_agent_numeric_code = lst_split_line[0].split('-')[2][:4]
                    elif len(lst_split_line[0].split('-')[2]) == 4:
                        ins_capture_ticket_data.str_ticketing_agent_code = lst_split_line[0].split('-')[2][:2]
                        ins_capture_ticket_data.str_ticketing_agent_numeric_code = ""
                        pass

                    # // Pricing code
                    if len(lst_split_line[0].split('-')[3]) == 1:
                        ins_capture_ticket_data.str_pricing_code = lst_split_line[0].split('-')[3]

                    ins_capture_emd_ticket_data.str_booking_agent_code = ins_capture_ticket_data.str_booking_agent_code
                    ins_capture_emd_ticket_data.str_booking_agent_numeric_code = ins_capture_ticket_data.str_booking_agent_numeric_code
                    ins_capture_emd_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_ticketing_agent_code
                    ins_capture_emd_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_ticketing_agent_numeric_code

                    pass
                elif str_line.split('-')[0] == "D": # AIR.pdf page no 76
                    lst_split_line = str_line.split(";")

                    # // PNR Creation Date
                    ins_capture_ticket_data.str_pnr_creation_date = lst_split_line[0][2:] # this is taken as the ticket booking date
                    # // PNR Change Date
                    ins_capture_ticket_data.str_pnr_change_date = lst_split_line[1]
                    # // Air Creation Date
                    ins_capture_ticket_data.str_file_creation_date = lst_split_line[2][:6]

                    ins_capture_emd_ticket_data.str_pnr_creation_date = ins_capture_ticket_data.str_pnr_creation_date
                    ins_capture_emd_ticket_data.str_pnr_change_date = ins_capture_ticket_data.str_pnr_change_date
                    ins_capture_emd_ticket_data.str_file_creation_date = ins_capture_ticket_data.str_file_creation_date

                    pass
                elif str_line[:3] == "MCO": # AIR.pdf page no 76
                    lst_split_line = str_line.split(";")

                    ins_capture_ticket_data.str_airline_code = lst_split_line[1][-2:]
                    ins_capture_ticket_data.str_total_amount = lst_split_line[14][3:].strip()
                    ins_capture_ticket_data.str_tax_indicator = lst_split_line[15].strip()

                    if ins_capture_ticket_data.str_tax_indicator == 'Y':
                        ins_capture_ticket_data.flt_market_fare += float(ins_capture_ticket_data.str_total_amount) - float(ins_capture_ticket_data.flt_tax)
                    else:
                        ins_capture_ticket_data.flt_market_fare += float(ins_capture_ticket_data.str_total_amount)

                    pass
                elif str_line.split('-')[0] == "G":
                    # // Ticket Sale Type - X/S
                    ins_capture_ticket_data.chr_ticket_sale_type = 'S' # str_line.split('-')[1][:1]
                    ins_capture_emd_ticket_data.chr_ticket_sale_type = ins_capture_ticket_data.chr_ticket_sale_type
                    pass
                elif str_line.split('-')[0] == "H": # page 79 Air.pdf
                
                    bln_open_segment = False
                    lst_split_line = str_line.split(";")

                    # // Stopover Permitted
                    if lst_split_line[1][3] == 'O':
                        ins_capture_ticket_data.bln_stopover_permitted = True
                        ins_capture_ticket_data.lst_stop_over_airports.append(lst_split_line[3])
                    elif lst_split_line[1][3] == 'X':
                        ins_capture_ticket_data.bln_stopover_permitted = False
                        pass
                    # // Orgin Airport Code
                    ins_capture_ticket_data.str_orgin_airport_code = lst_split_line[1][4:]
                    if ins_capture_ticket_data.str_orgin_airport_code in ins_capture_ticket_data.dct_airport:
                        ins_capture_ticket_data.str_orgin_name = ins_capture_ticket_data.dct_airport[ins_capture_ticket_data.str_orgin_airport_code]

                    if ins_capture_ticket_data.int_number_of_segments == 0:
                        ins_capture_ticket_data.str_start_port_code = ins_capture_ticket_data.str_orgin_airport_code
                        pass

                    ins_capture_ticket_data.str_dest_code = lst_split_line[3]
                    if ins_capture_ticket_data.str_dest_code in ins_capture_ticket_data.dct_airport:
                        ins_capture_ticket_data.str_dest_name = ins_capture_ticket_data.dct_airport[ins_capture_ticket_data.str_dest_code]
                    if lst_split_line[5].strip() in ['VOID','COACH-RESERVED','COACH-UNRESERVED']:
                        ins_capture_ticket_data.lst_sector.append('')
                    ins_capture_ticket_data.lst_sector.append(ins_capture_ticket_data.str_dest_code)
                    lst_splt_air_details = lst_split_line[5].split()
                    ins_capture_ticket_data.str_airline_code = lst_splt_air_details[0]
                    str_class_of_booking = ''
                    if len(lst_splt_air_details) == 5:
                        ins_capture_ticket_data.str_flight_number = lst_splt_air_details[1]
                        ins_capture_ticket_data.str_class_of_service = lst_splt_air_details[2]

                        str_class_of_booking = lst_splt_air_details[3]
                        if not ins_capture_ticket_data.str_class_of_booking.count(str_class_of_booking):
                            if ins_capture_ticket_data.str_class_of_booking:
                                ins_capture_ticket_data.str_class_of_booking += '/'+str_class_of_booking
                            else:
                                ins_capture_ticket_data.str_class_of_booking = str_class_of_booking
                                pass
                            ins_capture_ticket_data.str_class_of_booking = ins_capture_ticket_data.str_class_of_booking.strip('/')
                            ins_capture_emd_ticket_data.str_class_of_booking = ins_capture_ticket_data.str_class_of_booking

                        ### ins_capture_ticket_data.str_class_of_booking


                        ins_capture_ticket_data.str_departure_date = lst_splt_air_details[4][:5]
                        ins_capture_ticket_data.str_departure_time = lst_splt_air_details[4][5:9]
                        ins_capture_ticket_data.str_arrival_time = lst_splt_air_details[4][9:13]
                        ins_capture_ticket_data.str_arrival_date = lst_splt_air_details[4][13:][:5]
                    
                    elif len(lst_splt_air_details) == 6 and lst_splt_air_details[1] != 'OPEN':  ## Air file not getting captured in asfar
                    
                        ins_capture_ticket_data.str_flight_number = lst_splt_air_details[1][:4]
                        ins_capture_ticket_data.str_class_of_service = lst_splt_air_details[1][5]

                        str_class_of_booking = lst_splt_air_details[2]
                        if not ins_capture_ticket_data.str_class_of_booking.count(str_class_of_booking):
                            if ins_capture_ticket_data.str_class_of_booking:
                                ins_capture_ticket_data.str_class_of_booking += '/'+str_class_of_booking
                            else:
                                ins_capture_ticket_data.str_class_of_booking = str_class_of_booking
                                pass
                            ins_capture_ticket_data.str_class_of_booking = ins_capture_ticket_data.str_class_of_booking.strip('/')
                            ins_capture_emd_ticket_data.str_class_of_booking = ins_capture_ticket_data.str_class_of_booking

                        ins_capture_ticket_data.str_departure_date = lst_splt_air_details[3][:5]
                        ins_capture_ticket_data.str_departure_time = lst_splt_air_details[3][5:9]
                        ins_capture_ticket_data.str_arrival_time = lst_splt_air_details[4]
                        ins_capture_ticket_data.str_arrival_date = lst_splt_air_details[5][:5]
                        if ins_capture_ticket_data.int_number_of_segments == 0:
                            ins_capture_ticket_data.str_first_departure_date = ins_capture_ticket_data.str_departure_date
                            ins_capture_emd_ticket_data.str_first_departure_date = ins_capture_ticket_data.str_departure_date
                        
                    elif len(lst_splt_air_details) ==7:
                        ins_capture_ticket_data.str_flight_number = lst_splt_air_details[1]
                        ins_capture_ticket_data.str_class_of_service = lst_splt_air_details[2]

                        str_class_of_booking = lst_splt_air_details[3]
                        if not ins_capture_ticket_data.str_class_of_booking.count(str_class_of_booking):
                            if ins_capture_ticket_data.str_class_of_booking:
                                ins_capture_ticket_data.str_class_of_booking += '/'+str_class_of_booking
                            else:
                                ins_capture_ticket_data.str_class_of_booking = str_class_of_booking
                                pass
                            ins_capture_ticket_data.str_class_of_booking = ins_capture_ticket_data.str_class_of_booking.strip('/')
                            ins_capture_emd_ticket_data.str_class_of_booking = ins_capture_ticket_data.str_class_of_booking

                        ins_capture_ticket_data.str_departure_date = lst_splt_air_details[4][:5]
                        ins_capture_ticket_data.str_departure_time = lst_splt_air_details[4][5:][-4:]
                        if ins_capture_ticket_data.int_number_of_segments == 0:
                            ins_capture_ticket_data.str_first_departure_date = ins_capture_ticket_data.str_departure_date
                            ins_capture_emd_ticket_data.str_first_departure_date = ins_capture_ticket_data.str_departure_date

#                        if not ins_capture_ticket_data.str_departure_date:
                        ins_capture_ticket_data.str_arrival_time = lst_splt_air_details[5][-4:]
                        ins_capture_ticket_data.str_arrival_date = lst_splt_air_details[6][:5]
                        pass
                    else:
                        if len(lst_splt_air_details) == 1 and lst_splt_air_details[0] == 'VOID':
                            ins_capture_ticket_data.str_departure_date = None
                            ins_capture_ticket_data.str_departure_time = None
                            ins_capture_ticket_data.str_arrival_time = None
                            ins_capture_ticket_data.str_arrival_date = None
                            ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments -1 
                            if ins_capture_ticket_data.lst_sector_details :
                                ins_capture_ticket_data.str_airline_code = ins_capture_ticket_data.lst_sector_details[-1][2]
                            
                            bln_open_segment = True
                            
                        if len(lst_splt_air_details) >= 2 and lst_splt_air_details[1] == 'OPEN':
                            ins_capture_ticket_data.str_open_segment_indicator = lst_splt_air_details[1]
                            ins_capture_ticket_data.str_departure_date = lst_split_line[6].strip() or None
                            ins_capture_ticket_data.str_departure_time = None
                            ins_capture_ticket_data.str_arrival_time = None
                            ins_capture_ticket_data.str_arrival_date = None
                            bln_open_segment = True
                            #40719
                            ins_capture_ticket_data.str_class_of_service = lst_splt_air_details[2]
                            ins_capture_ticket_data.str_class_of_booking = lst_splt_air_details[3]
                            pass

                    try:
                        ins_capture_ticket_data.int_mileage = int(lst_split_line[19].strip() or 0) # milage per sector
                    except:
                        ins_capture_ticket_data.int_mileage = 0
                    #refer CR #41852
                    try:
                        if str_line.find('CO2-') != -1 and lst_split_line[23][:4] == 'CO2-':
                            ins_capture_ticket_data.flt_co2_emission +=  float(lst_split_line[23].split('-')[1].strip()[:-2])
                    except:
                        ins_capture_ticket_data.flt_co2_emission = 0.0

                    dbl_sector_wise_fare = 0 #@todo

                    ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments + 1

                    str_issue_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_file_creation_date)
                    if ins_capture_ticket_data.lst_sector_details and ins_capture_ticket_data.str_departure_date:
                        if ins_capture_ticket_data.lst_sector_details[-1][8] :
                            ins_capture_ticket_data.str_departure_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_departure_date,ins_capture_ticket_data.lst_sector_details[-1][8])
                        elif len(ins_capture_ticket_data.lst_sector_details) > 1 and ins_capture_ticket_data.lst_sector_details[-2][8] :
                            ins_capture_ticket_data.str_departure_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_departure_date,ins_capture_ticket_data.lst_sector_details[-2][8])

                    elif ins_capture_ticket_data.str_departure_date :
                        ins_capture_ticket_data.str_departure_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_departure_date,str_issue_date)
                        
                    if ins_capture_ticket_data.str_arrival_date :
                        ins_capture_ticket_data.str_arrival_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_arrival_date,ins_capture_ticket_data.str_departure_date)

#                    if lst_splt_air_details[0] != 'VOID':
                    ins_capture_ticket_data.lst_sector_details.append([ins_capture_ticket_data.str_orgin_airport_code,
                                                                        ins_capture_ticket_data.str_dest_code,
                                                                        ins_capture_ticket_data.str_airline_code,
                                                                        ins_capture_ticket_data.str_ticketing_airline_numeric_code,#37651
                                                                        ins_capture_ticket_data.str_flight_number,
                                                                        ins_capture_ticket_data.str_class_of_service,
                                                                        str_class_of_booking or ins_capture_ticket_data.str_class_of_service,
                                                                        ins_capture_ticket_data.str_arrival_date,
                                                                        ins_capture_ticket_data.str_departure_date,
                                                                        ins_capture_ticket_data.bln_stopover_permitted,
                                                                        ins_capture_ticket_data.int_mileage,
                                                                        dbl_sector_wise_fare,
                                                                        ins_capture_ticket_data.str_arrival_time,
                                                                        ins_capture_ticket_data.str_departure_time,
                                                                        bln_open_segment])



                    pass
                elif str_line.split('-')[0] == "K":
                    lst_split_line = str_line.split(";")

                    if len(lst_split_line) > 1:
                        try:
                            ins_capture_ticket_data.flt_published_fare = lst_split_line[0][3:].strip()
                            ins_capture_ticket_data.str_currency_type_code = ins_capture_ticket_data.flt_published_fare[:3]
                            if ins_capture_ticket_data.str_currency_type_code not in (self.str_defult_currency_code, ):
                                ins_capture_ticket_data.flt_published_fare = float(lst_split_line[1][3:].strip()) or 0
                            else:
                                ins_capture_ticket_data.flt_published_fare = float(ins_capture_ticket_data.flt_published_fare[3:].strip()) or 0
                                pass
                        except:
                              ins_capture_ticket_data.flt_published_fare = 0.0

                        ins_capture_ticket_data.flt_total_amount_collected = lst_split_line[12][3:]
                        ins_capture_ticket_data.bln_k_section_fare = True
                    else:
                        pass
                    pass

                elif str_line.split('-')[0] == "KS":
                    lst_split_line = str_line.split(";")
                    # // In case of pricing code is 'B' then the value is manually edited and the total amount collected is from this section
                    # // or (G or F) and not flt_total_amount_collected then total amount collected is from this section
                    if ins_capture_ticket_data.str_pricing_code in ('B','G','F','M'): # refer bug no: 11322 , if the file having KS section and pricing code is B,G or F means amount take from KS section
                        ins_capture_ticket_data.flt_total_amount_collected = lst_split_line[12][3:]
                        ins_capture_ticket_data.bln_k_section_fare = True
                    else:
                        pass
                    pass

                elif str_line.split('-')[0] == "ATC":
                    # Need to consider this section , if it is a reissue ticket
                    # said by Dass sir.
                    # Bug #8105
                    # // Since Tax balance and fare balance from ATC section was not reliable,
                    # // Decided to Take Total amount from 7th Position ( TST Total ) and Subtract tax amount To get fare
                    # // After discussion with Mohan Das Sir Refer Bug #9875
                    try:
                        lst_split_line = str_line.split(";")
                        if len(lst_split_line) > 7:
                            ins_capture_ticket_data.flt_it_total = float(lst_split_line[7][3:].strip())
                            ins_capture_ticket_data.str_it_total = str(ins_capture_ticket_data.flt_it_total)
                        pass
                    except:
                        ins_capture_ticket_data.flt_it_total = 0.0

                elif str_line.split('-')[0] == "M":
                    lst_split_line = str_line.split(";")
                    lst_ticket_designator = lst_split_line
                    ins_capture_ticket_data.str_ticket_designator = lst_ticket_designator[0][-6:].strip().strip('\n').strip()

                    if not ins_capture_ticket_data.str_ticket_designator:
                        ins_capture_ticket_data.str_ticket_designator = ''
                    int_flag = 0
                    for str_fare_basis in lst_split_line:
                        str_new_fare_basis = ''

                        if len(lst_split_line) == 1:
                            str_new_fare_basis = lst_split_line[0][2:17]
                        elif len(str_fare_basis) == 17 and int_flag == 0:
                            str_new_fare_basis = str_fare_basis.split("-")[1]
                            int_flag = 1
                            pass

                        elif len(str_fare_basis) == 15 or int_flag == 1:
                            str_new_fare_basis = str_fare_basis
                            pass

                        if str_new_fare_basis:
                            ins_capture_ticket_data.lst_fare_basis.append(str_new_fare_basis.strip())
                            pass
                        pass
                    pass
                elif str_line.split('-')[0] == "Q":
                    lst_split_line = str_line.split(";")
                    ins_capture_ticket_data.str_fare_construction = lst_split_line[0][2:].strip().strip('\n').strip()
                    if "NUC" in lst_split_line[0]:
                        str_splitter = "NUC"
                    else:
                        str_splitter = "END"
                        pass

                    lst_temp_sector = lst_split_line[0].split(str_splitter)[0].strip().split()
                    lst_temp_sector.reverse()

                    int_index = 0
                    lst_new_sector = []
                    for str_sector in lst_temp_sector:
                        if int_index == 0:
                            if str_sector[:3].isalpha():
                                lst_new_sector.append(str_sector[:3])
                                pass
                            pass
                        else:
                            if len(str_sector) > 2:
                                if str_sector[:3].isalpha():
                                    lst_new_sector.append(str_sector[:3])
                                elif str_sector[-3:].isalpha():
                                    lst_new_sector.append(str_sector[-3:])
                                    pass
                                pass
                            pass

                        int_index = int_index + 1
                        pass
                    pass

                    lst_new_sector.reverse()
                    ins_capture_ticket_data.lst_new_sector = lst_new_sector
                    pass
                elif str_line.split('-')[0] == "I":
                    
                    ins_capture_ticket_data.str_i_section_pax_name = ''
                    ins_capture_ticket_data.str_isection_email = ''
                    ins_capture_ticket_data.str_isection_mobile = ''
                    ins_capture_ticket_data.str_isection_email1 = ''
                    ins_capture_ticket_data.str_isection_mobile1 = ''
                    
                    lst_split_line = str_line.split(";")
                    int_passenger_num = 0
                    if lst_split_line[0]:
                        try:
                            lst_i_section_data = lst_split_line[0].split("-")
                            if len(lst_i_section_data)>1:
                                int_passenger_num = int(lst_i_section_data[1])
                            self.int_pax_identifier = int_passenger_num
                        except Exception as msg:
                            self.int_pax_identifier = None
                            pass
                        try:  
                            lst_email_section = lst_split_line[3].split("//")

                            for str_data in lst_email_section:
                                if str_data.split("-")[0] == "E" or str_data.split("-")[0][-1] == 'E':
                                    if not ins_capture_ticket_data.str_isection_email:
                                            ins_capture_ticket_data.str_isection_email = '-'.join(str_data.split("-")[1:]).split('/')[0].strip()[:50]
                                    elif not ins_capture_ticket_data.str_isection_email1:
                                            ins_capture_ticket_data.str_isection_email1 = '-'.join(str_data.split("-")[1:]).strip().split('/')[0].strip()[:50]
                                            
                                elif str_data.strip()[0] == "M" or str_data.strip()[0] == "N": #Refer CR #38412
                                    if not ins_capture_ticket_data.str_isection_mobile :
                                        if str_data.find("+") != -1 :
                                            ins_capture_ticket_data.str_isection_mobile = str_data.split("+")[1].strip()
                                        elif str_data.find("-") != -1 :
                                            ins_capture_ticket_data.str_isection_mobile = str_data.split("-")[1].strip()    

                                    elif not ins_capture_ticket_data.str_isection_mobile1:
                                        if str_data.find("+") != -1 :
                                            ins_capture_ticket_data.str_isection_mobile1 = str_data.split("+")[1].strip()
                                        elif str_data.find("-") != -1 :
                                            ins_capture_ticket_data.str_isection_mobile1 = str_data.split("-")[1].strip()    
                                            
                                elif str_data.strip()[-1] == "M":
                                    try :
                                        str_isection_mobile = str_data.strip('AP').split('-')[0].replace(' ','').replace('+','').replace('M','')

                                        if not ins_capture_ticket_data.str_isection_mobile :
                                            ins_capture_ticket_data.str_isection_mobile = str_isection_mobile
                                        elif not ins_capture_ticket_data.str_isection_mobile1:
                                            ins_capture_ticket_data.str_isection_mobile1 = str_isection_mobile
                                    except :
                                        pass
                                #Refer #39210
                                elif str_data.strip()[:3] == 'APM':
                                    try :
                                        str_isection_mobile = str_data.strip('AP').split('-')[1].replace('+','').replace('M','').strip()

                                        if not ins_capture_ticket_data.str_isection_mobile :
                                            ins_capture_ticket_data.str_isection_mobile = str_isection_mobile
                                        elif not ins_capture_ticket_data.str_isection_mobile1:
                                            ins_capture_ticket_data.str_isection_mobile1 = str_isection_mobile
                                    except :
                                        pass
                                            
                        except Exception as m:
                            pass
                    try:
                        # // Pax Type - Defalult 'ADT'
                        lst_pax_details = lst_split_line[1].split("(")
                    except:
                        continue

                    str_pax_type = ''
                    ins_capture_ticket_data.str_pax_type = 'ADT'
                    if len(lst_pax_details) > 1:
                        ins_capture_ticket_data.str_pax_type = lst_pax_details[-1][:-1].strip()
                        str_pax_type = '(' + lst_pax_details[-1]

                        pass
                    # // Pax Name
                    ins_capture_ticket_data.str_pax_name = lst_split_line[1].replace(str_pax_type, '')[2:].strip()
                    if ins_capture_ticket_data.str_pax_type != 'ADT' and ins_capture_ticket_data.str_pax_type != 'CHD':
                        lst_pax_type = ins_capture_ticket_data.str_pax_name.split('(')
                        if len(lst_pax_type) > 1:
                            str_pax_type = lst_pax_type[-1].replace(')', '')
                            if str_pax_type == 'ADT' or str_pax_type == 'CHD':
                                ins_capture_ticket_data.str_pax_type = str_pax_type
                                pass
                        else:
                            ins_capture_ticket_data.str_pax_type = ins_capture_ticket_data.str_pax_type[:10].strip()
                            pass
                        
                    #// Ref No- 11548- Tickets moving to non parsed due to wrong length of pax type
                    ins_capture_ticket_data.str_pax_type = ins_capture_ticket_data.str_pax_type[:10].strip()
                    if ins_capture_ticket_data.str_pax_type.strip() == '':
                        ins_capture_ticket_data.str_pax_type = 'ADT'
                        pass

                    ins_capture_emd_ticket_data.str_pax_type = ins_capture_ticket_data.str_pax_type
                    ins_capture_emd_ticket_data.str_pax_name = ins_capture_ticket_data.str_pax_name

                    ins_capture_emd_ticket_data.str_isection_mobile = ins_capture_ticket_data.str_isection_mobile
                    ins_capture_emd_ticket_data.str_isection_email = ins_capture_ticket_data.str_isection_email
                    
                    try :
                        int_pax_num = int(lst_split_line[0].split('-')[1])
                    except :
                        int_pax_num = 0
                    ins_capture_ticket_data.int_pax_item_number = ins_capture_emd_ticket_data.int_pax_item_number = int_pax_num
                    
                    ins_capture_ticket_data.dct_pax_name[int(int_pax_num)] = ins_capture_ticket_data.str_pax_name
                    ins_capture_emd_ticket_data.dct_pax_name[int(int_pax_num)] = ins_capture_ticket_data.str_pax_name
                    ins_capture_ticket_data.lst_pax_names.append([ins_capture_ticket_data.str_pax_name , ins_capture_ticket_data.str_pax_type])

                elif str_line.split('-')[0] == "U" :
                    if  ins_general_methods.ins_auto_inv.bln_voucher_capture:
                        ## Refer 13823
                     
                        try : 
                            str_line = str_line.strip().strip(';')
                            lst_line = str_line.split(';')

                            int_voucher_identifier = int(lst_line[1][:3])

                            try :
                                int_pax_identifier = int(lst_line[-1].strip('P').strip())
                            except :
                                try :
                                    int_pax_identifier = int(str_line.split('/')[-1].split('-')[0].strip().strip('P').strip())
                                except :
                                    int_pax_identifier = 1

                            if str_line[10:16] == 'HTL 1A' :
                                ins_voucher_base = instanceBase.VoucherBase()

                                ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                ins_voucher_base.int_pax_identifier = int_pax_identifier
                                ins_voucher_base.str_voucher_type = 'H'
                                ins_voucher_base.str_voucher_category = 'HOTEL'
                                ins_capture_ticket_data.bln_voucher_present_in_file = True
                                ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['VOUCHER_SERVICE_CODE'].strip(':')
                                ins_voucher_base.str_voucher_service_name = "HOTEL"

                                try :
                                    ins_voucher_base.int_no_of_guest_inv = int(lst_line[2][0:5].strip().strip('HK'))
                                except :
                                    ins_voucher_base.int_no_of_guest_inv = 1

                                ins_voucher_base.int_no_of_adults_inv = ins_voucher_base.int_no_of_guest_inv

                                ins_voucher_base.str_city_code = lst_line[4].strip()
                                ins_voucher_base.str_city_name = lst_line[5].strip()


                                if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                    ins_voucher_base.str_hotel_check_in_date = lst_line[2][-9:-4].strip()
                                    ins_voucher_base.str_hotel_check_out_date = lst_line[3][:-4].strip()
                                    pass
                                else :
                                    ins_voucher_base.str_hotel_check_in_date = lst_line[2][-5:].strip()
                                    ins_voucher_base.str_hotel_check_out_date = lst_line[3].strip()

                                try :
                                    ins_voucher_base.int_no_of_rooms = int(lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['NO_OF_ROOMS'])[1].split('/')[0].strip())
                                except :
                                    ins_voucher_base.int_no_of_rooms = 1

                                try :
                                    ins_voucher_base.str_meals_plan = lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['MEALS_PLAN'])[1].split('/')[0].strip()
                                except :
                                    ins_voucher_base.str_meals_plan = ''

                                str_room_type_1 = str_room_type_2 = ''
                                if lst_line[6].find("RO:") != -1 :
                                    str_room_type_1 = lst_line[6].split("RO:")[1].split('/')[0].strip().upper()
                                if lst_line[6].find("RT:") != -1 :
                                    str_room_type_2 = lst_line[6].split("RT:")[1].split('/')[0].strip().upper()


                                ins_voucher_base.str_room_type = str_room_type_1 +' ROOM - '+ str_room_type_2

                                if lst_line[6].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['HOTEL_NAME']) != -1 :
                                    ins_voucher_base.str_hotel_name = lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['HOTEL_NAME'])[1].split('/')[0].strip()
                                elif lst_line[6].find('HN:') != -1 :
                                    ins_voucher_base.str_hotel_name = lst_line[6].split('HN:')[1].split('/')[0].strip()
                                elif ins_general_methods.ins_capture_base.bln_consider_dana_hotel_voucher_capture and not ins_voucher_base.str_hotel_name:
                                        ins_voucher_base.str_hotel_name = lst_line[6].split(',')[0].strip()
                                else :
                                    continue

                                if lst_line[6].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['HOTEL_CODE']) != -1 :
                                    ins_voucher_base.str_property_code = lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['HOTEL_CODE'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_property_code = ''

                                try :
                                    if ins_general_methods.ins_capture_base.bln_consider_dana_hotel_voucher_capture:
                                        ins_voucher_base.flt_fare_inv = float(lst_line[6].split(',')[3][3:].strip().replace(self.str_defult_currency_code,''))
                                    else:
                                        ins_voucher_base.flt_fare_inv = float(lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['TOTAL_FARE'])[1].split('/')[0].strip().replace(self.str_defult_currency_code,''))
                                except :
                                    ins_voucher_base.flt_fare_inv = 0.0

                                if lst_line[6].find("SI:") != -1 :
                                    ins_voucher_base.str_remarks = lst_line[6].split("SI:")[1].split('/')[0].split(';')[0].strip().upper()

                                if lst_line[6].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['SUPPLIER_CODE']) != -1 :
                                    ins_voucher_base.str_vendor_code = lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_vendor_code = ''

                                if lst_line[6].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['HOTEL_CONF_NO']) != -1 :
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['HOTEL_CONF_NO'])[1].split('/')[0].strip()
                                elif ins_general_methods.ins_capture_base.bln_consider_dana_hotel_voucher_capture and not ins_voucher_base.str_hotel_confirm_number:
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['HOTEL_CONF_NO'][:-1])[1].split(',')[0].strip()
                                elif ins_general_methods.dct_conf_data.get('htl_1a_voucher_capture','').upper() == 'TRUE' and ins_general_methods.dct_conf_data.get('htl_split_chr','') != '':    #refer #41471
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[6].split('CF-')[1].split(ins_general_methods.dct_conf_data['htl_split_chr'])[0]
                                else :
                                    ins_voucher_base.str_hotel_confirm_number = ''

                                try :
                                    ins_voucher_base.str_voucher_number = lst_line[6].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['VOUCHER_NO'])[1].split('/')[0].strip()
                                except :
                                    if ins_voucher_base.str_hotel_confirm_number :
                                        ins_voucher_base.str_voucher_number = ins_voucher_base.str_hotel_confirm_number
                                    else :
                                        continue

                                #refer #41471
                                if ins_general_methods.dct_conf_data.get('htl_1a_voucher_capture','').upper() == 'TRUE' and ins_general_methods.dct_conf_data.get('htl_split_chr','') != '':
                                    
                                    lst_htl_line = lst_line[6].split(ins_general_methods.dct_conf_data['htl_split_chr'])
                                    try:
                                        ins_voucher_base.str_room_type = lst_htl_line[1].replace('RATE','').strip()
                                    except:
                                        ins_voucher_base.str_room_type = ''
                                    
                                    try:
                                        if lst_line[6].find('**') != -1:
                                            ins_voucher_base.str_credit_card_num = lst_htl_line[4][2:18].strip()
                                            ins_voucher_base.str_cc_type = lst_htl_line[4][:2].strip()
                                    except:
                                        ins_voucher_base.str_credit_card_num = ''
                                        ins_voucher_base.str_cc_type = ''
                                    try:    
                                        if lst_line[6].find('RATE*()') != -1:
                                            lst_htl_line1 = lst_line[6].split('RATE*()')
                                            ins_voucher_base.str_voucher_currency_code = lst_htl_line1[1].split('*')[0][-3:].strip()
                                            ins_voucher_base.flt_fare_inv = float(lst_htl_line1[1].split('*')[0][:-3].strip())

                                        elif ins_general_methods.dct_conf_data['htl_split_chr'] == ',':
                                            ins_voucher_base.str_voucher_currency_code = lst_htl_line[3][:3].strip()
                                            ins_voucher_base.flt_fare_inv = float(lst_htl_line[3][3:].strip())
                                    except:
                                        ins_voucher_base.str_voucher_currency_code = ''
                                        ins_voucher_base.flt_fare_inv = 0.0
                                        
                                    if not ins_voucher_base.str_supplier_confirm_number and ins_voucher_base.str_hotel_confirm_number:
                                        ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_hotel_confirm_number
                                    
                                    if not ins_voucher_base.str_vendor_code:
                                        ins_voucher_base.str_vendor_code =  ins_voucher_base.str_hotel_name 
                                
                                ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                    ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                    ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)
                                pass

                            elif str_line[10:13] == 'HHL' :
                                #refs #17717
                                ###  Amadeus hotel booking format
                                ins_voucher_base1 = instanceBase.VoucherBase()

                                ins_voucher_base1.int_voucher_identifier = int_voucher_identifier
                                ins_voucher_base1.int_pax_identifier = int_pax_identifier
                                ins_voucher_base1.str_voucher_type = 'H'
                                ins_voucher_base1.str_voucher_category = 'HOTEL'
                                ins_voucher_base1.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['VOUCHER_SERVICE_CODE'].strip(':')
                                ins_voucher_base1.str_voucher_service_name = "HOTEL"
                                ins_capture_ticket_data.bln_voucher_present_in_file = True

                                try :
                                    ins_voucher_base1.int_no_of_guest_inv = int(lst_line[2][0:5].strip().strip('HK'))
                                except :
                                    ins_voucher_base1.int_no_of_guest_inv = 1

                                ins_voucher_base1.int_no_of_adults_inv = ins_voucher_base1.int_no_of_guest_inv

                                ins_voucher_base1.str_hotel_chain_code = lst_line[1][7:9].strip()
                                ins_voucher_base1.str_city_code = lst_line[4].strip()
                                ins_voucher_base1.str_city_name = lst_line[5].strip()
                                if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                    ins_voucher_base1.str_hotel_check_in_date = lst_line[2][-9:-4].strip()
                                    ins_voucher_base1.str_hotel_check_out_date = lst_line[3][:-4].strip()
                                    pass
                                else :
                                    ins_voucher_base1.str_hotel_check_in_date = lst_line[2][-5:].strip()
                                    ins_voucher_base1.str_hotel_check_out_date = lst_line[3].strip()

                                if lst_line[8][:3] != lst_line[8][3:] :
                                    flt_voucher_amount = lst_line[9].split('+')[-1]
                                else :
                                    flt_voucher_amount = lst_line[9].split('+')[0]

                                try :
                                    ins_voucher_base1.flt_fare_inv = float(flt_voucher_amount)
                                except :
                                    ins_voucher_base1.flt_fare_inv = 0.0

                                if lst_line[8][:3] != lst_line[8][3:] :
                                    int_no_of_nights = lst_line[9].split('+')[2]
                                else :
                                    int_no_of_nights = lst_line[9].split('+')[2]
                                ins_voucher_base1.str_rate_type = lst_line[20].strip()  #40299
                                ins_voucher_base1.str_book_rate = lst_line[9].split('+')[0]
                                
                                try :
                                    ins_voucher_base1.int_no_of_rooms = int(lst_line[6][0])
                                except :
                                    ins_voucher_base1.int_no_of_rooms = 1

                                ins_voucher_base1.flt_fare_inv = ins_voucher_base1.flt_fare_inv * ins_voucher_base1.int_no_of_rooms
                                ins_voucher_base1.str_room_type = lst_line[57].strip() #40777
                                
                                ins_voucher_base1.str_meals_plan = lst_line[6][1:4]
                                str_rate_plan = lst_line[6][4:]
                                str_rate_type_booking_code = lst_line[23][3:]
                                ins_voucher_base1.str_iata_num = lst_line[24][3:]

                                ins_voucher_base1.str_hotel_confirm_number = lst_line[25][3:]
                                ins_voucher_base1.str_supplier_confirm_number = ins_voucher_base1.str_hotel_confirm_number
                                ins_voucher_base1.str_voucher_number = ins_voucher_base1.str_hotel_confirm_number

                                str_guarantee_method = lst_line[33][2:22]

                                if str_guarantee_method[:2] == 'CC' :
                                    ins_voucher_base1.str_credit_card_num = str_guarantee_method[4:20].strip()
                                    ins_voucher_base1.str_cc_type = str_guarantee_method[2:4].strip()

                                    if len(ins_voucher_base1.str_credit_card_num) < 15 :
                                        ins_voucher_base1.str_credit_card_num = ''


                                ins_voucher_base1.str_hotel_name = lst_line[42]
                                ins_voucher_base1.str_hotel_address = lst_line[43] + ' ' + lst_line[44] +' ' +  lst_line[45] + ' PO ' +  lst_line[49]

                                ins_voucher_base1.str_hotel_phone = lst_line[50]
                                ins_voucher_base1.str_hotel_fax = lst_line[51]



                                ins_voucher_base1.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                ins_voucher_base1.bln_hhl = True

                                if ins_voucher_base1.str_voucher_number and ins_voucher_base1.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                    ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base1.str_voucher_number)
                                    ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base1)
                                pass

                            elif str_line[10:13] == 'CCR' :#40359
                                ins_voucher_base2 = instanceBase.VoucherBase()
                                ins_voucher_base2.int_voucher_identifier = int_voucher_identifier
                                ins_voucher_base2.int_pax_identifier = int_pax_identifier
                                ins_voucher_base2.str_voucher_type = 'C'
                                ins_voucher_base2.str_voucher_category = 'CAR'
                                ins_voucher_base2.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['CAR']['VOUCHER_SERVICE_CODE'].strip(':')
                                ins_voucher_base2.str_voucher_service_name = "CAR"
                                ins_capture_ticket_data.bln_voucher_present_in_file = True
                                
                                try :
                                    ins_voucher_base2.int_no_of_car = int(lst_line[1][-2:])
                                except :
                                    ins_voucher_base2.int_no_of_car = 1
                                    
                                try:
                                    ins_voucher_base2.str_vendor_code = lst_line[1].split(' ')[1]
                                except:
                                    ins_voucher_base2.str_vendor_code = ''
                                    
                                if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                    ins_voucher_base2.str_pick_up_date = lst_line[2][-9:-4].strip()
                                else :
                                    ins_voucher_base2.str_pick_up_date = lst_line[2][-5:].strip()
                                    
                                if len(lst_line[3].strip()) > 6: 
                                    ins_voucher_base2.str_drop_off_date = lst_line[3][:-4].strip()
                                else :
                                    ins_voucher_base2.str_drop_off_date = lst_line[3].strip()

                                ins_voucher_base2.str_city_code = lst_line[4].strip()
                                ins_voucher_base2.str_city_name = lst_line[5].strip()
                                ins_voucher_base2.str_remarks = "CODE:%s,NAME:%s"%(ins_voucher_base2.str_city_code,ins_voucher_base2.str_city_name)
                                ins_voucher_base2.str_booking_details = "CAR CODE:%s ,CAR TYPE:%s"%(lst_line[6].strip(),lst_line[7].strip())#car details
                                ins_voucher_base2.str_iata_num = lst_line[13][3:]
                                ins_voucher_base2.str_supplier_confirm_number = lst_line[15][3:]
                                ins_voucher_base2.str_voucher_number = ins_voucher_base2.str_supplier_confirm_number
                                
                                if lst_line[19].find('*') != -1:
                                    ins_voucher_base2.str_drop_off_location = lst_line[19].split('*')[1].strip()
                                    ins_voucher_base2.bln_with_dropup = True #tbl_car_voucher_sales
                                else:
                                    ins_voucher_base2.str_drop_off_location = ''
                                    ins_voucher_base2.bln_with_dropup = False
                                
                                str_form_of_payment = lst_line[20][3:]

                                if str_form_of_payment[:2] == 'CC' :
                                    ins_voucher_base2.str_credit_card_num = str_form_of_payment[4:20].strip()
                                    ins_voucher_base2.str_cc_type = str_form_of_payment[2:4].strip()
                                    if len(ins_voucher_base2.str_credit_card_num) >= 15 :
                                        (int_account_master_id,int_account_type) = ins_general_methods.get_corporate_card_id(ins_voucher_base2.str_credit_card_num,'AMADEUS')
                                        ins_voucher_base2.int_corp_card_id = int_account_master_id
                                    else:
                                        ins_voucher_base2.str_credit_card_num = ''
                                        ins_voucher_base2.int_corp_card_id = None
                                
                                if lst_line[28].find('*') != -1:
                                    ins_voucher_base2.str_pick_up_location = lst_line[28][4:].strip()
                                    ins_voucher_base2.bln_with_pickup = True
                                else:
                                    ins_voucher_base2.str_pick_up_location = ''
                                    ins_voucher_base2.bln_with_pickup = False
                                
                                try:
                                    ins_voucher_base2.int_no_of_days = int(lst_line[34].split('+')[1][:1])
                                except:
                                    ins_voucher_base2.int_no_of_days = 1
                                    
                                ins_voucher_base2.str_rate_type = lst_line[30][-2:] or ''
                                
                                try:
                                    ins_voucher_base2.str_voucher_currency_code = lst_line[34].split('+')[0][3:6] #eg:ES-GBP109.10
                                except:
                                    ins_voucher_base2.str_voucher_currency_code = ins_capture_ticket_data.str_defult_currency_code
                                    
                                try:
                                    ins_voucher_base2.flt_fare_inv = float(lst_line[34].split('+')[0][6:]) #eg:ES-GBP109.10
                                except:
                                    ins_voucher_base2.flt_fare_inv = 0.0
                                
                                
                                ins_voucher_base2.str_car_details = lst_line[78][4:]
                                
                                ins_voucher_base2.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                
                                if ins_voucher_base2.str_voucher_number and ins_voucher_base2.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                    ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base2.str_voucher_number)
                                    ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base2)
                                pass

                            #refer #41724 - Car voucher capturing
                            elif str_line[10:13] == "CAR":
                                ins_voucher_base3 = instanceBase.VoucherBase()
                                ins_voucher_base3.int_voucher_identifier = int_voucher_identifier
                                ins_voucher_base3.int_pax_identifier = int_pax_identifier
                                ins_voucher_base3.str_voucher_type = 'C'
                                ins_voucher_base3.str_voucher_category = 'CAR'
                                ins_capture_ticket_data.bln_voucher_present_in_file = True
                                ins_voucher_base3.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['CAR']['VOUCHER_SERVICE_CODE'].strip(':')
                                ins_voucher_base3.str_voucher_service_name = "CAR"
                                try :
                                    ins_voucher_base3.int_no_of_car = int(lst_line[1][-2:])
                                except :
                                    ins_voucher_base3.int_no_of_car = 1
                                #status code 1a
                                if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                    ins_voucher_base3.str_pick_up_date = lst_line[2][-9:-4].strip()
                                else :
                                    ins_voucher_base3.str_pick_up_date = lst_line[2][-5:].strip()
                            
                                if len(lst_line[2].strip()) > 6:
                                    ins_voucher_base3.str_drop_off_date = lst_line[3][:-4].strip()
                                else :
                                    ins_voucher_base3.str_drop_off_date = lst_line[3].strip()
                                    
                                ins_voucher_base3.str_city_code = lst_line[4].strip()
                                ins_voucher_base3.str_city_name = lst_line[5].strip()
                                ins_voucher_base3.str_remarks = "CODE: %s, NAME: %s"%(ins_voucher_base3.str_city_code,ins_voucher_base3.str_city_name)
                                ins_voucher_base3.str_booking_details = "CAR CODE: %s, CAR TYPE: %s"%(lst_line[6].strip(),lst_line[7].strip())
                                
                                lst_car_details = lst_line[8].split('*')
                                ins_voucher_base3.str_vendor_code = lst_car_details[1].strip()
                                
                                if lst_car_details[7].startswith('RATE'):
                                    ins_voucher_base3.str_voucher_currency_code = lst_car_details[7][-3:].strip()
                                    ins_voucher_base3.flt_fare_inv = float(lst_car_details[7][4:].replace(ins_voucher_base3.str_voucher_currency_code,''))
                                else:
                                    ins_voucher_base3.str_voucher_currency_code = ins_capture_ticket_data.str_defult_currency_code
                                    ins_voucher_base3.flt_fare_inv = 0.0
                                
                                str_fop = lst_car_details[8].split('/')[0]
                                ins_voucher_base3.str_credit_card_num = str_fop[2:18].strip()
                                ins_voucher_base3.str_cc_type = str_fop[:2].strip()
                                
                                ins_voucher_base3.str_supplier_confirm_number = lst_line[8].split('CF-')[1].split(';')[0].strip()
                                ins_voucher_base3.str_voucher_number = ins_voucher_base3.str_supplier_confirm_number
                                
                                if ins_voucher_base3.str_voucher_number and ins_voucher_base3.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                    ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base3.str_voucher_number)
                                    ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base3)
                                pass
                                

                            elif str_line[10:16] == 'ATX 1A' :

                                ins_voucher_base = instanceBase.VoucherBase()

                                ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                ins_voucher_base.int_pax_identifier = int_pax_identifier
                                ins_voucher_base.str_voucher_type = 'O'
                                ins_voucher_base.str_voucher_category = 'TRANSFER'
                                ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['VOUCHER_SERVICE_CODE'].strip(':')
                                ins_voucher_base.str_voucher_service_name = "TRANSFERS"
                                ins_capture_ticket_data.bln_voucher_present_in_file = True
                                try :
                                    ins_voucher_base.int_no_of_guest_inv = int(lst_line[2][0:5].strip().strip('HK'))
                                except :
                                    ins_voucher_base.int_no_of_guest_inv = 1


                                if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                    ins_voucher_base.str_pick_up_date = lst_line[2][-9:-4].strip()
                                else :
                                    ins_voucher_base.str_pick_up_date = lst_line[2][-5:]

                                ins_voucher_base.bln_from_to_date = True
                                ins_voucher_base.str_from_date = ins_voucher_base.str_pick_up_date

                                if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['PICKUP_LOCATION']) != -1 :
                                    ins_voucher_base.str_pick_up_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['PICKUP_LOCATION'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_pick_up_location = ''

                                if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['PICKUP_TIME']) != -1 :
                                    ins_voucher_base.str_pick_up_time = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['PICKUP_TIME'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_pick_up_time = ''

                                if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['DROP_OFF_LOCATION']) != -1 :
                                    ins_voucher_base.str_drop_off_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['DROP_OFF_LOCATION'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_drop_off_location = ''

                                if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['DRIVER_PHONE']) != -1 :
                                    ins_voucher_base.str_driver_ph_no  = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['DRIVER_PHONE'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_driver_ph_no = ''

                                if lst_line[5].find("CT:") != -1 :
                                    ins_voucher_base.str_car_type  = lst_line[5].split("CT:")[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_car_type = ''

                                if lst_line[5].find('TOS:') != -1 :
                                    ins_voucher_base.str_voucher_code  = lst_line[5].split('TOS:')[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_voucher_code = ''

                                if ins_voucher_base.str_voucher_code.find("LIMO") != -1 :
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['VOUCHER_SERVICE_CODE'].strip(':')
                                else :
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['VOUCHER_SERVICE_CODE'].strip(':')

                                ins_voucher_base.str_voucher_service_name = ins_voucher_base.str_voucher_code

                                try :
                                    ins_voucher_base.flt_fare_inv = float(lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['TOTAL_FARE'])[1].split('/')[0].strip().replace(self.str_defult_currency_code,''))
                                except :
                                    ins_voucher_base.flt_fare_inv = 0.0
                                    pass

                                if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['SUPPLIER_CODE']) != -1 :
                                    ins_voucher_base.str_vendor_code = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_vendor_code = ''

                                if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['CONF_NO']) != -1 :
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['CONF_NO'])[1].split('/')[0].strip()
                                else :
                                    ins_voucher_base.str_hotel_confirm_number = ''

                                if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['VOUCHER_NO']) != -1 :
                                    ins_voucher_base.str_voucher_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['VOUCHER_NO'])[1].split('/')[0].strip()
                                else :
                                    continue

                                ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                ins_voucher_base.str_particulars += "From:%s,"%ins_voucher_base.str_pick_up_location
                                ins_voucher_base.str_particulars += "To:%s,"%ins_voucher_base.str_drop_off_location
                                ins_voucher_base.str_particulars += "Make:,Model:,Type:,Chauffeur:"

                                ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                ins_voucher_base.bln_transfer_voucher = True

                                if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                    ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                    ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                pass
                            
                            
                            elif str_line[10:13] == 'INS' or str_line[9:12] == "INS" :
                                
                                ins_voucher_base = instanceBase.VoucherBase()
                                ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                ins_voucher_base.int_pax_identifier = int_pax_identifier
                                ins_voucher_base.str_voucher_type = 'O'
                                str_service_type = ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_SERVICE_CODE'].strip(':')
                                ins_voucher_base.str_voucher_category = str_service_type
                                ins_voucher_base.str_voucher_code = str_service_type
                                ins_voucher_base.str_voucher_service_name = str_service_type
                                ins_capture_ticket_data.bln_voucher_present_in_file = True
                                
                                try:
                                    ins_voucher_base.int_no_of_guests = int(lst_line[4].strip())
                                except:
                                    ins_voucher_base.int_no_of_guests = 1

                                str_from_date = datetime.datetime.strptime((lst_line[7]), "%d%b%Y").date()
                                str_to_date = datetime.datetime.strptime((lst_line[8]), "%d%b%Y").date()
                                
                                ins_voucher_base.str_from_date = lst_line[7][:5]
                                ins_voucher_base.str_to_date = lst_line[8][:5]
                                
                                ins_voucher_base.int_no_of_days = (str_to_date - str_from_date).days
                                    
                                ins_voucher_base.bln_from_to_date = True

                                try :
                                    ins_voucher_base.flt_fare_inv = float(lst_line[36][3:].strip() or lst_line[41][3:].strip())
                                except :
                                    ins_voucher_base.flt_fare_inv = 0.0

                                ins_voucher_base.str_particulars = 'Insurance for ' + str(ins_voucher_base.int_no_of_guests) + ' Passenger(s), From : ' + str_from_date.strftime("%d-%b-%Y") + ' To : ' + str_to_date.strftime("%d-%b-%Y")
                                
                                ins_voucher_base.str_vendor_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE'].get('SUPPLIER','').strip(':')
                                ins_voucher_base.str_hotel_confirm_number = lst_line[31].strip().split('-')[1].upper()  \
                                                             or lst_line[33].strip().split('-')[1].upper()
                                ins_voucher_base.str_voucher_number = lst_line[31].strip().split('-')[1].upper()  \
                                                             or lst_line[33].strip().split('-')[1].upper()


                                ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                    ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                    ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                            
                            elif str_line[10:16] in ('MIS 1A','TUR 1A') :

                                str_service_type =''
                                try :

                                    str_service_type = lst_line[5].split('/')[0].upper().strip()
                                    str_service_type2 = lst_line[5].split('/')[2]
                                    if str_service_type2 in ('MEETING ROOM','EXTRA CHARGES') :
                                        str_service_type = str_service_type2
                                        pass
                                except :
                                    pass

                                if str_service_type == 'LIMO' :

                                    ins_voucher_base = instanceBase.VoucherBase()

                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_voucher_base.str_voucher_service_name = str_service_type
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        ins_voucher_base.int_no_of_guest_inv = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        ins_voucher_base.int_no_of_guest_inv = 1

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_pick_up_date = lst_line[2][-9:-4].strip()
                                    else :
                                        ins_voucher_base.str_pick_up_date = lst_line[2][-5:].strip()

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['PICKUP_LOCATION']) != -1 :
                                        ins_voucher_base.str_pick_up_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['PICKUP_LOCATION'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_pick_up_location = ''

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['PICKUP_TIME']) != -1 :
                                        ins_voucher_base.str_pick_up_time = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['PICKUP_TIME'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_pick_up_time = ''

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['DROP_OFF_LOCATION']) != -1 :    
                                        ins_voucher_base.str_drop_off_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['DROP_OFF_LOCATION'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_drop_off_location = ''

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['DRIVER_PHONE']) != -1 :
                                        ins_voucher_base.str_driver_ph_no  = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['DRIVER_PHONE'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_driver_ph_no  = ''

                                    ins_voucher_base.str_particulars  = "LIMO SERVICE PICKUP FROM %s AT %s DROP OFF AT %s , Driver Ph No : %s "%(ins_voucher_base.str_pick_up_location ,ins_voucher_base.str_pick_up_time ,ins_voucher_base.str_drop_off_location ,ins_voucher_base.str_driver_ph_no)

                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['TOTAL_FARE'])[1].split('/')[0].strip().replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0
                                        pass

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['SUPPLIER_CODE']) != -1 :
                                        ins_voucher_base.str_vendor_code = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_vendor_code = ''
                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['CONF_NO']) != -1 :
                                        ins_voucher_base.str_hotel_confirm_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['CONF_NO'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_hotel_confirm_number = ''

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['VOUCHER_NO']) != -1 :
                                        ins_voucher_base.str_voucher_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['LIMO']['VOUCHER_NO'])[1].split('/')[0].strip()
                                    else :
                                        continue

                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1

                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                elif str_service_type == 'TRANSFER' :

                                    ins_voucher_base = instanceBase.VoucherBase()

                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_voucher_base.str_voucher_service_name = str_service_type
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        ins_voucher_base.int_no_of_guest_inv = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        ins_voucher_base.int_no_of_guest_inv = 1

                                    ins_voucher_base.str_pick_up_date = lst_line[2][-5:]

                                    ins_voucher_base.str_pick_up_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['PICKUP_LOCATION'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_pick_up_time = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['PICKUP_TIME'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_drop_off_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['DROP_OFF_LOCATION'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_driver_ph_no  = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['DRIVER_PHONE'])[1].split('/')[0].strip()

                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['TOTAL_FARE'])[1].split('/')[0].strip().replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0
                                        pass

                                    ins_voucher_base.str_vendor_code = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['CONF_NO'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_voucher_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRANSFER']['VOUCHER_NO'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.str_particulars += " PICKUP : %s ,"%ins_voucher_base.str_pick_up_location
                                    ins_voucher_base.str_particulars += " PICK UP TIME : %s ,"%ins_voucher_base.str_pick_up_time
                                    ins_voucher_base.str_particulars += " DROPOFF : %s ,"%ins_voucher_base.str_drop_off_location
                                    ins_voucher_base.str_particulars += " DRIVER PH : %s ,"%ins_voucher_base.str_driver_ph_no

                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1

                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                elif str_service_type == 'TRAIN BOOKING' :

                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_voucher_base.str_voucher_service_name = str_service_type
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        int_no_of_guest_inv = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_guest_inv = 1

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_from_date = lst_line[2][-9:-4].strip()
                                        ins_voucher_base.str_to_date = lst_line[2][-9:-4].strip()
                                    else :

                                        ins_voucher_base.str_from_date = lst_line[2][-5:].strip()
                                        ins_voucher_base.str_to_date = lst_line[2][-5:].strip()

                                    ins_voucher_base.bln_from_to_date = True

                                    str_dep_loc = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['PICK_UP_LOCATION'])[1].split('/')[0].strip()
                                    str_dep_time = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['PICK_UP_TIME'])[1].split('/')[0].strip()
                                    str_arriv_time = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['DROPOFF_TIME'])[2].split('/')[0].strip()
                                    str_arrival_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['DROP_OFF_LOCATION'])[1].split('/')[0].strip()
                                    str_class = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['CLASS'])[1].split('/')[0].strip()

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['COMPANY']) != -1 :
                                        str_train_company = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['COMPANY'])[1].split('/')[0].strip()
                                    else :
                                        str_train_company = ''


                                    ins_voucher_base.str_particulars = "DEPT : %s, DEP TIME %s , ARR TIME : %s ,ARR LOC : %s , Class : %s, Confirmation Num : %s "%(str_dep_loc,str_dep_time  ,str_arriv_time, str_arrival_location,str_class ,ins_voucher_base.str_voucher_number)

                                    if str_train_company :
                                        ins_voucher_base.str_particulars +=  ", COMPANY : %s "%str_train_company


                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['TOTAL_FARE'])[1].split('/')[0].strip().replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0


                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['SUPPLIER_CODE']) != -1 :
                                        ins_voucher_base.str_vendor_code = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_vendor_code = ''

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['CONF_NO']) != -1 :
                                        ins_voucher_base.str_hotel_confirm_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['CONF_NO'])[1].split('/')[0].strip()
                                    else :
                                        ins_voucher_base.str_hotel_confirm_number = ''
                                    ins_voucher_base.str_remarks = ins_voucher_base.str_hotel_confirm_number

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['VOUCHER_NO']) != -1 :
                                        ins_voucher_base.str_voucher_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['TRAIN BOOKING']['VOUCHER_NO'])[1].split('/')[0].strip()
                                    else :
                                        continue
                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                elif str_service_type in ('VISA SERVICES','VISASERVICES','VISA SERVICE','VISASERVICE') :


                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['VISA']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_voucher_base.str_voucher_service_name = str_service_type
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        int_no_of_applications = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_applications = 1
                                    pass

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-9:-4].strip()
                                    else :
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-5:].strip()

                                    ins_voucher_base.bln_from_to_date = True
                                    ins_voucher_base.str_from_date = ins_voucher_base.str_voucher_issue_date

                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['VISA']['VISA_TYPE']) != -1 :
                                        str_visa_type = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['VISA']['VISA_TYPE'])[1].split('/')[0].strip()
                                    else :
                                        str_visa_type = ''

                                    ins_voucher_base.str_particulars = "VISA TYPE : " +  str_visa_type + ' , Applications : '  + str(int_no_of_applications)


                                    if lst_line[5].find('SERVICE TYPE:') != -1 :
                                        ins_voucher_base.str_voucher_code  = lst_line[5].split('SERVICE TYPE:')[1].split('/')[0].strip()

                                    str_duration = ''    
                                    if lst_line[5].find('DURATION:') != -1 :
                                        str_duration  = lst_line[5].split('DURATION:')[1].split('/')[0].strip()
                                        ins_voucher_base.str_particulars += ", Duration : %s"%str_duration
                                        ins_voucher_base.str_remarks = "Duration : %s"%str_duration
    #                                else :
    #                                    ins_voucher_base.str_voucher_code = ''

                                    ins_voucher_base.str_country = ''
                                    if lst_line[5].find('COUNTRY:') != -1 :
                                        ins_voucher_base.str_country = lst_line[5].split('COUNTRY:')[1].split('/')[0].strip().upper()

                                    if ins_voucher_base.str_voucher_code.find("VISIT VISA") != -1 :
                                        ins_voucher_base.str_voucher_code = 'VVISA'
                                        ins_voucher_base.bln_visa_voucher = True
                                    elif ins_voucher_base.str_voucher_code.find("UMRAH VISA") != -1 :
                                        ins_voucher_base.str_voucher_code = 'UVISA'
                                        ins_voucher_base.bln_visa_voucher = True
                                    elif ins_voucher_base.str_voucher_code.find("TOURIST VISA") != -1 :
                                        ins_voucher_base.str_voucher_code = 'TVISA'
                                        ins_voucher_base.bln_visa_voucher = True
                                    elif ins_voucher_base.str_voucher_code.find("BUSINESS VISA") != -1 :
                                        ins_voucher_base.str_voucher_code = 'BVISA'
                                        ins_voucher_base.bln_visa_voucher = True

                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['VISA']['TOTAL_FARE'])[1].split('/')[0].replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0

                                    ins_voucher_base.str_vendor_code = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['VISA']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['VISA']['CONF_NO'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_voucher_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['VISA']['VOUCHER_NO'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1

                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                elif str_service_type in ('MEET & GREET','MEET AND ASSIST','MEET&GREET','MEETANDASSIST'):

                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_voucher_base.str_voucher_service_name = str_service_type
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True

                                    try :
                                        int_no_of_guests = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_guests = 1

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-9:-4].strip()
                                    else:
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-5:].strip()

                                    str_service_location = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['AIRPORT'])[1].split('/')[0].strip()
                                    try :
                                        str_meet_assist_type = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['SERVICE_TYPE'])[1].split('/')[0].strip()
                                    except :
                                        str_meet_assist_type = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['SERVICE_TYPE'])[1].split('/')[0].strip()
                                        str_meet_assist_type += lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['TYPE'])[1].split('/')[0].strip()

                                    str_time_of_service = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['DURATION'])[1].split('/')[0].strip()


                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['TOTAL_FARE'])[1].split('/')[0].replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0

                                    ins_voucher_base.str_vendor_code = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                    if lst_line[5].find(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['CONF_NO']) != -1 :
                                        ins_voucher_base.str_hotel_confirm_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['CONF_NO'])[1].split('/')[0].strip()

                                    str_type = ''    
                                    if lst_line[5].find("TYPE:") != -1 :
                                        str_type = lst_line[5].split("TYPE:")[1].split('/')[0].strip()

                                    ins_voucher_base.str_voucher_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['VOUCHER_NO'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    try :
                                        ins_voucher_base.str_phone = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['MEETANDASSIST']['PHONE'])[1].split('/')[0].strip()
                                    except :
                                        ins_voucher_base.str_phone = ''

                                    ins_voucher_base.str_particulars = "MEET AND GREET SERVICE AT %s on %s"%(str_service_location,ins_voucher_base.str_voucher_issue_date  )

                                    if str_type :
                                        ins_voucher_base.str_particulars +=  ", Type :%s "%str_type

                                    if lst_line[5].find("PH:") != -1 :
                                        ins_voucher_base.str_remarks = 'Ph : ' + lst_line[5].split("PH:")[1].split('/')[0].strip()

                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)
                                    pass

                                elif str_service_type in ('INSURANCE - AXA','INSURANCE','INSURANCE-AXA') :


                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_voucher_base.str_voucher_service_name = str_service_type
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        int_no_of_guests = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_guests = 1


                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-9:-4].strip()
                                    else:
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-5:].strip()
                                    ins_voucher_base.bln_from_to_date = True

                                    str_country = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['COUNTRY_CODE'])[1].split('/')[0].strip()
                                    try :
                                        str_insurance_type = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['INSURANCE_TYPE'])[1].split('/')[0].strip()
                                    except :
                                        str_insurance_type = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['INSURANCE_TYPE'].replace(' ',''))[1].split('/')[0].strip()
                                    str_duration = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['INSURANCE_DURATION'])[1].split('/')[0].strip().upper()

                                    try :
                                        ins_voucher_base.int_no_of_days = 0
                                        if str_duration.find('MONTH') != -1 :
                                            ins_voucher_base.int_no_of_days = int(str_duration.split('M')[0].strip()) * 30
                                        if str_duration.find('DAY') != -1  :
                                            ins_voucher_base.int_no_of_days = int(str_duration.split('D')[0].strip())
                                        if str_duration.find('WEEK') != -1  :
                                            ins_voucher_base.int_no_of_days = int(str_duration.split('D')[0].strip()) * 7
                                    except :
                                        pass

                                    ins_voucher_base.str_particulars += "COUNTRY :%s"%str_country
                                    ins_voucher_base.str_particulars += ", SERVICE TYPE :%s"%str_insurance_type



    #                                ins_voucher_base.str_particulars += ", DURATION :%s"%str_duration

                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['TOTAL_FARE'])[1].split('/')[0].replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0

                                    ins_voucher_base.str_vendor_code = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['SUPPLIER_CODE'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['CONF_NO'])[1].split('/')[0].strip()
                                    ins_voucher_base.str_voucher_number = lst_line[5].split(ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_NO'])[1].split('/')[0].strip()

                                    if ins_general_methods.ins_capture_base.dct_voucher_conf_data['AGENCY']['NAME'] == 'NIR:' :
                                        ins_voucher_base.str_particulars += ", POLICY NUM :%s"%ins_voucher_base.str_hotel_confirm_number


                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)


                                elif str_service_type == 'YAS WATER WORLD' :


                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = 'MISC'#ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_voucher_base.str_voucher_service_name = str_service_type
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        int_no_of_guests = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_guests = 1

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-9:-4].strip()
                                    else:
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-5:].strip()

                                    ins_voucher_base.str_particulars = 'City : '+lst_line[4] + ' ' +lst_line[5].split('SI:')[1].split('/')[0].split(';')[0].strip()


                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split('RQ:')[1].split('/')[0].replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0

                                    ins_voucher_base.str_vendor_code = lst_line[5].split('VN:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split('VC:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_voucher_number = lst_line[5].split('VC:')[1].split('/')[0].strip()

                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                elif str_service_type == 'TOURS' :


                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.str_voucher_code = 'MISC'#ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        int_no_of_guests = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_guests = 1

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-9:-4].strip()
                                    else:
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-5:].strip()

                                    ins_voucher_base.str_particulars = 'Service : TOURS'

                                    if  lst_line[5].find("SI:") != -1 :
                                        ins_voucher_base.str_particulars += ', ' + lst_line[5].split('SI:')[1].split('/')[0].split(';')[0].strip()
                                    ins_voucher_base.str_particulars += ', Department : '+lst_line[5].split('DEPT:')[1].split('/')[0].split(';')[0].strip()
                                    ins_voucher_base.str_particulars += ', Type : '+lst_line[5].split('TYPE:')[1].split('/')[0].split(';')[0].strip()


                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split('RATE:')[1].split('/')[0].replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0

                                    ins_voucher_base.str_vendor_code = lst_line[5].split('VN:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split('VC:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_voucher_number = lst_line[5].split('VC:')[1].split('/')[0].strip()

                                    ins_voucher_base.str_remarks = 'Confirmation Num : ' +lst_line[5].split('CF:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                elif str_service_type == 'EXTRA CHARGES' :


                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.bln_from_to_date = True
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    ins_voucher_base.str_voucher_code = 'MISC'#ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_SERVICE_CODE'].strip(':')

                                    try :
                                        int_no_of_guests = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_guests = 1

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-9:-4].strip()
                                    else:
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-5:].strip()

                                    ins_voucher_base.str_from_date = ins_voucher_base.str_voucher_issue_date
                                    ins_voucher_base.str_to_date = lst_line[5][:5]

                                    ins_voucher_base.str_particulars = 'Service : ' + lst_line[5].split('ST:')[1].split('/')[0].split(';')[0].strip()
                                    ins_voucher_base.str_particulars += ', Name : ' + lst_line[5].split('NAME:')[1].split('/')[0].split(';')[0].strip()
                                    ins_voucher_base.str_particulars += ', Code : '+lst_line[5].split('CODE:')[1].split('/')[0].split(';')[0].strip()



                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split('QUOTED RATE:')[1].split('/')[0].split(';')[0].replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0

                                    ins_voucher_base.str_vendor_code = lst_line[5].split('VN:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split('VC:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_voucher_number = lst_line[5].split('VC:')[1].split('/')[0].strip()

                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)

                                elif str_service_type == 'MEETING ROOM' :


                                    ins_voucher_base = instanceBase.VoucherBase()
                                    ins_voucher_base.int_voucher_identifier = int_voucher_identifier
                                    ins_voucher_base.int_pax_identifier = int_pax_identifier
                                    ins_voucher_base.str_voucher_type = 'O'
                                    ins_voucher_base.str_voucher_category = str_service_type
                                    ins_voucher_base.bln_from_to_date = True
                                    ins_voucher_base.str_voucher_code = 'MISC'#ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_SERVICE_CODE'].strip(':')
                                    ins_capture_ticket_data.bln_voucher_present_in_file = True
                                    try :
                                        int_no_of_guests = int(lst_line[2][0:5].strip().strip('HK'))
                                    except :
                                        int_no_of_guests = 1

                                    if len(lst_line[2].split(' ')[1].strip()) > 6: 
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-9:-4].strip()
                                    else:
                                        ins_voucher_base.str_voucher_issue_date = lst_line[2][-5:].strip()
                                    ins_voucher_base.str_from_date = ins_voucher_base.str_voucher_issue_date

                                    ins_voucher_base.str_to_date = lst_line[5][:5]

                                    ins_voucher_base.str_particulars = 'MEETING in '  + lst_line[4] + ' at ' + lst_line[5].split('NAME:')[1].split('/')[0].split(';')[0].strip()

                                    try :
                                        ins_voucher_base.flt_fare_inv = float(lst_line[5].split('QUOTED RATE:')[1].split('/')[0].split(';')[0].replace(self.str_defult_currency_code,''))
                                    except :
                                        ins_voucher_base.flt_fare_inv = 0.0

                                    ins_voucher_base.str_vendor_code = lst_line[5].split('VN:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_hotel_confirm_number = lst_line[5].split('VC:')[1].split('/')[0].strip()
                                    ins_voucher_base.str_voucher_number = lst_line[5].split('VC:')[1].split('/')[0].strip()

                                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                                    ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)
                        except :
                            raise
                            pass
                    
                    if ins_capture_ticket_data.bln_xo_capturing and ins_general_methods.ins_auto_inv.bln_xo_capture :  # refer 25579
                        if ins_capture_ticket_data.lst_sector_details:
                            continue

                        try:
                            ins_capture_ticket_data.str_return_date = str_line[80:85]
                        except:
                            pass
                        pass

                        lst_split_line = str_line.split(";")

                        # // Stopover Permitted
                        if lst_split_line[1][3] == 'O':
                            ins_capture_ticket_data.bln_stopover_permitted = True

                            ins_capture_ticket_data.lst_stop_over_airports.append(lst_split_line[3])
                        elif lst_split_line[1][3] == 'X':
                            ins_capture_ticket_data.bln_stopover_permitted = False
                            pass
                        # // Orgin Airport Code
                        ins_capture_ticket_data.str_orgin_airport_code = lst_split_line[1][4:]
                        if ins_capture_ticket_data.int_number_of_segments == 0:
                            ins_capture_ticket_data.str_start_port_code = ins_capture_ticket_data.str_orgin_airport_code
                            pass

                        ins_capture_ticket_data.str_dest_code = lst_split_line[3]
                        # //Adding an empty sector if the not travelled by air
                        if lst_split_line[5].strip() in ['VOID','COACH-RESERVED','COACH-UNRESERVED']:
                            ins_capture_ticket_data.lst_u_section_sector.append('')
                        ins_capture_ticket_data.lst_u_section_sector.append(ins_capture_ticket_data.str_dest_code)
                        lst_splt_air_details = lst_split_line[5].split()
                        ins_capture_ticket_data.str_airline_code = lst_splt_air_details[0]

                        if len(lst_splt_air_details) == 5:
                            ins_capture_ticket_data.str_flight_number = lst_splt_air_details[1]
                            ins_capture_ticket_data.str_class_of_service = lst_splt_air_details[2]
                            ins_capture_ticket_data.str_class_of_booking = lst_splt_air_details[3]

                            ### ins_capture_ticket_data.str_class_of_booking


                            ins_capture_ticket_data.str_departure_date = lst_splt_air_details[4][:5]
                            ins_capture_ticket_data.str_departure_time = lst_splt_air_details[4][5:9]
                            ins_capture_ticket_data.str_arrival_time = lst_splt_air_details[4][9:13]
                            ins_capture_ticket_data.str_arrival_date = lst_splt_air_details[4][13:]
                            pass
                        elif len(lst_splt_air_details) ==7:
                            ins_capture_ticket_data.str_flight_number = lst_splt_air_details[1]
                            ins_capture_ticket_data.str_class_of_service = lst_splt_air_details[2]
                            ins_capture_ticket_data.str_class_of_booking = lst_splt_air_details[3]

    #                        ins_capture_ticket_data.str_airline_no = lst_splt_air_details[4][-3:]
    #                        ins_capture_ticket_data.str_departure_date = lst_splt_air_details[4][5:]

                            ins_capture_ticket_data.str_departure_date = lst_splt_air_details[4][:5]
                            ins_capture_ticket_data.str_departure_time = lst_splt_air_details[4][5:]
                            if ins_capture_ticket_data.int_number_of_segments == 0:
                                ins_capture_ticket_data.str_first_departure_date = ins_capture_ticket_data.str_departure_date

    #                        if not ins_capture_ticket_data.str_departure_date:
                            ins_capture_ticket_data.str_arrival_time = lst_splt_air_details[5]
                            ins_capture_ticket_data.str_arrival_date = lst_splt_air_details[6]
                            pass
                        else:
                            if len(lst_splt_air_details) == 1 and lst_splt_air_details[0] == 'VOID':
                                ins_capture_ticket_data.str_departure_date = None
                                ins_capture_ticket_data.str_departure_time = None
                                ins_capture_ticket_data.str_arrival_time = None
                                ins_capture_ticket_data.str_arrival_date = None
                                pass
                            if len(lst_splt_air_details) >= 2 and lst_splt_air_details[1] == 'OPEN':
                                ins_capture_ticket_data.str_open_segment_indicator = lst_splt_air_details[1]
                                ins_capture_ticket_data.str_departure_date = lst_split_line[6].strip() or None
                                ins_capture_ticket_data.str_departure_time = None
                                ins_capture_ticket_data.str_arrival_time = None
                                ins_capture_ticket_data.str_arrival_date = None
                                pass

                        try:
                            ins_capture_ticket_data.int_mileage = int(lst_split_line[19].strip() or 0) # milage per sector
                        except:
                            ins_capture_ticket_data.int_mileage = 0

                        dbl_sector_wise_fare = 0 #@todo

                        ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments + 1

                        str_issue_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_file_creation_date)
                        ins_capture_ticket_data.str_arrival_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_arrival_date,str_issue_date)
                        ins_capture_ticket_data.str_departure_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_departure_date,str_issue_date)

                        ins_capture_ticket_data.lst_new_u_section_sector.append([ins_capture_ticket_data.str_orgin_airport_code,
                                                                                ins_capture_ticket_data.str_dest_code,
                                                                                ins_capture_ticket_data.str_airline_code,
                                                                                ins_capture_ticket_data.str_airline_no,
                                                                                ins_capture_ticket_data.str_flight_number,
                                                                                ins_capture_ticket_data.str_class_of_service,
                                                                                ins_capture_ticket_data.str_class_of_booking,
                                                                                ins_capture_ticket_data.str_arrival_date,
                                                                                ins_capture_ticket_data.str_departure_date,
                                                                                ins_capture_ticket_data.bln_stopover_permitted,
                                                                                ins_capture_ticket_data.int_mileage,
                                                                                dbl_sector_wise_fare,
                                                                                ins_capture_ticket_data.str_arrival_time,
                                                                                ins_capture_ticket_data.str_departure_time,
                                                                                False])
                        ins_capture_ticket_data.lst_booking_sector_details.append([ins_capture_ticket_data.str_orgin_airport_code,
                                                                            ins_capture_ticket_data.str_dest_code,
                                                                            ins_capture_ticket_data.str_airline_code,
                                                                            ins_capture_ticket_data.str_airline_no,
                                                                            ins_capture_ticket_data.str_flight_number,
                                                                            ins_capture_ticket_data.str_class_of_service,
                                                                            ins_capture_ticket_data.str_class_of_booking,
                                                                            ins_capture_ticket_data.str_arrival_date,
                                                                            ins_capture_ticket_data.str_departure_date,
                                                                            ins_capture_ticket_data.str_departure_time,
                                                                            ins_capture_ticket_data.bln_stopover_permitted,
                                                                            ins_capture_ticket_data.int_mileage,
                                                                            dbl_sector_wise_fare])


                        pass

                elif str_line[:3] == "IVD": ## refer 35080
                    if  ins_general_methods.ins_auto_inv.bln_voucher_capture: 
                        try :
                            str_line = str_line.strip().strip(';')
                            lst_line = str_line.split(';')
                            
                            ins_voucher_base = instanceBase.VoucherBase()

                            try :
                                int_pax_identifier = int(lst_line[-1].strip('P').strip())
                            except :
                                try :
                                    int_pax_identifier = int(str_line.split('/')[-1].split('-')[0].strip().strip('P').strip())
                                except :
                                    int_pax_identifier = 1
                            
                            ins_voucher_base.int_voucher_identifier = int(lst_line[1][:3])
                            ins_voucher_base.int_pax_identifier = int_pax_identifier
                            ins_voucher_base.str_voucher_type = 'O'
                            str_service_type = ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['VOUCHER_SERVICE_CODE'].strip(':')
                            ins_voucher_base.str_voucher_category = str_service_type
                            ins_voucher_base.str_voucher_code = str_service_type
                            ins_voucher_base.str_voucher_service_name = str_service_type
                            ins_capture_ticket_data.bln_voucher_present_in_file = True

                            try:
                                ins_voucher_base.int_no_of_guests = int(lst_line[3].strip())
                            except:
                                ins_voucher_base.int_no_of_guests = 1

                            str_from_date = datetime.datetime.strptime((lst_line[6]), "%d%b%Y").date()
                            str_to_date = datetime.datetime.strptime((lst_line[7]), "%d%b%Y").date()

                            ins_voucher_base.str_from_date = lst_line[6][:5]
                            ins_voucher_base.str_to_date = lst_line[7][:5]

                            ins_voucher_base.int_no_of_days = (str_to_date - str_from_date).days

                            ins_voucher_base.bln_from_to_date = True

                            try :
                                ins_voucher_base.flt_fare_inv = float(lst_line[35][3:].strip() or lst_line[34][3:].strip())
                            except :
                                ins_voucher_base.flt_fare_inv = 0.0

                            ins_voucher_base.str_particulars = 'Insurance for ' + str(ins_voucher_base.int_no_of_guests) + ' Passenger(s), From : ' + str_from_date.strftime("%d-%b-%Y") + ' To : ' + str_to_date.strftime("%d-%b-%Y")

                            ins_voucher_base.str_vendor_code = ins_general_methods.ins_capture_base.dct_voucher_conf_data['INSURANCE']['SUPPLIER'].strip(':')
                            ins_voucher_base.str_hotel_confirm_number = lst_line[30].strip().split('-')[1].upper() \
                                                         or lst_line[32].strip().split('-')[1].upper()
                            ins_voucher_base.str_voucher_number = lst_line[30].strip().split('-')[1].upper()  \
                                                         or lst_line[32].strip().split('-')[1].upper()


                            ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_voucher_number
                            ins_voucher_base.int_voucher_count = len(ins_capture_ticket_data.lst_ins_voucher) + 1
                            if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                                ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                                ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)
                        except:
                            pass
                        
                elif str_line.split('-')[0] == "T":
                    bln_has_t_section = True
                    lst_split_line = str_line.replace('\r','').replace('\n','').split("-")
                    
                    if not len(lst_split_line) > 1 or  lst_split_line[1] == '':
                        continue
                        

                    str_ticketing_airline_character_code = ''
                    str_conjection_ticket_number = ""
                    # // Ticket Type
                    chr_ticket_type = lst_split_line[1][1].strip()
                    # // Ticketing Airline Numeric Code
                    str_ticketing_airline_numeric_code = lst_split_line[1][1:].strip()
                    if not ins_capture_ticket_data.str_ticketing_airline_numeric_code :
                        ins_capture_ticket_data.str_ticketing_airline_numeric_code = str_ticketing_airline_numeric_code
                    try:
                        # // Ticket Number
                        str_ticket_number = lst_split_line[2][:10].strip()
                        if str_ticket_number == '' and not ins_capture_ticket_data.bln_xo_capturing:
                            raise
                    except:
                        raise

                    # // Conjection Ticket Number
                    if len(lst_split_line) > 3:

                        try:
                            str_tmp_conj_tkt1 = str_ticket_number
                            str_tmp_conj_tkt2 = str_tmp_conj_tkt1[:-2] + lst_split_line[3][:2]
                            if int(str_tmp_conj_tkt1) >= int(str_tmp_conj_tkt2):
                                str_conjection_ticket_number = str(int(str_tmp_conj_tkt1) + 1)[:-2] + lst_split_line[3][:2]
                            else:
                                str_conjection_ticket_number = str_tmp_conj_tkt1[:-2] + lst_split_line[3][:2]
                                pass
                            pass
                        except:
                            str_conjection_ticket_number = str_ticket_number[:-2] + lst_split_line[3][:2]
                            pass
                        pass


                    pass
                    flt_service_charge = 0
                    ins_capture_ticket_data.lst_ticket_detls.append([str_ticket_number,
                                                                     str_conjection_ticket_number,
                                                                     chr_ticket_type,
                                                                     str_ticketing_airline_character_code,
                                                                     str_ticketing_airline_numeric_code,
                                                                     ins_capture_ticket_data.str_pax_name,
                                                                     ins_capture_ticket_data.str_pax_type,
                                                                     ins_capture_ticket_data.flt_service_charge,
                                                                     ins_capture_ticket_data.str_isection_mobile,
                                                                     ins_capture_ticket_data.str_isection_email,
                                                                     ins_capture_ticket_data.int_pax_item_number
                                                                     ])
                    pass
                elif str_line.split('-')[0] == "R":
                    
                    lst_refund_split_line =[]
                    ## Page No : 266
                    lst_split_line = str_line.split(";")
                    dat_refund_date = lst_split_line[1].replace('\r','').replace('\n','').strip()
                    dat_refund_date = datetime.datetime.strptime(dat_refund_date,"%d%b%y")
                    ins_capture_ticket_data.dat_ticket_refund =  datetime.datetime.strftime(dat_refund_date,"%d/%m/%Y")
                    ins_capture_ticket_data.bln_refund = True
                    
                    ins_capture_emd_ticket_data.dat_ticket_refund = ins_capture_ticket_data.dat_ticket_refund
                    ins_capture_emd_ticket_data.bln_refund = True
                    # refer 22375
                    lst_refund_split_line = lst_split_line[0].replace('\r','').replace('\n','').split("-")
                    str_ref_conjection_ticket_number = ""
                    str_ref_ticket_number = lst_refund_split_line[2][:10].strip()
                    chr_ref_ticket_type = lst_refund_split_line[1][0].strip()
                    str_ref_ticketing_airline_character_code = ''
                    str_ref_ticketing_airline_numeric_code = lst_refund_split_line[1][0:].strip()
                    
                    if not ins_capture_ticket_data.str_ticketing_airline_numeric_code :
                        ins_capture_ticket_data.str_ticketing_airline_numeric_code = str_ref_ticketing_airline_numeric_code
                    flt_ref_service_charge = 0
                    ins_capture_ticket_data.lst_refund_ticket_detls.append([str_ref_ticket_number,
                                                                     str_ref_conjection_ticket_number,
                                                                     chr_ref_ticket_type,
                                                                     str_ref_ticketing_airline_character_code,
                                                                     str_ref_ticketing_airline_numeric_code,
                                                                     ins_capture_ticket_data.str_pax_name,
                                                                     ins_capture_ticket_data.str_pax_type,
                                                                     flt_ref_service_charge,
                                                                     '',
                                                                     '',
                                                                     ins_capture_ticket_data.int_pax_item_number])
                    
                    

                elif str_line[:3] == "KFT":
                  if ins_capture_ticket_data.lst_tax == [] and ins_capture_ticket_data.flt_tax == 0.00:
                    lst_split_line = str_line[5:].replace('\r','').replace('\n','').split(";")#42414
                    for str_tax in lst_split_line:

                        if str_tax:  #len(str_tax) == 18 or len(str_tax) == 16:   #42414

                            # Tax of original issue need not to be considered.
                            if str_tax and str_tax[0]== 'O':
                                continue

                            str_tax_amount = str_tax[4:10].strip()
                            str_tax_code = str_tax[13:15]  #42414
                            str_nature_code = ''  #42414

                            try:
                                ## Check for valid Tax Amount
                                float(str_tax_amount)
                            except:
                                str_tax_amount = '0'

                            ins_capture_ticket_data.lst_tax.append((str_tax_amount,
                                                                     str_tax_code,
                                                                     str_nature_code))
                            ins_capture_ticket_data.flt_tax = ins_capture_ticket_data.flt_tax + float(str_tax_amount)
                            pass
                        pass
                    pass
                elif str_line[:3] == "KST":
                    if ins_capture_ticket_data.lst_tax == [] and ins_capture_ticket_data.flt_tax == 0.00:
                        lst_split_line = str_line[5:].replace('\r','').replace('\n','').split(";")  #42414

                        for str_tax in lst_split_line:

                            if str_tax:  #len(str_tax) == 18 or len(str_tax) == 16:   #42414

                                # Tax of original issue need not to be considered.
                                if str_tax and str_tax[0]== 'O':
                                    continue

                                str_tax_amount = str_tax[4:10].strip()
                                str_tax_code = str_tax[13:15]  #42414
                                str_nature_code = ''    #42414

                                try:
                                    ## Check for valid Tax Amount
                                    float(str_tax_amount)
                                except:
                                    str_tax_amount = '0'

                                ins_capture_ticket_data.lst_tax.append((str_tax_amount,
                                                                         str_tax_code,
                                                                         str_nature_code))
                                ins_capture_ticket_data.flt_tax = ins_capture_ticket_data.flt_tax + float(str_tax_amount)
                                pass
                            pass
                        pass
                    pass
                elif str_line[:3] == "TAX":
                    if ins_capture_ticket_data.lst_tax == [] and ins_capture_ticket_data.flt_tax == 0.00:
                        str_new_line = str_line[4:].strip()

                        lst_split_line = str_new_line.replace('\r','').replace('\n','').split(";")
                        for str_tax in lst_split_line:
                            str_tax_amount = str_tax[3:12].strip()
                            str_tax_code = str_tax[12:15].strip()
                            str_nature_code = ''

                            if str_tax and str_tax[:3]== 'PD ':
                               continue

                            try:
                                ## Check for valid Tax Amount
                                float(str_tax_amount)
                            except:
                                str_tax_amount = '0'

                            ins_capture_ticket_data.lst_tax.append((str_tax_amount,
                                                                     str_tax_code,
                                                                     str_nature_code))
                            ins_capture_ticket_data.flt_tax = ins_capture_ticket_data.flt_tax + float(str_tax_amount)
                            pass
                        pass
                    pass
                
                elif str_line[:3] == "ATF":
                    str_new_line = str_line[4:].strip()
                    lst_split_line = str_new_line.replace('\r','').replace('\n','').split(";")
                    if len(lst_split_line) > 4 :#Bugno25328
                        str_tax_amount = lst_split_line[4][3:12].strip() or 0
                        str_tax_code = 'SURCHRG'
                        str_nature_code = ''
                        ins_capture_ticket_data.lst_tax.append((str_tax_amount,
                                                                     str_tax_code,
                                                                     str_nature_code))
                        ins_capture_ticket_data.flt_tax = ins_capture_ticket_data.flt_tax + float(str_tax_amount)
                        pass
                
                elif str_line[:3] == "KRF":  # REFER 30830
                    if ins_capture_ticket_data.lst_tax == [] and ins_capture_ticket_data.flt_tax == 0.00:
                        str_new_line = str_line[4:].strip()
                        lst_split_line = str_new_line.replace('\r','').replace('\n','').split(";")

                        for str_tax in lst_split_line:
                            if str_tax:      #42414
                                str_tax_amount = str_tax[4:12].strip()
                                str_tax_code = str_tax[12:15].strip()
                                str_nature_code = ''
                                ins_capture_ticket_data.lst_tax.append((str_tax_amount,
                                                                         str_tax_code,
                                                                     str_nature_code))
                elif str_line[:3] == "KN-":
                    try:
                        # // In case of Deal fare Found after the 13th semicolon then published_fare is taken from there else old published_fare is taken
                        # // Refer #9346 Code modified as instructed by Mohan Das
                        try:
                            lst_line = str_line.split(';')
                            if not ins_capture_ticket_data.flt_published_fare and len(lst_line) > 13 and lst_line[13][:3] == self.str_defult_currency_code:
                                str_published_fare = lst_line[13][3:14].strip()
                                ins_capture_ticket_data.flt_published_fare = float(str_published_fare)
                                pass
                        except:
                            ins_capture_ticket_data.flt_published_fare = 0.0
                            pass

                        if str_line[4:7] == self.str_defult_currency_code:
                            str_market_fare = str_line[7:18].strip()
                            ins_capture_ticket_data.flt_market_fare = float(str_market_fare)
                        elif str_line[19:22] == self.str_defult_currency_code:
                            str_market_fare = str_line[22:33].strip()
                            ins_capture_ticket_data.flt_market_fare = float(str_market_fare)
                            pass
                    except:
                        ins_capture_ticket_data.flt_market_fare = 0.00
                        pass
                    pass
                elif str_line[:2] == "FT":
                    ins_capture_ticket_data.str_tour_code = str_line[2:].split(";")[0].split("\r\n")[0].split("*")[-1].strip()[:30]
                elif str_line[:3] == "RIS":
                    try:
                        ins_capture_ticket_data.flt_service_charge = float(str_line.split(";")[1].strip())
                    except:
                        ins_capture_ticket_data.flt_service_charge = 0.00
                        pass
                elif str_line[:3] == "TFD":
                    # // Here we will add the service fee amount and adjustment amount. The sum will be treated as Service Fee.
                    # // #7355
                    if len(str_line.split(';')) > 6 and not bln_svf_identifier:
                        try:
                            ins_capture_ticket_data.flt_service_charge = float(str_line.split(";")[4].strip()[3:]) + float(str_line.split(";")[5].strip() or 0.0)
                            if ins_capture_ticket_data.lst_ticket_detls:
                                ins_capture_ticket_data.lst_ticket_detls[-1][7] = ins_capture_ticket_data.flt_service_charge
                        except:
                            ins_capture_ticket_data.flt_service_charge = 0.00
                            pass

                elif str_line[:3] == "TFT": 
                    lst_tft_line = str_line.split(';')
                    if len(lst_tft_line) > 1:   #referBugno25328
                        lst_tft = lst_tft_line[1].split('+')
                        if len(lst_tft) > 1:
                            str_tst_airline = lst_tft[1][5:].strip()
                    
                            if bln_tst_count and lst_tft_line[0][3:4] and (int(lst_tft_line[0][3:4]) == ins_capture_ticket_data.int_tst_count) and \
                                (ins_capture_ticket_data.str_airline_code == str_tst_airline):
                                bln_svf_identifier = True    
                                pass
                            elif bln_tst_count and not lst_tft_line[0][3:4] and (ins_capture_ticket_data.str_airline_code == str_tst_airline):
                                bln_svf_identifier = True    
                                pass
                            elif not bln_tst_count and (ins_capture_ticket_data.str_airline_code == str_tst_airline):
                                bln_svf_identifier = True    
                                pass
                elif str_line[:3] == "CM-":
                    try:
                        if ins_capture_ticket_data.flt_std_commn_percentage_inv == 0:
                            ins_capture_ticket_data.flt_std_commn_percentage_inv = float(str_line.split("/")[1].strip())
                    except:
                        pass
                elif str_line[:3] == "FM*":
                    try:
                        if ins_capture_ticket_data.flt_std_commn_percentage_inv == 0:
                            ins_capture_ticket_data.flt_std_commn_percentage_inv = float(str_line.strip().split(";")[0].split("*")[-1].strip())
                    except:
                        try:
                            if str_line.strip().split(";")[0].split("*")[-1][-1] == 'A':#ref 19763 
                                ins_capture_ticket_data.flt_standard_commission_captured = float(str_line.strip().split(";")[0].split("*")[-1][:-1].strip())
                            else:
                                if ins_capture_ticket_data.flt_std_commn_percentage_inv == 0:
                                    ins_capture_ticket_data.flt_std_commn_percentage_inv = float(str_line.strip().split(";")[0].split("*")[-1][:-1].strip())
                        except:
                            pass
                        pass

                elif str_line[:2] == "FM":
                # To capture standard commn in the form FM7.00P
                    str_line = str_line[2:]
                    str_std_commn = ''

                    for chr_temp in str_line :
                        if chr_temp.isdigit() or chr_temp == '.':
                            str_std_commn += chr_temp
                        else:
                            break
                    try:
                        flt_std_commn = float(str_std_commn)
                        if flt_std_commn and ins_capture_ticket_data.flt_std_commn_percentage_inv == 0:
                            ins_capture_ticket_data.flt_std_commn_percentage_inv = flt_std_commn
                        if flt_std_commn and ins_capture_ticket_data.flt_std_commn_percentage_rfd == 0 and ins_capture_ticket_data.bln_refund == True :
                            ins_capture_ticket_data.flt_std_commn_percentage_rfd = flt_std_commn
                    except:
                        pass

                elif str_line[:3] == "RFD":
                    ## Page No : 187
                    lst_split_line = str_line.split(";")
#                    str_ticket_issue = lst_split_line[1]
                    ins_capture_ticket_data.flt_fare_paid = float(lst_split_line[3][3:] or 0)
                    ins_capture_ticket_data.flt_fare_used = float(lst_split_line[4] or 0)
                    ins_capture_ticket_data.flt_fare_refund = float(lst_split_line[5] or 0)
                    ins_capture_ticket_data.flt_cancellation_fee = float(lst_split_line[8] or 0)
                    ins_capture_ticket_data.flt_tax_refund = float(lst_split_line[11][2:] or 0)
                    ins_capture_ticket_data.flt_total_refund = float(lst_split_line[12] or 0)
                    if 'NF' in lst_split_line and ins_general_methods.ins_capture_base.str_consider_nf_key_word_in_amdeus.upper() == 'TRUE':
                        int_index = lst_split_line.index('NF')
                        ins_capture_ticket_data.flt_fare_refund = float(lst_split_line[int_index+1] or 0)
                    
                    ins_capture_emd_ticket_data.flt_fare_paid = float(lst_split_line[3][3:] or 0)
                    ins_capture_emd_ticket_data.flt_fare_used = float(lst_split_line[4] or 0)
                    ins_capture_emd_ticket_data.flt_fare_refund = float(lst_split_line[5] or 0)
                    ins_capture_emd_ticket_data.flt_cancellation_fee = float(lst_split_line[8] or 0)
                    ins_capture_emd_ticket_data.flt_tax_refund = float(lst_split_line[11][2:] or 0)
                    ins_capture_emd_ticket_data.flt_total_refund = float(lst_split_line[12] or 0)
                    if 'NF' in lst_split_line and ins_general_methods.ins_capture_base.str_consider_nf_key_word_in_amdeus.upper() == 'TRUE':
                        int_index = lst_split_line.index('NF')
                        ins_capture_emd_ticket_data.flt_fare_refund = float(lst_split_line[int_index+1] or 0)

                elif str_line[:3] == "FOI":
                    pass
                
                #38905
                elif str_line[:8].strip() == 'SSR DOCS':
                    try:
                        lst_line_split =  str_line.split(";")
                        str_pax_data = ''
                        str_pax_identifier = ''
                        if len(lst_line_split)>2:
                            str_pax_data = lst_line_split[2]
                            if str_pax_data[0].strip() == 'P':
                                str_pax_identifier = str_pax_data.strip()[:2].lstrip('0').replace('P','')
                            else:
                                raise
                        elif len(lst_line_split)>1:
                            if lst_line_split[1].strip()[0] != 'P':
                                raise
                        if not str_pax_identifier:
                            str_pax_identifier = str_line.split(";")[1][:2].lstrip('0').replace('P','')
                            
                        if str_pax_identifier not in ins_capture_ticket_data.dct_ssr_docs_data:
                            ins_capture_ticket_data.dct_ssr_docs_data[str_pax_identifier] = []
                                            
                        if str_pax_identifier and self.int_pax_identifier:
                            lst_passenger_details = []
                            str_nationality = ''
                            str_passport_number = ''
                            lst_passenger_details = str_line.split('/')
                            if lst_passenger_details and lst_passenger_details[1] == 'P': #passport

                                try:
                                    str_passport_number = lst_passenger_details[3]
                                except:
                                    str_passport_number = ''
                                
                                try:
                                    str_nationality = lst_passenger_details[4]
                                except:
                                    str_nationality = ''

                                if str_passport_number:
                                    ins_capture_ticket_data.str_passport_no = str_passport_number
                                    ins_capture_emd_ticket_data.str_passport_no = str_passport_number
                                    
                                if str_nationality:
                                    ins_capture_ticket_data.str_nationality = str_nationality
                                    ins_capture_emd_ticket_data.str_nationality = str_nationality
                                
                                ins_capture_ticket_data.dct_ssr_docs_data[str_pax_identifier] = [str_passport_number,str_nationality]
                                
                                                
                    except Exception as msg:
                        pass
                
                
                elif str_line[:2] == "FO":
                    try:
                        #43797
                        if ins_capture_ticket_data.bln_refund:
                                ins_capture_ticket_data.str_original_issue = str_line[5:15]
                                ins_capture_ticket_data.str_base_ticket_number = str_line[5:15]
                        else:
                            ins_capture_ticket_data.str_original_issue = str_line.split('/')[-1].split('-')[1][:10].strip()
                            # Base ticket number (first issue)
                            ins_capture_ticket_data.str_base_ticket_number = str_line[6:16]

                        # // If the ticket number and original ticket number are same in a file-
                        # // It will set the original issue as blank
                        # // refs -#10171
                        if ins_capture_ticket_data.lst_ticket_detls[0][0] == ins_capture_ticket_data.str_original_issue:
                            ins_capture_ticket_data.str_original_issue = ''


                    except:
                        try:
                            ins_capture_ticket_data.str_original_issue = str_line[6:16]
                            ins_capture_ticket_data.str_base_ticket_number = str_line[6:16]

                            if ins_capture_ticket_data.lst_ticket_detls[0][0] == ins_capture_ticket_data.str_original_issue:
                                ins_capture_ticket_data.str_original_issue = ''
                        except:
                            ins_capture_ticket_data.str_original_issue = ''
                            ins_capture_ticket_data.str_base_ticket_number = ''

                elif str_line[:2] == "FH":
                    try:
                        ins_capture_ticket_data.str_hand_ticket_number = str_line[6:16]
                    except:
                        ins_capture_ticket_data.str_hand_ticket_number = ''
                        
                #refer #41722 
                elif str_line[:2] == "FV":    ## Airline character code is taken from this line if it is not obtained from "A-"
                    if not ins_capture_ticket_data.str_ticketing_airline_character_code:
                        if str_line.find(';') and len(str_line.split(';')[0]) == 4:
                            ins_capture_ticket_data.str_ticketing_airline_character_code = str_line.split(';')[0][2:4].strip()
                    pass
                
                elif str_line[:2] == "FP":  ### page 332
                
                    
                    if str_line[:3] == 'FPO' :
                        ins_capture_ticket_data.bln_reissue_fop = True
                        
                    if str_line.strip() not in ins_capture_ticket_data.lst_rm_field_data :
                        ins_capture_ticket_data.lst_rm_field_data.append(str_line.strip())
                        
                    if str_line.find('CASH') != -1 :
                        ins_capture_ticket_data.str_fop_data = 'CASH'
                 
                    try:
                        (str_customer_code,
                            str_lpo_number,
                            flt_discount_given,
                            flt_discount_given_per,
                            flt_service_fee,
                            flt_service_fee_per,
                            str_card_approval_code,
                            str_cc_type,
                            str_cc_card_no,
                            flt_uccf_amount,
                            bln_partial_uccf ,
                            lst_card_data,
                            lst_cc_seg_nos,flt_credit_amount) = self.get_fp_section_data(str_line)
                            
                            
                        if str_customer_code or str_lpo_number or flt_discount_given or flt_discount_given_per or \
                            flt_service_fee or flt_service_fee_per or str_card_approval_code or str_cc_type or \
                                str_cc_card_no or flt_uccf_amount or bln_partial_uccf :
                            ins_capture_ticket_data.str_customer_code = str_customer_code.split(';')[0].strip()


                            if ins_capture_ticket_data.str_customer_code == 'CASH':
                                ins_capture_ticket_data.str_customer_code = ''
                                str_customer_code = ''

                            ins_capture_ticket_data.bln_partial_uccf = bln_partial_uccf
                            ins_capture_ticket_data.flt_uccf_amount = flt_uccf_amount
                            ins_capture_ticket_data.flt_credit_amount = flt_credit_amount
                            ins_capture_ticket_data.str_lpo_number = str_lpo_number
                            ins_capture_ticket_data.str_card_approval_code = str_card_approval_code
                            ins_capture_ticket_data.str_cc_card_no = str_cc_card_no
                            ins_capture_ticket_data.str_cc_type = str_cc_type
                            ins_capture_ticket_data.lst_cc_seg_nos = lst_cc_seg_nos
                            if not ins_capture_ticket_data.lst_card_data :
                                ins_capture_ticket_data.lst_card_data = lst_card_data
                            if flt_discount_given:
                                ins_capture_ticket_data.flt_service_charge = flt_discount_given
                                pass
                            ins_capture_ticket_data.dct_fp_data[ins_capture_ticket_data.lst_ticket_detls[-1][0]] = [str_customer_code, str_lpo_number, flt_discount_given, flt_discount_given_per, flt_service_fee, flt_service_fee_per]
                    except:
                        pass
                elif str_line[:3] == "MFM":
                    try :
                        #refs ##18184
                        if ins_capture_ticket_data.flt_std_commn_percentage_inv == 0:
                            ins_capture_ticket_data.flt_std_commn_percentage_inv = float(str_line.split(";")[0].split("*")[-1].strip())
                    except :
                        ins_capture_ticket_data.flt_std_commn_percentage_inv = 0
                        pass
                elif str_line[:3] == "EMD":
                    lst_split_line = str_line.split(";")
                    # //  checks whether the ticket is already saved.
                    if lst_tsm_number:
                        if lst_split_line[5][1:3].strip('\n').strip('\r').strip() in lst_tsm_number:
                            continue

                    if ins_capture_emd_ticket_data.str_tsm_number:
                        if ins_capture_emd_ticket_data.str_tsm_number != lst_split_line[5][1:3].strip('\n').strip('\r').strip():
                            ins_capture_emd_ticket_data.int_tickets_count = ins_capture_emd_ticket_data.int_tickets_count + 1
                        continue  #42910

                    ins_capture_emd_ticket_data.str_ticketing_airline_character_code = lst_split_line[1][3:].strip()
                    ins_capture_emd_ticket_data.str_ticketing_airline_numeric_code = lst_split_line[2][:3].strip()
                    ins_capture_emd_ticket_data.str_check_digit = lst_split_line[2][3:].strip()
                    ins_capture_emd_ticket_data.str_ticketing_airline_name = lst_split_line[3].strip()
                    #ins_capture_emd_ticket_data.str_file_creation_date = lst_split_line[4].strip()
                    ins_capture_emd_ticket_data.str_tsm_identifier = lst_split_line[5][0].strip()
                    ins_capture_emd_ticket_data.str_tsm_number = lst_split_line[5][1:3].strip()
                    ins_capture_emd_ticket_data.str_emd_ticket_number = lst_split_line[6].strip()
                    ins_capture_emd_ticket_data.str_carrier_airline_code_mkt = lst_split_line[7].strip()
                    ins_capture_emd_ticket_data.str_carrier_airline_code_srvcng = lst_split_line[8].strip()
                    ins_capture_emd_ticket_data.str_carrier_fee_owner = lst_split_line[9].strip()
                    ins_capture_emd_ticket_data.str_orgin_airport_code = lst_split_line[10].strip()
                    ins_capture_emd_ticket_data.str_dest_code = lst_split_line[11].strip()
                    ins_capture_emd_ticket_data.str_carrier_or_operator = lst_split_line[12].split("-")[1].strip()
                    ins_capture_emd_ticket_data.str_at_location = lst_split_line[13].split("-")[1].strip()

                    ins_capture_emd_ticket_data.str_emd_type = lst_split_line[14]
                    ins_capture_emd_ticket_data.str_reason_issuance = lst_split_line[15].strip()
                    ins_capture_emd_ticket_data.str_reason_issuance_descr = lst_split_line[16].strip()
                    ins_capture_emd_ticket_data.str_issuance_subcode = lst_split_line[17].strip()
                    ins_capture_emd_ticket_data.str_issuance_subcode_descr = lst_split_line[18].strip()
                    ins_capture_emd_ticket_data.str_emd_remarks = lst_split_line[19].strip()
                    ins_capture_emd_ticket_data.str_service_remarks = lst_split_line[20].strip()
                    ins_capture_emd_ticket_data.str_ticket_value = lst_split_line[26][3:].strip()
                    ins_capture_emd_ticket_data.str_issue_type = lst_split_line[27].strip()
                    # In case of reissue amount collected is from this position Refer #9874 #9875
                    if ins_capture_emd_ticket_data.str_issue_type.upper() == 'R':
                        ins_capture_emd_ticket_data.str_currecy_code = lst_split_line[132][:3].strip()
                        ins_capture_emd_ticket_data.str_total_amount = lst_split_line[132][3:].strip()
                    else:
                        ins_capture_emd_ticket_data.str_currecy_code = lst_split_line[28][:3].strip()
                        ins_capture_emd_ticket_data.str_total_amount = lst_split_line[28][3:].strip()
                        pass

                    ins_capture_emd_ticket_data.str_tax_indicator = lst_split_line[29].strip()

                    if ins_capture_emd_ticket_data.int_number_of_segments == 0:
                        ins_capture_emd_ticket_data.str_start_port_code = ins_capture_emd_ticket_data.str_orgin_airport_code
                        pass

                    ins_capture_emd_ticket_data.int_number_of_segments = ins_capture_emd_ticket_data.int_number_of_segments + 1

                    ins_capture_emd_ticket_data.str_refundable_amount = lst_split_line[32][3:].strip()

                    for str_temp_data in lst_split_line[33:] : # refer bug 13203 in premium and 28967
                        
                        if str_temp_data.strip() and str_temp_data[:2] == 'T-' :
                            str_tax_amount = str_temp_data[-13:-4].strip() or str(0)
                            str_tax_code =  str_temp_data[-4:].strip()
                            str_nature_code = float(0)

                            if str_tax_amount and str_tax_amount not in ins_capture_emd_ticket_data.lst_tax:  # refer Bug #39651
                                ins_capture_emd_ticket_data.lst_tax.append((str_tax_amount,
                                                                                 str_tax_code,
                                                                                 str_nature_code))
                                ins_capture_emd_ticket_data.flt_tax = ins_capture_emd_ticket_data.flt_tax + float(str_tax_amount)

                    dbl_sector_wise_fare = 0

                    str_issue_date = ins_general_methods.generate_valid_date(ins_capture_emd_ticket_data.str_file_creation_date)
                    ins_capture_emd_ticket_data.str_arrival_date = ins_general_methods.generate_valid_date(ins_capture_emd_ticket_data.str_arrival_date,str_issue_date)
                    ins_capture_emd_ticket_data.str_departure_date = ins_general_methods.generate_valid_date(ins_capture_emd_ticket_data.str_departure_date,str_issue_date)

                    ins_capture_emd_ticket_data.bln_stopover_permitted = False

                    ins_capture_emd_ticket_data.lst_sector.append(ins_capture_emd_ticket_data.str_dest_code)

                    ins_capture_emd_ticket_data.lst_sector_details.append([ins_capture_emd_ticket_data.str_orgin_airport_code,
                                                                        ins_capture_emd_ticket_data.str_dest_code,
                                                                        ins_capture_emd_ticket_data.str_ticketing_airline_character_code,
                                                                        ins_capture_emd_ticket_data.str_airline_no,
                                                                        ins_capture_emd_ticket_data.str_flight_number,
                                                                        ins_capture_emd_ticket_data.str_class_of_service,
                                                                        ins_capture_emd_ticket_data.str_class_of_booking,
                                                                        ins_capture_emd_ticket_data.str_arrival_date,
                                                                        ins_capture_emd_ticket_data.str_departure_date,
                                                                        ins_capture_emd_ticket_data.bln_stopover_permitted,
                                                                        ins_capture_emd_ticket_data.int_mileage,
                                                                        dbl_sector_wise_fare,
                                                                        ins_capture_emd_ticket_data.str_arrival_time,
                                                                        ins_capture_emd_ticket_data.str_departure_time,
                                                                        False
                                                                        ])


                    if ins_capture_emd_ticket_data.str_tax_indicator == 'Y' and not ins_capture_emd_ticket_data.flt_market_fare:
                        ins_capture_emd_ticket_data.flt_market_fare += float(ins_capture_emd_ticket_data.str_total_amount) - float(ins_capture_emd_ticket_data.flt_tax)
                    elif not ins_capture_emd_ticket_data.flt_market_fare:
                        ins_capture_emd_ticket_data.flt_market_fare += float(ins_capture_emd_ticket_data.str_total_amount)
                    pass
                elif str_line[:3] == "ICW":
                    #Code to capture connection ticket.(Refer #11315)
                    lst_emd_ticket = str_line.split(';')
                    str_connection_ticket = lst_emd_ticket[0][6:16]
                    ins_capture_emd_ticket_data.str_connection_ticket = str_connection_ticket
                    ins_capture_emd_ticket_data.bln_emd_ticket = True
                    pass

                elif str_line[:4] == "TMCM":
                    lst_split_line = str_line.split(";")

                    str_conjection_ticket_number = ''
                    chr_ticket_type = "VMCO"
                    ins_capture_ticket_data.bln_vmco = True
                    ins_capture_ticket_data.str_ticketing_airline_numeric_code = str_line[4:7].strip()
                    ins_capture_ticket_data.str_base_ticket_number = lst_split_line[0].split("-")[1].strip()
                    ins_capture_ticket_data.str_icw_doc_no = lst_split_line[2].strip()

                    flt_service_charge = 0
                    ins_capture_ticket_data.lst_ticket_detls.append([ins_capture_ticket_data.str_base_ticket_number,
                                                                     str_conjection_ticket_number,
                                                                     chr_ticket_type,
                                                                     ins_capture_ticket_data.str_ticketing_airline_character_code,
                                                                     ins_capture_ticket_data.str_ticketing_airline_numeric_code,
                                                                     ins_capture_ticket_data.str_pax_name,
                                                                     ins_capture_ticket_data.str_pax_type,
                                                                     flt_service_charge,
                                                                     ins_capture_ticket_data.str_isection_mobile,
                                                                     ins_capture_ticket_data.str_isection_email,
                                                                     ins_capture_ticket_data.int_pax_item_number
                                                                     ])


                    pass
                elif str_line[:4] in  ("TMCD","TMCN"):
                    lst_split_line = str_line.split(";")
                    # //  checks whether the ticket is already saved.
                    if lst_tsm_number:
                        if lst_split_line[2][1:3].strip('\n').strip('\r').strip() in lst_tsm_number:
                            continue

                    if ins_capture_emd_ticket_data.str_tsm_number:
                        if ins_capture_emd_ticket_data.str_tsm_number != lst_split_line[2][1:3].strip('\n').strip('\r').strip():
                            continue

                    str_conjection_ticket_number = ''
                    chr_ticket_type = "EMD"
                    ins_capture_emd_ticket_data.bln_emd = True
                    ins_capture_emd_ticket_data.str_ticketing_airline_numeric_code = str_line[4:7].strip()
                    ins_capture_emd_ticket_data.str_base_ticket_number = lst_split_line[0].split("-")[1].strip()
                    
                    if len(lst_split_line) > 2 :
                        ins_capture_emd_ticket_data.str_icw_doc_no = lst_split_line[2].strip()
                    else :
                        ins_capture_emd_ticket_data.str_icw_doc_no = ''

                    flt_service_charge = 0
                    ins_capture_emd_ticket_data.lst_ticket_detls.append([ins_capture_emd_ticket_data.str_base_ticket_number,
                                                                     str_conjection_ticket_number,
                                                                     chr_ticket_type,
                                                                     ins_capture_emd_ticket_data.str_ticketing_airline_character_code,
                                                                     ins_capture_emd_ticket_data.str_ticketing_airline_numeric_code,
                                                                     ins_capture_emd_ticket_data.str_pax_name,
                                                                     ins_capture_emd_ticket_data.str_pax_type,
                                                                     flt_service_charge,
                                                                     ins_capture_ticket_data.str_isection_mobile,
                                                                     ins_capture_ticket_data.str_isection_email,
                                                                     ins_capture_ticket_data.int_pax_item_number
                                                                     ])
                    pass

                elif str_line[:3] == "MFP":
                    
                    if str_line.strip() not in ins_capture_emd_ticket_data.lst_rm_field_data :
                        ins_capture_emd_ticket_data.lst_rm_field_data.append(str_line.strip())
                        
                    if str_line.find('CASH') != -1 :
                        ins_capture_emd_ticket_data.str_fop_data = 'CASH'
                        
                    try:   ## refer #14903
                        (str_customer_code,
                            str_lpo_number,
                            flt_discount_given,
                            flt_discount_given_per,
                            flt_service_fee,
                            flt_service_fee_per,
                            str_card_approval_code,
                            str_cc_type,
                            str_cc_card_no,
                            flt_uccf_amount,
                            bln_partial_uccf ,
                            lst_card_data,
                            lst_cc_seg_nos,
                            flt_credit_amount
                            ) = self.get_fp_section_data(str_line)
                        if str_customer_code or str_lpo_number or flt_discount_given or flt_discount_given_per or \
                            flt_service_fee or flt_service_fee_per or str_card_approval_code or str_cc_type or \
                                str_cc_card_no or flt_uccf_amount or bln_partial_uccf :
                            ins_capture_emd_ticket_data.str_customer_code = str_customer_code.split(';')[0].strip()
                            ins_capture_emd_ticket_data.str_lpo_number = str_lpo_number
                            ins_capture_emd_ticket_data.str_card_approval_code = str_card_approval_code
                            ins_capture_emd_ticket_data.str_cc_type = str_cc_type
                            ins_capture_emd_ticket_data.str_cc_card_no = str_cc_card_no
                            ins_capture_emd_ticket_data.bln_partial_uccf = bln_partial_uccf
                            ins_capture_emd_ticket_data.flt_uccf_amount = flt_uccf_amount
                            if not ins_capture_emd_ticket_data.lst_card_data :
                                ins_capture_emd_ticket_data.lst_card_data = lst_card_data
                            if flt_discount_given:
                                ins_capture_emd_ticket_data.flt_service_charge = flt_discount_given
                                pass
                            ins_capture_emd_ticket_data.dct_fp_data[ins_capture_ticket_data.lst_ticket_detls[-1][0]] = [str_customer_code, str_lpo_number, flt_discount_given, flt_discount_given_per, flt_service_fee, flt_service_fee_per]
                    except:
                        pass
                    pass
#                
                elif 'CUST_PAX_EMAIL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][0]):
                    try:
                        ins_capture_ticket_data.str_rm_email = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][0],'').replace(str_seperator,'')[:50].strip().strip('\n').strip().split(';')[0]
                        ins_capture_ticket_data.str_cust_pax_email = ins_capture_ticket_data.str_rm_email
                        ins_capture_emd_ticket_data.str_rm_email = ins_capture_ticket_data.str_rm_email
                        ins_capture_emd_ticket_data.str_cust_pax_email = ins_capture_ticket_data.str_rm_email
                    except:
                        ins_capture_ticket_data.str_rm_email = ''
                        ins_capture_emd_ticket_data.str_rm_email = ''
                        pass

                elif 'AGENCY_INTERNAL_REMARKS' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][0]):
                    try:
                        ins_capture_ticket_data.str_ticket_rm_remarks = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][0],'').replace(str_seperator,'').strip().strip('\n').strip()[:1000].split(';')[0]
                        ins_capture_emd_ticket_data.str_ticket_rm_remarks = ins_capture_ticket_data.str_ticket_rm_remarks
                    except:
                        ins_capture_ticket_data.str_ticket_rm_remarks = ''
                        ins_capture_emd_ticket_data.str_ticket_rm_remarks = ''

                    ins_capture_ticket_data.str_agency_internal_remarks = ins_capture_ticket_data.str_ticket_rm_remarks
                    ins_capture_emd_ticket_data.str_agency_internal_remarks = ins_capture_emd_ticket_data.str_ticket_rm_remarks
                       
                elif 'AGENCY_TICKETING_STAFF' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][0]):
                    try:   #  Refer #40193
                        ins_capture_ticket_data.str_agency_ticketing_staff = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][0],'').replace(str_seperator,'').strip()[:50].strip().strip('\n').strip().split(';')[0]
                        if re.match('[^@]+@[^@]+\.[^@]+',ins_capture_ticket_data.str_agency_ticketing_staff):  #45305
                            ins_capture_ticket_data.str_ticketing_agent_code, ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_general_methods.get_staff_code_from_email(ins_capture_ticket_data.str_agency_ticketing_staff, 'Amadeus')
                        elif ins_capture_ticket_data.str_agency_ticketing_staff[:4].isdigit(): 
                            ins_capture_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_agency_ticketing_staff[4:6]
                            ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_agency_ticketing_staff[:4]
                        else:
                            ins_capture_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_agency_ticketing_staff[:2]
                            ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_agency_ticketing_staff[2:6]
                        
                        ins_capture_emd_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_ticketing_agent_code                                
                        ins_capture_emd_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_ticketing_agent_numeric_code
                        ins_capture_emd_ticket_data.str_agency_ticketing_staff = ins_capture_ticket_data.str_agency_ticketing_staff
                    except:
                        ins_capture_ticket_data.str_ticketing_agent_numeric_code = ''
                        ins_capture_ticket_data.str_ticketing_agent_code = ''
                        ins_capture_emd_ticket_data.str_ticketing_agent_numeric_code = ''
                        ins_capture_emd_ticket_data.str_ticketing_agent_code = ''

                elif 'CUST_PAX_MOBILE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][0]):
                    try:
                        ins_capture_ticket_data.str_phone = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][0],'').replace(str_seperator,'')[:100].strip().strip('\n').strip().split(';')[0]
                        ins_capture_ticket_data.str_cust_pax_mobile = ins_capture_ticket_data.str_phone
                        ins_capture_emd_ticket_data.str_phone = ins_capture_ticket_data.str_phone
                        ins_capture_emd_ticket_data.str_cust_pax_mobile = ins_capture_ticket_data.str_phone
                    except:
                        ins_capture_ticket_data.str_phone = ''
                        ins_capture_emd_ticket_data.str_phone = ''
                        ins_capture_ticket_data.str_cust_pax_mobile = ''
                        ins_capture_emd_ticket_data.str_cust_pax_mobile = ''

                elif 'CUST_PURPOSE_OF_TRAVEL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][0]):
                    try:
                        ins_capture_ticket_data.str_purpose = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][0],'').replace(str_seperator,'')[:200].strip('\n').strip().split(';')[0]
                        ins_capture_emd_ticket_data.str_purpose = ins_capture_ticket_data.str_purpose
                    except:
                        ins_capture_ticket_data.str_purpose = ''
                        ins_capture_emd_ticket_data.str_purpose = ''

                    ins_capture_ticket_data.str_cust_purpose_of_travel = ins_capture_ticket_data.str_purpose
                    ins_capture_emd_ticket_data.str_cust_purpose_of_travel = ins_capture_emd_ticket_data.str_purpose

                elif 'AGENCY_COST CENTRE_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][0]):
                        ins_capture_ticket_data.str_rm_cost_centre = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][0],'').replace(str_seperator,'')[:20].strip().strip('\n').strip().split(';')[0]
                        ins_capture_emd_ticket_data.str_rm_cost_centre = ins_capture_ticket_data.str_rm_cost_centre
                        ins_capture_ticket_data.str_auto_invoice_location = ins_capture_ticket_data.str_rm_cost_centre
                        ins_capture_emd_ticket_data.str_auto_invoice_location = ins_capture_ticket_data.str_rm_cost_centre

                elif 'CUST_JOB_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][0]):
                    ins_capture_ticket_data.str_rm_job_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][0],'').replace(str_seperator,'')[:20].strip().strip('\n').strip().split(';')[0]
                    ins_capture_ticket_data.str_cust_job_code = ins_capture_ticket_data.str_rm_job_code
                    ins_capture_emd_ticket_data.str_rm_job_code = ins_capture_ticket_data.str_rm_job_code
                    ins_capture_emd_ticket_data.str_cust_job_code = ins_capture_ticket_data.str_rm_job_code

                elif 'PARTY_LPO_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][0]):
                    ins_capture_ticket_data.str_rm_lpo_number = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][0],'').replace(str_seperator,'')[:50].strip().strip('\n').strip().split(';')[0]
                    ins_capture_ticket_data.str_party_lpo_no = ins_capture_ticket_data.str_rm_lpo_number
                    ins_capture_emd_ticket_data.str_rm_lpo_number = ins_capture_ticket_data.str_rm_lpo_number
                    ins_capture_emd_ticket_data.str_party_lpo_no = ins_capture_ticket_data.str_rm_lpo_number

                elif 'CUST_EMPLOYEE_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][0]):
                    ins_capture_ticket_data.str_rm_employee_number = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][0],'').replace(str_seperator,'')[:20].strip().strip('\n').strip().split(';')[0]
                    ins_capture_ticket_data.str_cust_employee_no = ins_capture_ticket_data.str_rm_employee_number
                    ins_capture_emd_ticket_data.str_rm_employee_number = ins_capture_ticket_data.str_rm_employee_number
                    ins_capture_emd_ticket_data.str_cust_employee_no = ins_capture_ticket_data.str_rm_employee_number

                

                #RM*SF:EY/AED18475/AED0/AED200;S2-3;P1-4
                #RM*SF:airlinecode/sellprice/commision/svf
                elif 'FARE_SERVICE_FEE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][0]):
                    ins_capture_ticket_data.bln_adt_svf = True
#                    ins_capture_emd_ticket_data.bln_adt_svf = True
                    
                    
                    try:

                        flt_rm_service_fee = 0.00
                        str_pax_no_string = ''
                        lst_pax_nos = []
                        lst_sector_nos = []

                        str_svf_line = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][0],'').replace(self.str_defult_currency_code,'')
                        lst_svf_line = str_svf_line.split(';')[0].split("/")

                        try :
                            flt_rm_service_fee = ins_general_methods.rm_str_to_flt(lst_svf_line[3].strip())
                        except: 
                            flt_rm_service_fee = ins_general_methods.rm_str_to_flt(lst_svf_line[2].strip())

                        """
                        ## refs CR #16670
                        #### OLD LOGIC. CHECK NEW COMMENTS BELOW  ####
                        
                            If a values like P1,P2 etc comes in service fee section then it means that the service is for
                            pax 1 and pax 2 (ticket 1 and 2)
                            these two passengers may be having one service fee and the others may be having an other service fee
                            this happens in case one passenger is adult and another one is a child.

                            Possibilities are :
                            P1-4  ==> P1,P2,P3,P4
                            P3-4,6 => P3,P4,P6
                            P1,P2 ==> P1,P2
                            P1    ==> P1

                            Like in this same format segment data also may come
                            S1,S2 etc
                            This is useful in the case of other service capturing
                            Other service data comes in the position of segments in the air file
                            When reading the file need to note the position from which the service fee was captured and
                            need to assign service fee with respect to that positiion
                            If the other service data was captured from the second segment then a service fee line which contains S2
                            will have the servive fee of that service.

                            If a service fee line do not have any segment or passenger number associated with it then the service fee
                            is common for all the items in the capture file


                        """
                        
                        """
                        
                        ### NEW CODE WORKING ###
                        
                        
                        Above logic changed on 09/09/2015
                        Instead of pax number TST number is selected.
                        Instead of P1 P2
                        lines like T1 T2 will come in capture file.

                        """

                        if str_svf_line.find(';') != -1 :
                            str_pax_no_string = ';'.join(str_svf_line.split(';')[1:])
                            lst_pax_no_string = list(str_pax_no_string)
                            
                            str_value = ''
                            str_prev = ''
                            bln_pax_data = False #39957
                            bln_sector_data = False
                            
                            
                            for str_value in lst_pax_no_string :

                                if not str_prev :
                                    #if first iteration
                                    str_prev = str_value
                                    if str_value == 'T' :
                                        bln_pax_data = True
                                        bln_sector_data = False
                                    elif str_value == 'S' :
                                        bln_sector_data = True
                                        bln_pax_data = False
                                    continue


                                if str_value == 'T' :
                                    bln_pax_data = True
                                    bln_sector_data = False
                                    str_prev = str_value
                                    continue
                                elif str_value == 'S' :
                                    bln_pax_data = False
                                    bln_sector_data = True
                                    str_prev = str_value
                                    continue

                                if bln_pax_data :

                                    if str_value.isdigit() and str_prev != '-' :
                                        lst_pax_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_pax_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_pax_nos.append(int_start)
                                            int_start += 1
                                            pass

                                elif bln_sector_data :
                                    if str_value.isdigit() and str_prev != '-' :
                                        lst_sector_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_sector_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_sector_nos.append(int_start)
                                            int_start += 1
                                    pass
                                str_prev = str_value
                                pass

                    except:
                        try :
                            flt_rm_service_fee = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].strip())
                        except :
                            flt_rm_service_fee = 0.00

                    
                    if lst_pax_nos :
                        for int_pax_no in lst_pax_nos :
                            ins_general_methods.ins_global.dct_pax_no_service_fee[int(int_pax_no)] = flt_rm_service_fee
                        pass
                    elif not  ins_general_methods.ins_global.dct_pax_no_service_fee and flt_rm_service_fee:
                        ins_general_methods.ins_global.dct_pax_no_service_fee['ALL'] = flt_rm_service_fee
                        pass
                    if lst_sector_nos :
                        for int_seg_no in lst_sector_nos :
                            ins_general_methods.ins_global.dct_seg_no_service_fee[int(int_seg_no)] = flt_rm_service_fee
                        pass
                    elif not ins_general_methods.ins_global.dct_seg_no_service_fee and flt_rm_service_fee:
                        ins_general_methods.ins_global.dct_seg_no_service_fee['ALL'] = flt_rm_service_fee

                    # refer 30961
                    if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                        starting_segment = str_line.split('$')[1][:3]
                        ins_general_methods.ins_global.dct_service_fee_sector_wise[starting_segment] =  ins_general_methods.rm_str_to_flt(lst_svf_line[0].strip() or 0)
                    elif flt_rm_service_fee :
                        ins_capture_ticket_data.flt_rm_service_charge = flt_rm_service_fee
#                        ins_capture_emd_ticket_data.flt_rm_service_charge = flt_rm_service_fee

                        
                    ins_capture_ticket_data.flt_service_fee_ext = ins_capture_ticket_data.flt_rm_service_charge
#                    ins_capture_emd_ticket_data.flt_service_fee_ext = ins_capture_emd_ticket_data.flt_rm_service_charge


                #RM*SF:EY/AED18475/AED0/AED200;S2-3;P1-4
                #RM*SF:airlinecode/sellprice/commision/svf
                elif 'VOUCHER_SUPPLIER_AMOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['VOUCHER_SUPPLIER_AMOUNT'][0] and (str_line.startswith('RX NR:') or str_line.startswith('RX *NR:')):
                    
                            
                    try:
                        str_pax_no_string = ''
                        lst_pax_nos = []
                        lst_sector_nos = []

                        str_svf_line = str_line.replace('RX NR:','').replace('RX *NR:','').replace(self.str_defult_currency_code,'')
                        
                        try :
                            flt_rm_voucher_supplier_amount = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].split('/')[0].strip() or 0)
                        except :
                            flt_rm_voucher_supplier_amount = 0.00
                        
                        if str_svf_line.find(';') != -1 :
                            str_pax_no_string = ';'.join(str_svf_line.split(';')[1:])
                            lst_pax_no_string = list(str_pax_no_string)
                            
                            str_value = ''
                            str_prev = ''

                            for str_value in lst_pax_no_string :

                                if not str_prev :
                                    #if first iteration
                                    str_prev = str_value
                                    if str_value == 'T' :
                                        bln_pax_data = True
                                        bln_sector_data = False
                                    elif str_value == 'S' :
                                        bln_sector_data = True
                                        bln_pax_data = False
                                    continue

                                if str_value == 'T' :
                                    bln_pax_data = True
                                    bln_sector_data = False
                                    str_prev = str_value
                                    continue
                                elif str_value == 'S' :
                                    bln_pax_data = False
                                    bln_sector_data = True
                                    str_prev = str_value
                                    continue

                                if bln_pax_data :

                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_pax_nos[-1]) + str_value
                                        lst_pax_nos.pop(-1)
                                        lst_pax_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_pax_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_pax_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_pax_nos.append(int_start)
                                            int_start += 1
                                            pass

                                elif bln_sector_data :
                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_sector_nos[-1]) + str_value
                                        lst_sector_nos.pop(-1)
                                        lst_sector_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_sector_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_sector_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_sector_nos.append(int_start)
                                            int_start += 1
                                    pass
                                str_prev = str_value
                                pass

                    except:
                        try :
                            flt_rm_voucher_supplier_amount = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].strip() or 0)
                        except :
                            flt_rm_voucher_supplier_amount = 0.00

                    if lst_sector_nos :
                        for int_seg_no in lst_sector_nos :
                            ins_general_methods.ins_global.dct_seg_no_voucher_supplier_amount[int(int_seg_no)] = flt_rm_voucher_supplier_amount
                        pass
                    elif not ins_general_methods.ins_global.dct_seg_no_voucher_supplier_amount :
                        ins_general_methods.ins_global.dct_seg_no_voucher_supplier_amount['ALL'] = flt_rm_voucher_supplier_amount

                    ins_capture_ticket_data.flt_rm_voucher_supplier_amount = flt_rm_voucher_supplier_amount
                    ins_capture_emd_ticket_data.flt_rm_voucher_supplier_amount = flt_rm_voucher_supplier_amount
                    
                elif 'VOUCHER_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['VOUCHER_SELLING_PRICE'][0] and (str_line.startswith('RX RQ:') or str_line.startswith('RX *RQ:')):     
                    try:
                        str_pax_no_string = ''
                        lst_pax_nos = []
                        lst_sector_nos = []
                    
                        str_svf_line = str_line.replace('RX RQ:','').replace('RX *RQ:','').replace(self.str_defult_currency_code,'')
                        
                        try :
                            flt_rm_voucher_selling_price = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].split('/')[0].strip() or 0)
                        except :
                            flt_rm_voucher_selling_price = 0.00
                        
                        if str_svf_line.find(';') != -1 :
                            str_pax_no_string = ';'.join(str_svf_line.split(';')[1:])
                            lst_pax_no_string = list(str_pax_no_string)
                            
                            str_value = ''
                            str_prev = ''

                            for str_value in lst_pax_no_string :

                                if not str_prev :
                                    #if first iteration
                                    str_prev = str_value
                                    if str_value == 'T' :
                                        bln_pax_data = True
                                        bln_sector_data = False
                                    elif str_value == 'S' :
                                        bln_sector_data = True
                                        bln_pax_data = False
                                    continue

                                if str_value == 'T' :
                                    bln_pax_data = True
                                    bln_sector_data = False
                                    str_prev = str_value
                                    continue
                                elif str_value == 'S' :
                                    bln_pax_data = False
                                    bln_sector_data = True
                                    str_prev = str_value
                                    continue

                                if bln_pax_data :

                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_pax_nos[-1]) + str_value
                                        lst_pax_nos.pop(-1)
                                        lst_pax_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_pax_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_pax_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_pax_nos.append(int_start)
                                            int_start += 1
                                            pass

                                elif bln_sector_data :
                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_sector_nos[-1]) + str_value
                                        lst_sector_nos.pop(-1)
                                        lst_sector_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_sector_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_sector_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_sector_nos.append(int_start)
                                            int_start += 1
                                    pass
                                str_prev = str_value
                                pass

                    except:
                        try :
                            flt_rm_voucher_selling_price = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].strip() or 0)
                        except :
                            flt_rm_voucher_selling_price = 0.00

                    if lst_sector_nos :
                        for int_seg_no in lst_sector_nos :
                            ins_general_methods.ins_global.dct_seg_no_voucher_selling_price[int(int_seg_no)] = flt_rm_voucher_selling_price
                        pass
                    elif not ins_general_methods.ins_global.dct_seg_no_voucher_selling_price and flt_rm_voucher_selling_price:
                        ins_general_methods.ins_global.dct_seg_no_voucher_selling_price['ALL'] = flt_rm_voucher_selling_price

                    if flt_rm_voucher_selling_price:
                        ins_capture_ticket_data.flt_rm_voucher_selling_price = flt_rm_voucher_selling_price
                        ins_capture_emd_ticket_data.flt_rm_voucher_selling_price = flt_rm_voucher_selling_price
                    
                elif 'VOUCHER_SUPPLIER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['VOUCHER_SUPPLIER'][0] and (str_line.startswith('RX VENDOR:') or str_line.startswith('RX *VENDOR:')):     
                    try:
                        str_pax_no_string = ''
                        lst_pax_nos = []
                        lst_sector_nos = []

                        str_svf_line = str_line.replace('RX VENDOR:','').replace('RX *VENDOR:','').replace(self.str_defult_currency_code,'')
                        
                        try :
                            str_voucher_supplier = str_svf_line.split(';')[0].split('/')[0].strip() or ''
                        except :
                            str_voucher_supplier = ''
                        
                        if str_svf_line.find(';') != -1 :
                            str_pax_no_string = ';'.join(str_svf_line.split(';')[1:])
                            lst_pax_no_string = list(str_pax_no_string)
                            
                            str_value = ''
                            str_prev = ''

                            for str_value in lst_pax_no_string :

                                if not str_prev :
                                    #if first iteration
                                    str_prev = str_value
                                    if str_value == 'T' :
                                        bln_pax_data = True
                                        bln_sector_data = False
                                    elif str_value == 'S' :
                                        bln_sector_data = True
                                        bln_pax_data = False
                                    continue

                                if str_value == 'T' :
                                    bln_pax_data = True
                                    bln_sector_data = False
                                    str_prev = str_value
                                    continue
                                elif str_value == 'S' :
                                    bln_pax_data = False
                                    bln_sector_data = True
                                    str_prev = str_value
                                    continue

                                if bln_pax_data :

                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_pax_nos[-1]) + str_value
                                        lst_pax_nos.pop(-1)
                                        lst_pax_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_pax_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_pax_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_pax_nos.append(int_start)
                                            int_start += 1
                                            pass

                                elif bln_sector_data :
                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_sector_nos[-1]) + str_value
                                        lst_sector_nos.pop(-1)
                                        lst_sector_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_sector_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_sector_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_sector_nos.append(int_start)
                                            int_start += 1
                                    pass
                                str_prev = str_value
                                pass

                    except:
                        try :
                            str_voucher_supplier = str_svf_line.split(';')[0].strip() or ''
                        except :
                            str_voucher_supplier = ''

                    if lst_sector_nos :
                        for int_seg_no in lst_sector_nos :
                            ins_general_methods.ins_global.dct_seg_no_voucher_supplier[int(int_seg_no)] = str_voucher_supplier
                        pass
                    elif not ins_general_methods.ins_global.dct_seg_no_voucher_supplier and str_voucher_supplier:
                        ins_general_methods.ins_global.dct_seg_no_voucher_supplier['ALL'] = str_voucher_supplier

                    if str_voucher_supplier :
                        ins_capture_ticket_data.str_voucher_supplier = str_voucher_supplier
                        ins_capture_emd_ticket_data.str_voucher_supplier = str_voucher_supplier
                    
                elif 'VOUCHER_DISCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['VOUCHER_DISCOUNT'][0] and (str_line.startswith('RX DISCOUNT:') or str_line.startswith('RX *DISCOUNT:')):

                    try:
                        str_pax_no_string = ''
                        lst_pax_nos = []
                        lst_sector_nos = []

                        str_svf_line = str_line.replace('RX DISCOUNT:','').replace('RX *DISCOUNT:','').replace(self.str_defult_currency_code,'')
                        
                        try :
                            flt_rm_voucher_discount = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].strip() or 0)
                        except :
                            flt_rm_voucher_discount = 0.00
                        
                        if str_svf_line.find(';') != -1 :
                            str_pax_no_string = ';'.join(str_svf_line.split(';')[1:])
                            lst_pax_no_string = list(str_pax_no_string)
                            
                            str_value = ''
                            str_prev = ''

                            for str_value in lst_pax_no_string :

                                if not str_prev :
                                    #if first iteration
                                    str_prev = str_value
                                    if str_value == 'T' :
                                        bln_pax_data = True
                                        bln_sector_data = False
                                    elif str_value == 'S' :
                                        bln_sector_data = True
                                        bln_pax_data = False
                                    continue
                                    
                                if str_value == 'T' :
                                    bln_pax_data = True
                                    bln_sector_data = False
                                    str_prev = str_value
                                    continue
                                elif str_value == 'S' :
                                    bln_pax_data = False
                                    bln_sector_data = True
                                    str_prev = str_value
                                    continue

                                if bln_pax_data :

                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_pax_nos[-1]) + str_value
                                        lst_pax_nos.pop(-1)
                                        lst_pax_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_pax_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_pax_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_pax_nos.append(int_start)
                                            int_start += 1
                                            pass

                                elif bln_sector_data :
                                    
                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_sector_nos[-1]) + str_value
                                        lst_sector_nos.pop(-1)
                                        lst_sector_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_sector_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_sector_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_sector_nos.append(int_start)
                                            int_start += 1
                                    pass
                                str_prev = str_value
                                pass
                            
                    except:
                        try :
                            flt_rm_voucher_discount = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].strip() or 0)
                        except :
                            flt_rm_voucher_discount = 0.00

                    if lst_sector_nos :
                        for int_seg_no in lst_sector_nos :
                            ins_general_methods.ins_global.dct_seg_no_voucher_discount[int(int_seg_no)] = flt_rm_voucher_discount
                        pass
                    elif not ins_general_methods.ins_global.dct_seg_no_voucher_discount and flt_rm_voucher_discount:
                        ins_general_methods.ins_global.dct_seg_no_voucher_discount['ALL'] = flt_rm_voucher_discount

                    if flt_rm_voucher_discount :
                        ins_capture_ticket_data.flt_rm_voucher_discount = flt_rm_voucher_discount
                        ins_capture_emd_ticket_data.flt_rm_voucher_discount = flt_rm_voucher_discount
                    
                elif 'VOUCHER_TAX' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['VOUCHER_TAX'][0] and (str_line.startswith('RX TAX:') or str_line.startswith('RX *TAX:')):
                        
                    try:
                        str_pax_no_string = ''
                        lst_pax_nos = []
                        lst_sector_nos = []

                        str_svf_line = str_line.replace('RX TAX:','').replace('RX *TAX:','').replace(self.str_defult_currency_code,'')
                        
                        try :
                            flt_rm_voucher_tax = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].strip() or 0)
                        except :
                            flt_rm_voucher_tax = 0.00
                        
                        if str_svf_line.find(';') != -1 :
                            str_pax_no_string = ';'.join(str_svf_line.split(';')[1:])
                            lst_pax_no_string = list(str_pax_no_string)
                            
                            str_value = ''
                            str_prev = ''

                            for str_value in lst_pax_no_string :

                                if not str_prev :
                                    #if first iteration
                                    str_prev = str_value
                                    if str_value == 'T' :
                                        bln_pax_data = True
                                        bln_sector_data = False
                                    elif str_value == 'S' :
                                        bln_sector_data = True
                                        bln_pax_data = False
                                    continue

                                if str_value == 'T' :
                                    bln_pax_data = True
                                    bln_sector_data = False
                                    str_prev = str_value
                                    continue
                                elif str_value == 'S' :
                                    bln_pax_data = False
                                    bln_sector_data = True
                                    str_prev = str_value
                                    continue

                                if bln_pax_data :
                                    
                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_pax_nos[-1]) + str_value
                                        lst_pax_nos.pop(-1)
                                        lst_pax_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_pax_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_pax_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_pax_nos.append(int_start)
                                            int_start += 1
                                            pass

                                elif bln_sector_data :
                                    if str_value.isdigit() and str_prev.isdigit() :
                                        str_num = str(lst_sector_nos[-1]) + str_value
                                        lst_sector_nos.pop(-1)
                                        lst_sector_nos.append(int(str_num))
                                        pass
                                    elif str_value.isdigit() and str_prev != '-' :
                                        lst_sector_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_sector_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_sector_nos.append(int_start)
                                            int_start += 1
                                    pass
                                str_prev = str_value
                                pass
                            
                    except:
                        try :
                            flt_rm_voucher_tax = ins_general_methods.rm_str_to_flt(str_svf_line.split(';')[0].strip() or 0)
                        except :
                            flt_rm_voucher_tax = 0.00

                    if lst_sector_nos :
                        for int_seg_no in lst_sector_nos :
                            ins_general_methods.ins_global.dct_seg_no_voucher_tax[int(int_seg_no)] = flt_rm_voucher_tax
                        pass
                    elif not ins_general_methods.ins_global.dct_seg_no_voucher_tax and flt_rm_voucher_tax :
                        ins_general_methods.ins_global.dct_seg_no_voucher_tax['ALL'] = flt_rm_voucher_tax

                    if flt_rm_voucher_tax :
                        ins_capture_ticket_data.flt_rm_voucher_tax = flt_rm_voucher_tax
                        ins_capture_emd_ticket_data.flt_rm_voucher_tax = flt_rm_voucher_tax

                elif 'FARE_DISCOUNT_GIVEN' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][0]):
                    
                    ins_capture_ticket_data.bln_rm_discount_adt = True
                    ins_capture_emd_ticket_data.bln_rm_discount_adt = True
                    
                    try:
                        str_discount = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][0],'').replace(str_seperator,'').split(';')[0].replace(self.str_defult_currency_code,'').strip() or '0'

                        ins_capture_ticket_data.flt_rm_discount = ins_general_methods.rm_str_to_flt(str_discount)
                        ins_capture_emd_ticket_data.flt_rm_discount = ins_general_methods.rm_str_to_flt(str_discount)
                    except:
                         ins_capture_ticket_data.flt_rm_discount = 0.0
                         ins_capture_emd_ticket_data.flt_rm_discount = 0.0

                    ins_capture_ticket_data.flt_discount_given_ext = ins_capture_ticket_data.flt_rm_discount
                    ins_capture_emd_ticket_data.flt_discount_given_ext = ins_capture_ticket_data.flt_rm_discount

                elif 'FARE_DISCOUNT_GIVEN_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][0]):
                    ins_capture_ticket_data.bln_rm_discount_chd = True
                    ins_capture_emd_ticket_data.bln_rm_discount_chd = True
                    try:
                        str_discount = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][0],'').replace(str_seperator,'').split(';')[0].replace(self.str_defult_currency_code,'').strip() or '0'

                        ins_capture_ticket_data.flt_rm_discount_chd = ins_general_methods.rm_str_to_flt(str_discount)
                        ins_capture_emd_ticket_data.flt_rm_discount_chd = ins_general_methods.rm_str_to_flt(str_discount)
                    except:
                         ins_capture_ticket_data.flt_rm_discount_chd = 0.0
                         ins_capture_emd_ticket_data.flt_rm_discount_chd = 0.0

                elif 'FARE_DISCOUNT_GIVEN_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][0]):
                    ins_capture_ticket_data.bln_rm_discount_inf = True
                    ins_capture_emd_ticket_data.bln_rm_discount_inf = True
                    try:
                        str_discount = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][0],'').replace(str_seperator,'').split(';')[0].replace(self.str_defult_currency_code,'').strip() or '0'

                        ins_capture_ticket_data.flt_rm_discount_inf = ins_general_methods.rm_str_to_flt(str_discount)
                        ins_capture_emd_ticket_data.flt_rm_discount_inf = ins_general_methods.rm_str_to_flt(str_discount)
                    except:
                         ins_capture_ticket_data.flt_rm_discount_inf = 0.0
                         ins_capture_emd_ticket_data.flt_rm_discount_inf = 0.0
                #45196         
                elif 'FARE_PLB_DISCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][0]):
                    try:
                        ins_capture_ticket_data.flt_rm_plb_discount = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_rm_plb_discount = ins_capture_ticket_data.flt_rm_plb_discount
                    except:
                        ins_capture_ticket_data.flt_rm_plb_discount = 0.0
                        ins_capture_emd_ticket_data.flt_rm_plb_discount = 0.0
                    
                elif 'FARE_DEAL_DISCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][0]):
                    try:
                        ins_capture_ticket_data.flt_rm_deal_discount = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_rm_deal_discount = ins_capture_ticket_data.flt_rm_deal_discount
                    except:
                        ins_capture_ticket_data.flt_rm_deal_discount = 0.0
                        ins_capture_emd_ticket_data.flt_fare_lost_amount_ext = 0.0

                elif 'FARE_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][0]):

                    try:
                        ## Refs #14792
                        flt_rm_selling_price = 0.00
                        str_pax_no_string = ''
                        lst_pax_nos = []
                        lst_sector_nos = []

                        str_sell_line = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][0],'').replace(self.str_defult_currency_code,'')
                        lst_sell_line = str_sell_line.split(';')[0].split("/")

                        try :
                            flt_rm_selling_price = ins_general_methods.rm_str_to_flt(lst_sell_line[0].strip())
                        except: 
                            flt_rm_selling_price = ins_general_methods.rm_str_to_flt(lst_sell_line[1].strip())

                        """ Same logic as capturing service fee"""

                        if str_sell_line.find('/') != -1 :
                            str_pax_no_string = '/'.join(str_sell_line.split('/')[1:])
                            lst_pax_no_string = list(str_pax_no_string)
                            
                            str_value = ''
                            str_prev = ''
                            bln_pax_data = False #39957
                            bln_sector_data = False

                            for str_value in lst_pax_no_string :

                                if not str_prev :
                                    #if first iteration
                                    str_prev = str_value
                                    if str_value == 'T' :
                                        bln_pax_data = True
                                        bln_sector_data = False
                                    elif str_value == 'S' :
                                        bln_sector_data = True
                                        bln_pax_data = False
                                    continue

                                if str_value == 'T' :
                                    bln_pax_data = True
                                    bln_sector_data = False
                                    str_prev = str_value
                                    continue
                                elif str_value == 'S' :
                                    bln_pax_data = False
                                    bln_sector_data = True
                                    str_prev = str_value
                                    continue

                                if bln_pax_data :

                                    if str_value.isdigit() and str_prev != '-' :
                                        lst_pax_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_pax_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_pax_nos.append(int_start)
                                            int_start += 1
                                            pass
                                    else :
                                        bln_pax_data = False

                                elif bln_sector_data :
                                    if str_value.isdigit() and str_prev != '-' :
                                        lst_sector_nos.append(int(str_value))
                                    elif str_value.isdigit() and str_prev == '-' :
                                        int_start = int(lst_sector_nos[-1]) + 1
                                        int_end = int(str_value)
                                        while (int_start<= int_end) :
                                            lst_sector_nos.append(int_start)
                                            int_start += 1
                                    pass
                                str_prev = str_value
                                pass

                    except:
                        try :
                            flt_rm_selling_price = ins_general_methods.rm_str_to_flt(str_sell_line.split('/')[0].strip())
                        except :
                            flt_rm_selling_price = 0.00

                    if lst_pax_nos :
                        for int_pax_no in lst_pax_nos :
                            ins_general_methods.ins_global.dct_pax_no_selling_price[int(int_pax_no)] = flt_rm_selling_price
                            
                        if 'ALL' in ins_general_methods.ins_global.dct_pax_no_selling_price :
                            ins_general_methods.ins_global.dct_pax_no_selling_price.pop('ALL')

                    elif not  ins_general_methods.ins_global.dct_pax_no_selling_price and flt_rm_selling_price:
                        ins_general_methods.ins_global.dct_pax_no_selling_price['ALL'] = flt_rm_selling_price
                        pass
                    if lst_sector_nos :
                        for int_seg_no in lst_sector_nos :
                            ins_general_methods.ins_global.dct_seg_no_selling_price[int(int_seg_no)] = flt_rm_selling_price
                        if 'ALL' in ins_general_methods.ins_global.dct_seg_no_selling_price :
                            ins_general_methods.ins_global.dct_seg_no_selling_price.pop('ALL')
                            
                    elif not ins_general_methods.ins_global.dct_seg_no_selling_price and flt_rm_selling_price:
                        ins_general_methods.ins_global.dct_seg_no_selling_price['ALL'] = flt_rm_selling_price

                    if flt_rm_selling_price :
                        ins_capture_ticket_data.flt_rm_collection_amount = flt_rm_selling_price

                    ins_capture_ticket_data.flt_selling_price_ext = ins_capture_ticket_data.flt_rm_collection_amount
                    ins_capture_ticket_data.bln_adt_selling_price = True
                    
                elif 'PARTY_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_CODE'][0]):
                    ins_capture_ticket_data.str_rm_customer_code = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_CODE'][0],'')[:50].split(';')[0].strip().strip('\n').strip() #...refer #18723
                    ins_capture_ticket_data.str_party_code = ins_capture_ticket_data.str_rm_customer_code
                    ins_capture_emd_ticket_data.str_rm_customer_code = ins_capture_ticket_data.str_rm_customer_code
                    ins_capture_emd_ticket_data.str_party_code = ins_capture_ticket_data.str_rm_customer_code
  
                elif 'AGENCY_DEPARTMENT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][0]):
                    ins_capture_ticket_data.str_branch_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][0],'').replace(str_seperator,'')[:30].strip().strip('\n').strip().split(';')[0]
                    ins_capture_emd_ticket_data.str_branch_code = ins_capture_ticket_data.str_branch_code

                elif 'CUST_ENGAGEMENT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][0]):
                    ins_capture_ticket_data.str_cust_engagement_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_engagement_code = ins_capture_ticket_data.str_cust_engagement_code

                elif 'CUST_RESOURCE_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][0]):
                    ins_capture_ticket_data.str_cust_resource_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_resource_code = ins_capture_ticket_data.str_cust_resource_code

                elif 'CUST_COMMITMENT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][0]):
                    ins_capture_ticket_data.str_cust_commitment_no = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_commitment_no = ins_capture_ticket_data.str_cust_commitment_no

                elif 'CUST_ACCOUNTING_UNIT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][0]):
                    ins_capture_ticket_data.str_cust_accounting_unit = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_accounting_unit = ins_capture_ticket_data.str_cust_accounting_unit

                elif 'CUST_INTERNAL_AC_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][0]):
                    ins_capture_ticket_data.str_cust_internal_ac_no = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_internal_ac_no = ins_capture_ticket_data.str_cust_internal_ac_no

                elif 'CUST_PROJECT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][0]):
                    ins_capture_ticket_data.str_cust_project_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_project_code = ins_capture_ticket_data.str_cust_project_code

                elif 'CUST_ACTION_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][0]):
                    ins_capture_ticket_data.str_cust_action_no = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_action_no = ins_capture_ticket_data.str_cust_action_no

                elif 'CUST_APPROVER_NAME' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][0]):
                    ins_capture_ticket_data.str_cust_approver_name = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_approver_name = ins_capture_ticket_data.str_cust_approver_name

                elif 'CUST_APPROVER_EMAIL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][0]):
                    ins_capture_ticket_data.str_cust_approver_email = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_approver_email = ins_capture_ticket_data.str_cust_approver_email

                elif 'CUST_EMPLOYEE_GRADE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][0]):
                    ins_capture_ticket_data.str_cust_employee_grade = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_employee_grade = ins_capture_ticket_data.str_cust_employee_grade

                elif 'CUST_COST_CENTRE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][0]):
                    ins_capture_ticket_data.str_cust_cost_centre = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_cost_centre = ins_capture_ticket_data.str_cust_cost_centre

                elif 'CUST_DEPARTMENT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][0]):
                    ins_capture_ticket_data.str_cust_department = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_department = ins_capture_ticket_data.str_cust_department

                elif 'CUST_SUB_CUSTOMER_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][0]):
                    ins_capture_ticket_data.str_sub_customer_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_sub_customer_code = ins_capture_ticket_data.str_sub_customer_code

                elif 'PARTY_MULTIPLE_FOP_YES_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][0]):
                    ins_capture_ticket_data.str_party_multiple_fop_yes_no = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_party_multiple_fop_yes_no = ins_capture_ticket_data.str_party_multiple_fop_yes_no

                elif 'PARTY_ADDITIONAL_AR' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][0]):
                    ins_capture_ticket_data.str_party_additional_ar = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][0],'').replace(str_seperator,'')[:150].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_party_additional_ar = ins_capture_ticket_data.str_party_additional_ar

                elif 'PARTY_MAXIMUM_TICKETS' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][0]):
                    try :
                        int_party_maximum_tickets = int(str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip())
                    except :
                        int_party_maximum_tickets = 0

                    ins_capture_ticket_data.int_party_maximum_tickets = int_party_maximum_tickets
                    ins_capture_emd_ticket_data.int_party_maximum_tickets = int_party_maximum_tickets
                    ins_capture_ticket_data.int_tickets_count_in_inv = int_party_maximum_tickets
                    ins_capture_emd_ticket_data.int_tickets_count_in_inv = int_party_maximum_tickets

                elif 'PARTY_FILE_JOB_CARD_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][0]):
                    ins_capture_ticket_data.str_party_file_job_card_no = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_party_file_job_card_no = ins_capture_ticket_data.str_party_file_job_card_no

                elif 'AGENCY_SALES_MAN' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][0]):
                    ins_capture_ticket_data.str_agency_sales_man = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_agency_sales_man = ins_capture_ticket_data.str_agency_sales_man

                elif 'AGENCY_TRAACS_USER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][0]):
                    ins_capture_ticket_data.str_agency_traacs_user = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_agency_traacs_user = ins_capture_ticket_data.str_agency_traacs_user

                elif 'AGENCY_ADV_RECEIPT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][0]):
                    ins_capture_ticket_data.str_agency_adv_receipt_no = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_agency_adv_receipt_no = ins_capture_ticket_data.str_agency_adv_receipt_no

                elif 'AGENCY_PRODUCT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][0]):
                    ins_capture_ticket_data.str_agency_product_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_agency_product_code = ins_capture_ticket_data.str_agency_product_code

                elif 'AGENCY_SUB_PRODUCT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_SUB_PRODUCT_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_SUB_PRODUCT_CODE'][0]):
                    ins_capture_ticket_data.str_agency_sub_product_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_SUB_PRODUCT_CODE'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_agency_sub_product_code = ins_capture_ticket_data.str_agency_sub_product_code

                elif 'AGENCY_AUTO_INVOICE_YES_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][0]):
                    ins_capture_ticket_data.str_agency_auto_invoice_yes_no = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][0],'').replace(str_seperator,'')[:4].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_agency_auto_invoice_yes_no = ins_capture_ticket_data.str_agency_auto_invoice_yes_no

                elif 'FARE_PUBLISHED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][0]):
                    ins_capture_ticket_data.bln_published_fare_adt = True
                    ins_capture_emd_ticket_data.bln_published_fare_adt = True
                    try:
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                            str_starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_published_fare_sector_wise[str_starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        else:
                            ins_capture_ticket_data.flt_published_fare_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                            ins_capture_emd_ticket_data.flt_published_fare_ext = ins_capture_ticket_data.flt_published_fare_ext
                    except:
                        ins_capture_ticket_data.flt_published_fare_ext = 0.0
                        ins_capture_emd_ticket_data.flt_published_fare_ext = 0.0

                elif 'FARE_PUBLISHED_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][0]):
                    ins_capture_ticket_data.bln_published_fare_chd = True
                    ins_capture_emd_ticket_data.bln_published_fare_chd = True
                    try:
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                            str_starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_child_published_fare_sector_wise[str_starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        else:
                            ins_capture_ticket_data.flt_published_fare_chd = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                            ins_capture_emd_ticket_data.flt_published_fare_chd = ins_capture_ticket_data.flt_published_fare_chd
                    except:
                        ins_capture_ticket_data.flt_published_fare_chd = 0.0
                        ins_capture_emd_ticket_data.flt_published_fare_chd = 0.0
                        
                elif 'FARE_PUBLISHED_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][0]):
                    ins_capture_ticket_data.bln_published_fare_inf = True
                    ins_capture_emd_ticket_data.bln_published_fare_inf = True
                    try:
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                            str_starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_infant_published_fare_sector_wise[str_starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        else:
                            ins_capture_ticket_data.flt_published_fare_inf = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                            ins_capture_emd_ticket_data.flt_published_fare_inf = ins_capture_ticket_data.flt_published_fare_inf
                    except:
                        ins_capture_ticket_data.flt_published_fare_inf = 0.0
                        ins_capture_emd_ticket_data.flt_published_fare_inf = 0.0

                elif 'FARE_ORIGINAL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][0]):
                    try:
                        ins_capture_ticket_data.flt_original_fare_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_original_fare_ext = ins_capture_ticket_data.flt_original_fare_ext
                    except:
                        ins_capture_ticket_data.flt_original_fare_ext = 0.0
                        ins_capture_emd_ticket_data.flt_original_fare_ext = 0.0

                elif 'FARE_PRINTING' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PRINTING'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PRINTING'][0]):
                    try:
                        ins_capture_ticket_data.flt_printing_fare_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PRINTING'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_printing_fare_ext = ins_capture_ticket_data.flt_printing_fare_ext
                    except:
                        ins_capture_ticket_data.flt_printing_fare_ext = 0.0
                        ins_capture_emd_ticket_data.flt_printing_fare_ext = 0.0

                elif 'FARE_EXTRA_EARNING' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][0]):
                    ins_capture_ticket_data.bln_extra_earning_adt = True
                    ins_capture_emd_ticket_data.bln_extra_earning_adt = True
                    try:
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                            starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_extra_earning_sector_wise[starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip()) 
                        else:
                            ins_capture_ticket_data.flt_extra_earning_inv = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                            ins_capture_emd_ticket_data.flt_extra_earning_inv = ins_capture_ticket_data.flt_extra_earning_inv
                    except:
                        ins_capture_ticket_data.flt_extra_earning_inv = 0.0
                        ins_capture_emd_ticket_data.flt_extra_earning_inv = 0.0

                elif 'FARE_EXTRA_EARNING_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][0]):
                    ins_capture_ticket_data.bln_extra_earning_chd = True
                    ins_capture_emd_ticket_data.bln_extra_earning_chd = True
                    try:
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                            starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_child_extra_earning_sector_wise[starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        else:
                            ins_capture_ticket_data.flt_extra_earning_chd = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                            ins_capture_emd_ticket_data.flt_extra_earning_chd = ins_capture_ticket_data.flt_extra_earning_chd
                    except:
                        ins_capture_ticket_data.flt_extra_earning_chd = 0.0
                        ins_capture_emd_ticket_data.flt_extra_earning_chd = 0.0
                        
                elif 'FARE_EXTRA_EARNING_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][0]):
                    ins_capture_ticket_data.bln_extra_earning_inf = True
                    ins_capture_emd_ticket_data.bln_extra_earning_inf = True
                    try:
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                            starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise[starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        else:
                            ins_capture_ticket_data.flt_extra_earning_inf = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                            ins_capture_emd_ticket_data.flt_extra_earning_inf = ins_capture_ticket_data.flt_extra_earning_inf
                    except:
                        ins_capture_ticket_data.flt_extra_earning_inf = 0.0
                        ins_capture_emd_ticket_data.flt_extra_earning_inf = 0.0

                elif 'FARE_PAYBACK_COMMISSION' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][0]):
                    try:
                        ins_capture_ticket_data.flt_payback_commission_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_payback_commission_ext = ins_capture_ticket_data.flt_payback_commission_ext
                    except:
                        ins_capture_ticket_data.flt_payback_commission_ext = 0.0
                        ins_capture_emd_ticket_data.flt_payback_commission_ext = 0.0

                elif 'FARE_CC_CHARGE_COLLECTED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][0]):
                    try:
                        ins_capture_ticket_data.flt_cc_charge_collected_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_cc_charge_collected_ext = ins_capture_ticket_data.flt_cc_charge_collected_ext
                    except:
                        ins_capture_ticket_data.flt_cc_charge_collected_ext = 0.0
                        ins_capture_emd_ticket_data.flt_cc_charge_collected_ext = 0.0
                #45305        
                elif 'FARE_CC_CHARGE_COLLECTED_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][0]):  #45305
                    ins_capture_ticket_data.bln_cc_charge_collected_chd = True
                    ins_capture_emd_ticket_data.bln_cc_charge_collected_chd = True
                    try:
                        ins_capture_ticket_data.flt_cc_charge_collected_chd = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_cc_charge_collected_chd = ins_capture_ticket_data.flt_cc_charge_collected_chd
                    except:
                        ins_capture_ticket_data.flt_cc_charge_collected_chd = 0.0
                        ins_capture_emd_ticket_data.flt_cc_charge_collected_chd = 0.0
                        
                elif 'FARE_CC_CHARGE_COLLECTED_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][0]):
                    ins_capture_ticket_data.bln_cc_charge_collected_inf = True
                    ins_capture_emd_ticket_data.bln_cc_charge_collected_inf = True
                    try:
                        ins_capture_ticket_data.flt_cc_charge_collected_inf = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_cc_charge_collected_inf = ins_capture_ticket_data.flt_cc_charge_collected_inf
                    except:
                        ins_capture_ticket_data.flt_cc_charge_collected_inf = 0.0
                        ins_capture_emd_ticket_data.flt_cc_charge_collected_inf = 0.0

                elif 'FARE_LOWEST_OFFERED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][0]):
                    try:
                        ins_capture_ticket_data.flt_lowest_offered_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_lowest_offered_ext = ins_capture_ticket_data.flt_lowest_offered_ext
                    except:
                        ins_capture_ticket_data.flt_lowest_offered_ext = 0.0
                        ins_capture_emd_ticket_data.flt_lowest_offered_ext = 0.0

                elif 'FARE_REASON_FOR_CHOOSE_HIGHER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][0]):

                    ins_capture_ticket_data.str_reason_for_choose_higher_ext = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][0],'').replace(str_seperator,'').split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_reason_for_choose_higher_ext = ins_capture_ticket_data.str_reason_for_choose_higher_ext

                elif 'FARE_ACCEPTED_OR_PAID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][0]):
                    try:
                        ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_fare_accepted_or_paid_ext = ins_capture_ticket_data.flt_fare_accepted_or_paid_ext
                    except:
                        ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = 0.0
                        ins_capture_emd_ticket_data.flt_fare_accepted_or_paid_ext = 0.0

                elif 'COMPLIANCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['COMPLIANCE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['COMPLIANCE'][0]):
                    ins_capture_ticket_data.str_compliance_ext = str_line.replace(ins_general_methods.dct_capturing_settings['COMPLIANCE'][0],'').replace(str_seperator,'')[:30].split('/')[0].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_compliance_ext = ins_capture_ticket_data.str_compliance_ext

                elif 'CORPORATE_CARD_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][0]):
                    ins_capture_ticket_data.str_corp_card_code_ext = str_line.replace(ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][0],'').replace(str_seperator,'')[:30].split('/')[0].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_corp_card_code_ext = ins_capture_ticket_data.str_corp_card_code_ext

                elif 'AGAINST_DOCUMENT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][0]):
                    ins_capture_ticket_data.str_against_doc_ext = str_line.replace(ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][0],'').replace(str_seperator,'')[:30].split('/')[0].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_against_doc_ext = ins_capture_ticket_data.str_against_doc_ext

                elif 'PNR_TYPE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PNR_TYPE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PNR_TYPE'][0]):
                    ins_capture_ticket_data.str_pnr_type_ext = str_line.replace(ins_general_methods.dct_capturing_settings['PNR_TYPE'][0],'').replace(str_seperator,'')[:30].split('/')[0].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_pnr_type_ext = ins_capture_ticket_data.str_pnr_type_ext

                elif 'AGENCY_PAYBACK_ACCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][0]):
                    ins_capture_ticket_data.str_pay_back_account_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][0],'').replace(str_seperator,'')[:30].split('/')[0].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_pay_back_account_code = ins_capture_ticket_data.str_pay_back_account_code

                elif 'FARE_LOST_AMOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][0]):
                    try:
                        ins_capture_ticket_data.flt_fare_lost_amount_ext = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_fare_lost_amount_ext = ins_capture_ticket_data.flt_fare_lost_amount_ext
                    except:
                        ins_capture_ticket_data.flt_fare_lost_amount_ext = 0.0
                        ins_capture_emd_ticket_data.flt_fare_lost_amount_ext = 0.0

                elif 'FARE_DEAL_SHARING' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DEAL_SHARING'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DEAL_SHARING'][0]):
                    ins_capture_ticket_data.str_deal_sharing_string = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DEAL_SHARING'][0],'').replace(str_seperator,'')[:30].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_deal_sharing_string = ins_capture_ticket_data.str_deal_sharing_string

                elif 'FARE_SERVICE_FEE_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][0]):
                    ins_capture_ticket_data.bln_chd_svf = True
                    try:
                        # refer 30961
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                        
                            starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_child_service_fee_sector_wise[starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        else:
                            ins_capture_ticket_data.flt_service_fee_child = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
#                            ins_capture_emd_ticket_data.flt_service_fee_child = ins_capture_ticket_data.flt_service_fee_child
                    except:
                        ins_capture_ticket_data.flt_service_fee_child = 0.0
                        
                elif 'FARE_SERVICE_FEE_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][0]):
                    ins_capture_ticket_data.bln_inf_svf = True
                    try:
                        # refer 30961
                        if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                            starting_segment = str_line.split('$')[1][:3]
                            ins_general_methods.ins_global.dct_infant_service_fee_sector_wise[starting_segment] = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        else:
                            ins_capture_ticket_data.flt_service_fee_infant = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
#                            ins_capture_emd_ticket_data.flt_service_fee_infant = ins_capture_ticket_data.flt_service_fee_infant
                    except:
                        ins_capture_ticket_data.flt_service_fee_infant = 0.0
                        
                elif 'FARE_CHD_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CHD_SELLING_PRICE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_CHD_SELLING_PRICE'][0]):
                    ins_capture_ticket_data.bln_chd_selling_price = True
                    ins_capture_emd_ticket_data.bln_chd_selling_price = True
                    try:
                        ins_capture_ticket_data.flt_selling_price_child = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_CHD_SELLING_PRICE'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_selling_price_child = ins_capture_ticket_data.flt_selling_price_child
                    except:
                        ins_capture_ticket_data.flt_selling_price_child = 0.0
                        ins_capture_emd_ticket_data.flt_selling_price_child = 0.0
                        
                elif 'FARE_INF_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_INF_SELLING_PRICE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_INF_SELLING_PRICE'][0]):
                    ins_capture_ticket_data.bln_inf_selling_price = True
                    ins_capture_emd_ticket_data.bln_inf_selling_price = True
                    try:
                        ins_capture_ticket_data.flt_selling_price_infant = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_INF_SELLING_PRICE'][0],'').replace(str_seperator,'').replace(self.str_defult_currency_code,'').split('/')[0].split(';')[0].strip().strip('\n').strip())
                        ins_capture_emd_ticket_data.flt_selling_price_infant = ins_capture_ticket_data.flt_selling_price_infant
                    except:
                        ins_capture_ticket_data.flt_selling_price_infant = 0.0
                        ins_capture_emd_ticket_data.flt_selling_price_infant = 0.0
                        
                elif 'OPTION_1' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['OPTION_1'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['OPTION_1'][0]):
                    ins_capture_ticket_data.str_quot_option_1 = str_line.replace(ins_general_methods.dct_capturing_settings['OPTION_1'][0],'').replace(str_seperator,'')[:49].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_quot_option_1 = ins_capture_ticket_data.str_agency_sub_product_code
      
                elif 'OPTION_2' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['OPTION_2'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['OPTION_2'][0]):
                    ins_capture_ticket_data.str_quot_option_2 = str_line.replace(ins_general_methods.dct_capturing_settings['OPTION_2'][0],'').replace(str_seperator,'')[:49].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_quot_option_2 = ins_capture_ticket_data.str_agency_sub_product_code
                
                elif 'CUST_POS_ID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_POS_ID'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_POS_ID'][0]): # refer 20879
                        ins_capture_ticket_data.int_credit_card_pos_id = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_POS_ID'][0],'').replace(str_seperator,'')[:50].split(';')[0].strip().strip('\n').strip() 

                elif 'CUST_CC_NUMBER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][0]): # refer 20879
                        ins_capture_ticket_data.str_cc_number = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][0],'')[:50].split(';')[0].strip().strip('\n').strip()
               
                elif 'CRM_REFERENCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][0]): # refer 23236
                        ins_capture_ticket_data.str_crm_reference = str_line.replace(ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][0],'')[:500].split(';')[0].strip().strip('\n').strip()
                        ins_capture_emd_ticket_data.str_crm_reference = ins_capture_ticket_data.str_crm_reference
                        
                elif 'MASTER_REFERENCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][0]):
                    ins_capture_ticket_data.str_master_reference = str_line.replace(ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][0],'').replace(str_seperator,'')[:49].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_master_reference = ins_capture_ticket_data.str_master_reference
                
                elif 'MASTER_NARRATION' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][0]):
                    ins_capture_ticket_data.str_master_narration = str_line.replace(ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][0],'').replace(str_seperator,'')[:49].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_master_narration = ins_capture_ticket_data.str_master_narration
                
                elif 'LPO_DATE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['LPO_DATE'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['LPO_DATE'][0]):
                    #  # Refer 34857
                    ins_capture_ticket_data.dat_rm_lpo_date = str_line.replace(ins_general_methods.dct_capturing_settings['LPO_DATE'][0],'').replace(str_seperator,'')[:49].split(';')[0].strip().strip('\n').strip()
                    ins_capture_ticket_data.dat_rm_lpo_date = ins_general_methods.validate_and_convert_format_of_date(ins_capture_ticket_data.dat_rm_lpo_date)
                    ins_capture_emd_ticket_data.dat_rm_lpo_date = ins_capture_ticket_data.dat_rm_lpo_date
                
                elif 'PASSIVE_SEGMENTS' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PASSIVE_SEGMENTS'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['PASSIVE_SEGMENTS'][0]): #43745
                    str_lcc_number = str_line.replace(ins_general_methods.dct_capturing_settings['PASSIVE_SEGMENTS'][0],'').replace(str_seperator,'')[:200].split('/')[0].strip().strip('\n').strip()
                    ins_capture_ticket_data.lst_lcc_ticket_voucher_number.append(str_lcc_number)  
                    
                elif 'CUST_TRAVELLER_ID_LOYALTY_ID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][0] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][0]):
                    #45745
                    ins_capture_ticket_data.str_cust_traveller_id = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][0],'').replace(str_seperator,'')[:49].split(';')[0].strip().strip('\n').strip()
                    ins_capture_emd_ticket_data.str_cust_traveller_id = ins_capture_ticket_data.str_cust_traveller_id
                
                elif str_line[:3] == "END":
                    lst_ticket_capture_details = []
                    lst_ticket_emd_capture_details = []
                    # refer 22375
                    if not bln_has_t_section and not ins_capture_ticket_data.lst_ticket_detls and ins_capture_ticket_data.lst_refund_ticket_detls:
                        ins_capture_ticket_data.lst_ticket_detls = ins_capture_ticket_data.lst_refund_ticket_detls
                        
                    if ins_capture_ticket_data.bln_xo_capturing and \
                        ins_general_methods.ins_auto_inv.bln_xo_capture and \
                        not ins_capture_ticket_data.lst_ticket_detls and \
                        not ins_capture_ticket_data.lst_refund_ticket_detls:

                        if not ins_capture_ticket_data.lst_sector:
                            ins_capture_ticket_data.lst_sector = ins_capture_ticket_data.lst_u_section_sector
                            str_sector = '/'.join(ins_capture_ticket_data.lst_u_section_sector)
                            str_sector = ins_capture_ticket_data.str_start_port_code + '/' + str_sector

                        int_count = 1
                        

                        for lst_pax in ins_capture_ticket_data.lst_pax_names:

                            ins_capture_ticket_data.str_ticket_number = ins_capture_ticket_data.str_pnr_no +"-" + str(int_count)

                            ins_capture_ticket_data.str_pax_name = lst_pax[0]
                            ins_capture_ticket_data.str_pax_type = lst_pax[1]
                            
                            ins_capture_ticket_data.lst_ticket_detls.append([ins_capture_ticket_data.str_ticket_number,
                                                                         '',
                                                                         'DEFAULT',
                                                                         'DEFAULT',
                                                                         'DEFAULT',
                                                                         ins_capture_ticket_data.str_pax_name,
                                                                         ins_capture_ticket_data.str_pax_type,
                                                                         ins_capture_ticket_data.flt_service_charge,
                                                                         '',
                                                                         '',
                                                                         ins_capture_ticket_data.int_pax_item_number
                                                                         ])

                            int_count += 1

                    else:
                        ins_capture_ticket_data.bln_xo_capturing = False
                    
                    lst_ticket_capture_details , lst_tickets = ins_create_ticket_base.create_ticket_data_to_save(ins_capture_ticket_data , str_file)
                    lst_ticket_emd_capture_details , lst_emd_tickets = ins_create_ticket_base.create_ticket_data_to_save(ins_capture_emd_ticket_data , str_file, lst_emd_tickets = lst_emd_tickets)
                    lst_hv,lst_cv,lst_os = ins_create_ticket_base.create_voucher_data_to_save(ins_capture_ticket_data , str_file)
                    
                    lst_keys = list(ins_general_methods.ins_global.dct_pax_no_service_fee.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_pax_no_service_fee.pop(key)

                    lst_keys = list(ins_general_methods.ins_global.dct_seg_no_service_fee.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_seg_no_service_fee.pop(key)
                        
                    ## Refer #14792
                    lst_keys = list(ins_general_methods.ins_global.dct_seg_no_selling_price.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_seg_no_selling_price.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_pax_no_selling_price.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_pax_no_selling_price.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_seg_no_voucher_supplier_amount.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_seg_no_voucher_supplier_amount.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_seg_no_voucher_selling_price.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_seg_no_voucher_selling_price.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_seg_no_voucher_discount.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_seg_no_voucher_discount.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_seg_no_voucher_tax.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_seg_no_voucher_tax.pop(key)
                        
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_seg_no_voucher_supplier.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_seg_no_voucher_supplier.pop(key)
                        
                     # refer 30961
                    lst_keys = list(ins_general_methods.ins_global.dct_service_fee_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_service_fee_sector_wise.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_child_service_fee_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_child_service_fee_sector_wise.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_infant_service_fee_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_infant_service_fee_sector_wise.pop(key)
                    # refer 34029    
                    lst_keys = list(ins_general_methods.ins_global.dct_extra_earning_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_extra_earning_sector_wise.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_child_extra_earning_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_child_extra_earning_sector_wise.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_published_fare_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_published_fare_sector_wise.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_child_published_fare_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_child_published_fare_sector_wise.pop(key)
                        
                    lst_keys = list(ins_general_methods.ins_global.dct_infant_published_fare_sector_wise.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_infant_published_fare_sector_wise.pop(key)
                    
                    #38116
                    lst_keys = list(ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector.keys())
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector.pop(key)    
                        
                    if ( lst_ticket_capture_details not in (None,[],[None]) or\
                        lst_ticket_emd_capture_details not in (None,[],[None]) ) and\
                            (ins_capture_ticket_data.bln_IMR_file or ins_capture_emd_ticket_data.bln_IMR_file) and\
                                (not ins_capture_ticket_data.bln_xo_capturing and not ins_general_methods.ins_auto_inv.bln_xo_capture):    
                        
                        
                        lst_ticket_capture_details = lst_tickets = []
                        lst_ticket_emd_capture_details = lst_emd_tickets = []
                        
                        if not (lst_hv or lst_cv or lst_os) :
                            print(("Skipping IMR file " + str_file))
                            raise SkipTicketException("Skip IMR File ")
                        
                    try:
                        str_message = ''
                        
                        if lst_ticket_capture_details not in (None,[],[None]):
                            if ins_capture_ticket_data.bln_xo_capturing and ins_general_methods.ins_auto_inv.bln_xo_capture: # refer 25579
                                ins_save_or_update_data.save_booking_ticket_data(lst_ticket_capture_details)
                                #there is a chance for change in DB instance Due to DB reload from client. so confirm whether the same instance is commited or not.
                                if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                    ins_general_methods.ins_db.commit()
                                else:
                                    ins_general_methods.ins_db.rollback()
                                    raise Exception('Database instance commit failed')
                                
                                if lst_tickets:
                                    print(('Saved PNR Tickets ' + ', '.join(lst_tickets)))
                                    str_message += 'Saved PNR Tickets ' + ', '.join(lst_tickets)
                            else:
                                ins_save_or_update_data.save_captured_ticket_data(lst_ticket_capture_details)
                                
                                if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                    ins_general_methods.ins_db.commit()
                                else:
                                    ins_general_methods.ins_db.rollback()
                                    raise Exception('Database instance commit failed')
                                
                                if lst_tickets:
                                    print(('Saved Amadeus Tickets ' + ', '.join(lst_tickets)))
                                    str_message += '\n' + 'Saved Amadeus Tickets ' + ', '.join(lst_tickets)
                                for ins_ticket_base in ins_general_methods.ins_global.lst_process_list :
                                    thread = Thread(target = ins_save_or_update_data.create_json_and_upload,args = [ins_ticket_base])
                                    thread.start()
                                    thread.join()

                                for ins_ticket_base in ins_general_methods.ins_global.lst_process_list :
                                    ins_general_methods.ins_global.lst_process_list.remove(ins_ticket_base)
                                
                                #37364    
                                if ins_general_methods.bln_enable_non_iata_capture :    
                                    for ins_ticket_base in ins_general_methods.ins_global.lst_process_list_void :
                                        thread = Thread(target = ins_save_or_update_data.create_void_json_and_upload,args = [ins_ticket_base])
                                        thread.start()
                                        thread.join()

                                    for ins_ticket_base in ins_general_methods.ins_global.lst_process_list_void :
                                        ins_general_methods.ins_global.lst_process_list_void.remove(ins_ticket_base)

                        lst_hotel_vouchers = []
                        if lst_hv not in (None,[],[None]):
                            lst_hotel_vouchers = ins_save_or_update_data.save_captured_hotel_voucher_data(lst_hv)
                            if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                    ins_general_methods.ins_db.commit()
                            else:
                                ins_general_methods.ins_db.rollback()
                                raise Exception('Database instance commit failed')
                            
                            if lst_hotel_vouchers:
                                print(('Saved Hotel Vouchers ' + ', '.join(lst_hotel_vouchers)))
                                str_message += '\n' + 'Saved Hotel Vouchers ' + ', '.join(lst_hotel_vouchers)
                                
                        if lst_cv not in (None,[],[None]): #40359
                            lst_car_vouchers = ins_save_or_update_data.save_captured_car_voucher_data(lst_cv)
                            if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                    ins_general_methods.ins_db.commit()
                            else:
                                ins_general_methods.ins_db.rollback()
                                raise Exception('Database instance commit failed')
                            
                            if lst_car_vouchers:
                                print(('Saved Car Vouchers ' + ', '.join(lst_car_vouchers)))
                                str_message += '\n' + 'Saved Car Vouchers ' + ', '.join(lst_car_vouchers)
                        lst_other_service_details = []
                        if lst_os not in (None,[],[None]):
                            lst_other_service_details =  ins_save_or_update_data.save_captured_other_service_voucher_data(lst_os)
                            if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                    ins_general_methods.ins_db.commit()
                            else:
                                ins_general_methods.ins_db.rollback()
                                raise Exception('Database instance commit failed')
                            
                            if lst_other_service_details:
                                print(('Saved service ' + ', '.join(lst_other_service_details)))
                                str_message += '\n' + 'Saved service ' + ', '.join(lst_other_service_details)

                        if lst_ticket_emd_capture_details not in (None,[],[None]):
                            ins_save_or_update_data.save_captured_ticket_data(lst_ticket_emd_capture_details)
                            ins_general_methods.ins_db.commit()
                            lst_tsm_number.append(ins_capture_emd_ticket_data.str_tsm_number)

                            ins_capture_emd_ticket_data.int_tickets_count = ins_capture_emd_ticket_data.int_tickets_count - 1
                            #// if the file contains more than one ticket, then parse the file again.
                            if ins_capture_emd_ticket_data.int_tickets_count > 0:
                                self.extract_ticket_data_from_file(str_file , lst_tsm_number = lst_tsm_number , bln_emd_ticket = True, lst_emd_tickets = lst_emd_tickets,bln_from_web_service = bln_from_web_service,str_file_content = str_file_content,int_count = int_count)
                                pass

                            if lst_emd_tickets:
                                print(('Saved EMD Tickets ' + ', '.join(lst_emd_tickets)))
                                str_message += '\n' + 'Saved EMD Tickets ' + ', '.join(lst_emd_tickets)
                            pass
                        
                            for ins_ticket_base in ins_general_methods.ins_global.lst_process_list :
                                thread = Thread(target = ins_save_or_update_data.create_json_and_upload,args = [ins_ticket_base])
                                thread.start()
                                thread.join()

                            for ins_ticket_base in ins_general_methods.ins_global.lst_process_list :
                                ins_general_methods.ins_global.lst_process_list.remove(ins_ticket_base)

                            pass

                        #43745
                        lst_lcc_no = []
                        if ins_capture_ticket_data.lst_lcc_ticket_voucher_number:
                            lst_lcc_no = ins_general_methods.update_lcc_ticket_voucher_remarks_data(ins_capture_ticket_data, ins_capture_ticket_data.lst_lcc_ticket_voucher_number,os.path.split(str_file)[-1])
                            if lst_lcc_no:
                                print ('Updated LCC Ticket/Voucher ' + ', '.join(lst_lcc_no))
                                str_message += '\n' + 'Saved/Updated LCC Ticket/Voucher ' + ', '.join(lst_lcc_no)
                            else:
                                print ('No LCC Ticket/Voucher')
                                str_message += '\n' + 'No LCC Ticket/Voucher'
                        
                        if lst_ticket_capture_details in (None,[],[None]) and lst_ticket_emd_capture_details in (None,[],[None]) and lst_hv in (None,[],[None]) and lst_cv in (None,[],[None]) and lst_os in (None,[],[None]) and not ins_capture_ticket_data.lst_ins_voucher and not lst_lcc_no:
                            if not ins_capture_ticket_data.bln_xo_capturing:
                                ins_general_methods.ins_db.rollback()
                                ins_general_methods.ins_global.dct_no_ticket_files[str_file] = 'No ticket No'
                                raise OperationalError('No Data')

#                        if self.bln_refund_void:
                        #43239 - Refund Cancellation
                        if ins_capture_ticket_data.str_void_date and ins_capture_ticket_data.bln_refund:
                            try: # Refer Bug No : 10781 ,  while capture a refund voiding file , clear the refund section in table ticket
                                for ins_refund_void in lst_ticket_capture_details:
                                    ins_save_or_update_data.clear_tickets_refund_data(ins_refund_void.str_ticket_number)
                            except:
                                ins_general_methods.ins_db.rollback()
                                print ('Refund Cancellation skipped.')
                                str_message += '\n' + 'Refund Cancellation skipped.'
                            else:
                                if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                    ins_general_methods.ins_db.commit()
                                else:
                                    ins_general_methods.ins_db.rollback()
                                    raise Exception('Database instance commit failed')

                                print ('Refund Cancelled - ' + lst_ticket_capture_details[0].str_ticket_number )
                                str_message += '\n' + 'Refund Cancelled: ' + lst_ticket_capture_details[0].str_ticket_number
                        
                        #save auto invoice
                        ins_save_or_update_data.save_auto_invoice_data(ins_capture_ticket_data, lst_tickets, lst_ticket_capture_details, lst_emd_tickets, lst_ticket_emd_capture_details, lst_hotel_vouchers, lst_other_service_details)
        
                        
                    except InputError:
                        ins_general_methods.ins_db.rollback()
                        raise InputError
                    except:
                        ins_general_methods.ins_db.rollback()
                        raise
                    pass
                for code in ins_general_methods.dct_amadeus_optional_fields:

                    if code and str_line[:len(code)]== code :
                        if ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS' :
                            if isinstance(ins_general_methods.dct_amadeus_optional_fields[code], list) and \
                                    ins_general_methods.dct_amadeus_optional_fields[code][0] == 'JSON': #43533
                                ins_capture_ticket_data.json_user_defined_remark[ins_general_methods.dct_amadeus_optional_fields[code][1]] = str_line[len(code):].split(';')[0].strip().upper()
                                ins_capture_emd_ticket_data.json_user_defined_remark = ins_capture_ticket_data.json_user_defined_remark
                                break
                            else:
                                setattr(ins_capture_ticket_data,ins_general_methods.dct_amadeus_optional_fields[code],str_line[len(code):].split(';')[0].strip().upper()[:50])
                                setattr(ins_capture_emd_ticket_data,ins_general_methods.dct_amadeus_optional_fields[code],str_line[len(code):].split(';')[0].strip().upper()[:50])   #42136
                        else :
                            ins_capture_ticket_data.dct_extra_capturing_data.update({ins_general_methods.dct_amadeus_optional_fields[code] : str_line[len(code):].strip().upper()})
                        break

                        
                if 'CUSTOMER CODE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['CUSTOMER CODE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['CUSTOMER CODE'][0]):
                    ins_capture_ticket_data.str_airplus_cust_code = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['CUSTOMER CODE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'COST CENTRE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['COST CENTRE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['COST CENTRE'][0]):
                    ins_capture_ticket_data.str_airplus_cost_center = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['COST CENTRE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'PROJECT CODE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['PROJECT CODE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['PROJECT CODE'][0]):
                    ins_capture_ticket_data.str_airplus_project_code = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['PROJECT CODE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'EMPLOYEE ID' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['EMPLOYEE ID'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['EMPLOYEE ID'][0]):
                    ins_capture_ticket_data.str_airplus_employee_id = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['EMPLOYEE ID'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'DEPARTMENT CODE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['DEPARTMENT CODE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['DEPARTMENT CODE'][0]):
                    ins_capture_ticket_data.str_airplus_dept_code = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['DEPARTMENT CODE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'ACTION NO' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['ACTION NO'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['ACTION NO'][0]):
                    ins_capture_ticket_data.str_airplus_action_no = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['ACTION NO'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'INTERNAL ACCOUNT NO' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['INTERNAL ACCOUNT NO'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['INTERNAL ACCOUNT NO'][0]):
                    ins_capture_ticket_data.str_airplus_internal_account_no = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['INTERNAL ACCOUNT NO'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'SECRETARY' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['SECRETARY'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['SECRETARY'][0]):
                    ins_capture_ticket_data.str_airplus_secretary = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['SECRETARY'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'TRIP REASON' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['TRIP REASON'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['TRIP REASON'][0]):
                    ins_capture_ticket_data.str_airplus_trip_reason = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['TRIP REASON'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'CARD EXPIRY DATE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['CARD EXPIRY DATE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['CARD EXPIRY DATE'][0]):
                    ins_capture_ticket_data.str_airplus_card_expiry_date = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['CARD EXPIRY DATE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'RESOURCE CODE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['RESOURCE CODE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['RESOURCE CODE'][0]):
                    ins_capture_ticket_data.str_airplus_resource_code = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['RESOURCE CODE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'LPO NO' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['LPO NO'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['LPO NO'][0]):
                    ins_capture_ticket_data.str_airplus_lpo_no = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['LPO NO'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'PA' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['PA'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['PA'][0]):
                    ins_capture_ticket_data.str_airplus_pa = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['PA'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'SERVICE FEE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['SERVICE FEE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['SERVICE FEE'][0]):
                    ins_capture_ticket_data.flt_airplus_service_fee = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['SERVICE FEE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'CARD TYPE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['CARD TYPE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['CARD TYPE'][0]):
                    ins_capture_ticket_data.str_airplus_card_type = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['CARD TYPE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'ACCOUNTING UNIT' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['ACCOUNTING UNIT'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['ACCOUNTING UNIT'][0]):
                    ins_capture_ticket_data.str_airplus_accounting_unit = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['ACCOUNTING UNIT'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'DEPARTURE DATE' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['DEPARTURE DATE'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['DEPARTURE DATE'][0]):
                    ins_capture_ticket_data.str_airplus_departure_date = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['DEPARTURE DATE'][0],'').replace(str_seperator,'').strip()[:30]
                elif 'ORDER NO' in ins_general_methods.dct_airplus_capturing_settings and ins_general_methods.dct_airplus_capturing_settings['ORDER NO'][0] and str_line.startswith(ins_general_methods.dct_airplus_capturing_settings['ORDER NO'][0]):
                    ins_capture_ticket_data.str_airplus_order_no = str_line.replace(ins_general_methods.dct_airplus_capturing_settings['ORDER NO'][0],'').replace(str_seperator,'').strip()[:30]

                if str_line.startswith("RM") :
                    if str_line.strip() not in ins_capture_ticket_data.lst_rm_field_data :
                        ins_capture_ticket_data.lst_rm_field_data.append(str_line.strip())
                        ins_capture_emd_ticket_data.lst_rm_field_data.append(str_line.strip())
                
                #save auto invoice
#                ins_save_or_update_data.save_auto_invoice_data(ins_capture_ticket_data)
                
        except SkipTicketException as msg : 
            ins_general_methods.ins_db.rollback()
            raise SkipTicketException(msg)
        except:
            try:

                str_file_name = os.path.split(str_file)[1].split('.')[0]
                lst_option = self.get_details_of_file(str_file,bln_from_web_service = bln_from_web_service,str_file_content = str_file_content)
                if lst_option == 'Error':
                    if str_file_name + ':' not in dct_error_messages:
                        dct_error_messages[str(str_file_name) + ':'] = ['', '', '', 'Error while opening file']
                elif lst_option[1] != "F":
                    if str_file_name + ':' not in dct_error_messages:
                        dct_error_messages[str(str_file_name) + ':'] = ['', '', '', 'The GDS File Format is wrong']
                elif lst_option[0] != "T" and not ins_capture_ticket_data.bln_voucher_present_in_file:
                    if str_file_name + ':' not in dct_error_messages:
                        ins_general_methods.ins_global.dct_no_ticket_files[str_file] = 'No ticket No'
                        dct_error_messages[str(str_file_name) + ':'] = ['', lst_option[3], '', 'The GDS has no Ticket No']
                else:
                    for int_ticket_count in range(len(lst_option[2])):
                        str_file_name_ticket_num = str(str_file_name) + ':' + str(lst_option[2][int_ticket_count])
                        if str_file_name_ticket_num not in dct_error_messages:
                            str_line_no = str(sys.exc_info()[2].tb_lineno)
                            str_message = str(sys.exc_info()[1])
                            dct_error_messages[str_file_name_ticket_num] = [str(lst_option[2][int_ticket_count]), lst_option[3], str_line_no, str_message]
                            pass
            except :
                pass
            ins_general_methods.ins_db.rollback()
            if str_file in ins_general_methods.ins_global.dct_no_ticket_files :
                print('NO TICKET')
                raise NoticketException('NOTICKET')
            else :
                raise
           
        return str_message

    def get_fp_section_data(self, str_line):
        str_customer_code = ''
        str_lpo_number = ''
        flt_discount_given = 0.00
        flt_discount_given_per = 0.00
        flt_service_fee = 0.00
        flt_service_fee_per = 0.00
        str_card_approval_code = ''
        str_cc_card_no = ''
        str_cc_type = ''
        bln_partial_uccf = False
        flt_uccf_amount = 0.0
        flt_amount = 0.0
        lst_card_data = []
        flt_credit_amount = 0

        ### If the fp section is in the following format then we need to capture the customer code from this section , 
        ### In traacs premium this works based on an admin settings ,
        ### Here this settings is given in the conf file , if there is correct value set in conf file then the client code will be 
        ### captured from this.
        ### Refer #
        if ins_general_methods.ins_capture_base.bln_diif_fp_section :
            str_amadeus_air_file_fp_section_format = 'FPCASH-/FPINV CUSTOMER-LPO-DSP*/DSA*/SFP*/SFA*'
        else :
            str_amadeus_air_file_fp_section_format = ''
            
            
#        rst = ins_save_or_update_data.get_amadeus_air_file_fp_section_format()
#        if rst:
#            str_amadeus_air_file_fp_section_format = rst['vchr_admin_settings'] or ''
#            pass
#        str_amadeus_air_file_fp_section_format = 'FPCASH-/FPINV CUSTOMER-LPO-DSP*/DSA*/SFP*/SFA*'
        if str_amadeus_air_file_fp_section_format == 'FPCASH-/FPINV CUSTOMER-LPO-DSP*/DSA*/SFP*/SFA*':
            #FPCASH  - for cash
            #FPINV 1003-12345 (1003 is the customer code and 12345 is the LPO Number)
            #FPINV 1003-12345-DSA45 (1003 is the customer code and 12345 is the LPO Number and  discount given is 45)
            #FPCASH-108-DSP1   - It means Discount 1% of the market fare.
            #FPCASH-108-DSA10   - It means Discount 10
#            if str_line[:6] == "FPCASH":
#                str_customer_code = 'CASH'
            if str_line[:5] == "FPINV":
                try:
                    str_customer_code = str_line.split('-')[0].replace('FPINV', '').strip().upper()
                except:
                    pass
                try:
                    str_lpo_number = str_line.split('-')[1].strip().upper()
                except:
                    pass
                pass
            
            
            # // Discount Given or Service Fee
            try:
                lst_discount_data = str_line.split('-')

                bln_is_discount = False
                bln_is_discount_per = False
                bln_is_service = False
                bln_is_service_per = False
                for str_discount_data in lst_discount_data:
                    if str_discount_data[:3] == 'DSA': # // Discount Amount
                        bln_is_discount = True
                        break

                    if str_discount_data[:3] == 'DSP': # // Discount %
                        bln_is_discount_per = True
                        break

                    if str_discount_data[:3] == 'SFA': # // Service Fee Amount
                        bln_is_service = True
                        break

                    if str_discount_data[:3] == 'SFP': # // Service Fee %
                        bln_is_service_per = True
                        break
                    pass

                if bln_is_discount:
                    str_discount_data = str_discount_data.replace("DSA", '')
                    flt_discount_given = float(str_discount_data)
                    pass

                if bln_is_discount_per:
                    str_discount_data = str_discount_data.replace("DSP", '')
                    flt_discount_given_per = float(str_discount_data)
                    pass

                if bln_is_service:
                    str_discount_data = str_discount_data.replace("SFA", '')
                    flt_service_fee = float(str_discount_data)
                    pass

                if bln_is_service_per:
                    str_discount_data = str_discount_data.replace("SFP", '')
                    flt_service_fee_per = float(str_discount_data)
                    pass
            except:
                pass
            pass

        ## getting CC type and card approval
        #FPCCAX376655220013517/0217/A542239
        # getting partial uccf data
        #FPCASH+CCVI4407861201165004/1114/USD1000.00/A112213;P1
        try:
            
            if str_line[:4] == "FPCC":
                    
                lst_cc_lines = str_line[2:].split('+')
                
                for str_cc_line in lst_cc_lines :
                    if str_cc_line[:2] == 'CC' :
                        str_cc_type = str_cc_line[2:4]
                        lst_line = str_cc_line.split('/')
                        str_cc_card_no = lst_line[0].strip()[4:]
                        if len(lst_line) >= 3:

                            if lst_line[2][:3] == self.str_defult_currency_code :
                                flt_amount = lst_line[2].strip(self.str_defult_currency_code).strip()
                                try :
                                    str_card_approval_code = lst_line[3].split(';')[0].strip('\n').strip()[-6:]
                                except :
                                    str_card_approval_code = ''
                            else:
                                str_card_approval_code = lst_line[2].split(';')[0].strip('\n').strip()[-6:]
                        lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_amount ,str_card_approval_code ))
                    ## refs 37404
                    elif (str_cc_line[0:4] == 'CASH') \
                        or (str_cc_line[0:9] == 'AGTNONREF') \
                        or (str_cc_line[0:7] == 'PAX INV') \
                        or (str_cc_line[0:3] == 'INV') : 

                        bln_partial_uccf = True
                        try :
                            flt_uccf_amount = float(lst_card_data[0][2])
                        except :
                            flt_uccf_amount = 0
                        if not flt_uccf_amount and len(str_cc_line.split(';')) > 0 and len(str_cc_line.split(';')[0].split('/'))>1 :
                            try :
                                flt_credit_amount = float(str_cc_line.split(';')[0].split('/')[1].strip(self.str_defult_currency_code) )
                            except :
                                  flt_uccf_amount = 0   
                   

            ## Refs #18530
            # Getting CC type and card approval from MCO Form of payment section(Miscellaneous Charges Order Form Of payment)(Refer #11194)
            elif str_line[:5] == "MFPCC":
                try:
                    lst_cc_lines = str_line.split('+')
                    for str_cc_line in lst_cc_lines :
                        if str_cc_line[3:5] == 'CC' :
                            str_cc_type = str_cc_line[5:7]
                            str_cc_card_no = str_cc_line[7:22].strip()
                            lst_line = str_cc_line.split('/')
                            if len(lst_line) >= 3:

                                if lst_line[2][:3] == self.str_defult_currency_code :
                                    flt_amount = lst_line[2].strip(self.str_defult_currency_code)
                                    str_card_approval_code = lst_line[3].split(';')[0].strip('\n').strip()[-6:]
                                else:
                                    str_card_approval_code = lst_line[2].split(';')[0].strip('\n').strip()[-6:]
                            lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_amount ,str_card_approval_code ))
                        elif str_cc_line[0:4] == 'CASH':
                            bln_partial_uccf = True
                            try :
                                flt_uccf_amount = float(lst_card_data[0][2])
                            except :
                                flt_uccf_amount = 0

                except:
                    pass

             #// Code to capture Approval code,Credit Card Type,Credit Card Number from FPO section(Refer #9740)
            elif str_line[:3] == "FPO":
                try:
                    lst_line = str_line.split('/')
                    if lst_line[2][:2] ==  "CC":
                        str_cc_type = lst_line[1][2:4]
                        str_cc_card_no = lst_line[2][4:].strip()
                        #//Capturing of approval code has got some issues for some AIR files in which the position of Approval code
                        #//is not as expected.In place of approval code fare amount is coming.As per instructed by Mr.Niyas changed the code,
                        #//so that if currency code is present the approval code will be taken from the next position.(Refer #9740.File name:-095206_00.AIR)
                        if lst_line[4][:3] == self.str_defult_currency_code :
                            flt_amount = lst_line[4].strip('\n').strip('\r').strip().strip(self.str_defult_currency_code)
                            str_card_approval_code = lst_line[5].strip('\n').strip('\r').strip()[1:7]
                        else:
                            str_card_approval_code = lst_line[4].strip('\n').strip('\r').strip()[1:7]
                            
                    elif lst_line[3][:2] ==  "CC" and len(lst_line[3]) > 16:  ## Refs #24807
                        try :
                            str_cc_type = lst_line[3][2:4]
                            str_cc_card_no = lst_line[3][4:].strip()
                        
                            if lst_line[5][:3] == self.str_defult_currency_code :
                                flt_amount = lst_line[5].strip('\n').strip('\r').strip().strip(self.str_defult_currency_code)
                                str_card_approval_code = lst_line[6].strip('\n').strip('\r').strip()[1:7]
                            else:
                                str_card_approval_code = lst_line[5].strip('\n').strip('\r').strip()[1:7]
                        except :
                            pass
                        
                    elif lst_line[1][:2] ==  "CC" and len(lst_line[1]) > 16:
                        str_cc_type = lst_line[1][2:4]
                        str_cc_card_no = lst_line[1][4:].strip()
                        
                        if lst_line[5][:3] == self.str_defult_currency_code :
                            flt_amount = lst_line[5].strip('\n').strip('\r').strip().strip(self.str_defult_currency_code)
                            str_card_approval_code = lst_line[6].strip('\n').strip('\r').strip()[1:7]
                        else:
                            str_card_approval_code = lst_line[5].strip('\n').strip('\r').strip()[1:7]
                            
                    elif lst_line[2][:6] == 'INV+CC' and len(lst_line[2]) > 16:  #40654
                        bln_partial_uccf = True
                        str_cc_type = lst_line[2][6:8]
                        str_cc_card_no = lst_line[2][8:].strip()
                        if lst_line[4][:3] == self.str_defult_currency_code :
                            flt_uccf_amount = float(lst_line[4][3:].strip('\r').strip('\n').strip())
                        str_card_approval_code = lst_line[5].strip('\n').strip('\r').strip()[1:7]
                        flt_amount = flt_uccf_amount
                            
                    if str_line.find('+CASH/') != -1 and flt_amount:  #41917
                        flt_uccf_amount = float(flt_amount)
                        bln_partial_uccf = True

                    lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_amount ,str_card_approval_code ))
                except:
                    pass
            elif str_line[:9] == "FPCASH+CC":
                str_cc_type = str_line[9:11]
                lst_line = str_line.split('/')
                str_cc_card_no = lst_line[0].strip()[11:]
                bln_partial_uccf = True
                flt_uccf_amount =  0.0
                if len(lst_line) >= 3:
                    flt_uccf_amount =  float(lst_line[2][3:].strip('\n').strip())
                    str_currency_code = lst_line[2][:3].strip('\n').strip()
                    if str_currency_code not in (self.str_defult_currency_code, ):
                        flt_uccf_amount = 0.0
                    str_card_approval_code = lst_line[3].split(';')[0].strip('\n').strip()[-6:]
                    
                lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
                #refer #41368
                if len(lst_line) >= 5:   
                    int_cc_start = lst_line[3].find('+CC')
                    if int_cc_start != -1 :
                        bln_partial_uccf = True
                        str_cc_type = lst_line[3][int_cc_start+3:int_cc_start+5]
                        str_cc_card_no = lst_line[3][int_cc_start+5:].strip()
                        str_currency_code = lst_line[5][:3].strip()
                        flt_uccf_amount = float(lst_line[5][3:].strip())
                        if str_currency_code not in (self.str_defult_currency_code, ):
                            flt_uccf_amount = 0
                        str_card_approval_code = lst_line[6].split(';')[0].strip('\n').strip()[-6:]
                        lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
                        
            elif str_line[:12] == "FPPAX INV+CC": ### Refer #15885
                str_cc_type = str_line[12:14]
                lst_line = str_line.split('/')
                str_cc_card_no = lst_line[0].strip()[14:]
                bln_partial_uccf = True
                flt_uccf_amount =  0.0
                if len(lst_line) >= 3:
                    flt_uccf_amount =  float(lst_line[2][3:].strip('\n').strip())
                    str_currency_code = lst_line[2][:3].strip('\n').strip()
                    if str_currency_code not in (self.str_defult_currency_code, ):
                        flt_uccf_amount = 0.0
                    str_card_approval_code = lst_line[3].split(';')[0].strip('\n').strip()[-6:]
                    
                lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
                
            elif str_line[:14] == "FPAGTNONREF+CC": ### Refer #16142
                str_cc_type = str_line[14:16]
                lst_line = str_line.split('/')
                str_cc_card_no = lst_line[0].strip()[16:]
                bln_partial_uccf = True
                flt_uccf_amount =  0.0
                if len(lst_line) >= 3:
                    flt_uccf_amount =  float(lst_line[2][3:].strip('\n').strip())
                    str_currency_code = lst_line[2][:3].strip('\n').strip()
                    if str_currency_code not in (self.str_defult_currency_code, ):
                        flt_uccf_amount = 0.0
                    str_card_approval_code = lst_line[3].split(';')[0].strip('\n').strip()[-6:]
                
                lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
                
            elif str_line[:4] == "FP$$":
                str_cc_type = '$$'
                
                
            elif str_line[:7] == "FPCASH/":
                int_cc_start = str_line.find('+CC')
                if int_cc_start != -1 :
                    str_cc_type = str_line[int_cc_start+3:int_cc_start+5]
                    bln_partial_uccf = True
                    str_cc_card_no = str_line[int_cc_start+5:int_cc_start+20].strip()
                    flt_uccf_amount =  0.0
                    lst_line = str_line.split('/')
                    str_currency_code = lst_line[3][:3].strip()
                    
                    try :
                        flt_uccf_amount = float(lst_line[3][3:].strip())
                    except :
                        flt_uccf_amount = 0
                        
                    if str_currency_code not in (self.str_defult_currency_code, ):
                        flt_uccf_amount = 0
                        str_currency_code = ''
                    str_card_approval_code = ''
                    lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
                    
            elif str_line[:6] == "FPINV/":  ## Refs #22276 
                int_cc_start = str_line.find('+CC')
                if int_cc_start != -1 :
                    str_cc_type = str_line[int_cc_start+3:int_cc_start+5]
                    bln_partial_uccf = True
                    str_cc_card_no = str_line[int_cc_start+5:int_cc_start+20].strip()
                    flt_uccf_amount =  0.0
                    lst_line = str_line.split('/')
                    str_currency_code = lst_line[3][:3].strip()
                    
                    try :
                        flt_uccf_amount = float(lst_line[3][3:].strip())
                    except :
                        flt_uccf_amount = 0
                        
                    if str_currency_code not in (self.str_defult_currency_code, ):
                        flt_uccf_amount = 0
                        str_currency_code = ''
                    str_card_approval_code = ''
                    lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
                    
            elif str_line[:6] == "FPINV+":  ## Refs #22276 
                int_cc_start = str_line.find('+CC')
                if int_cc_start != -1 :
                    str_cc_type = str_line[int_cc_start+3:int_cc_start+5]
                    bln_partial_uccf = True
                    str_cc_card_no = str_line[int_cc_start+5:int_cc_start+20].strip()
                    flt_uccf_amount =  0.0
                    lst_line = str_line.split('/')
                    str_currency_code = lst_line[2][:3].strip()
                    
                    try :
                        flt_uccf_amount = float(lst_line[2][3:].strip())
                    except :
                        flt_uccf_amount = 0
                        
                    if str_currency_code not in (self.str_defult_currency_code, ):
                        flt_uccf_amount = 0
                        str_currency_code = ''
                    str_card_approval_code = ''
                    lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
                    
            elif str_line[:5] == "FPNR+":  ## Refs #22276 
                int_cc_start = str_line.find('+CC')
                if int_cc_start != -1 :
                    str_cc_type = str_line[int_cc_start+3:int_cc_start+5]
                    bln_partial_uccf = True
                    str_cc_card_no = str_line[int_cc_start+5:int_cc_start+20].strip()
                    flt_uccf_amount =  0.0
                    lst_line = str_line.split('/')
                    str_currency_code = lst_line[2][:3].strip()
                    
                    try :
                        flt_uccf_amount = float(lst_line[2][3:].strip())
                    except :
                        flt_uccf_amount = 0
                        
                    if str_currency_code not in (self.str_defult_currency_code, ):
                        flt_uccf_amount = 0
                        str_currency_code = ''
                    str_card_approval_code = lst_line[3].split(';')[0][1:]
                    lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_uccf_amount ,str_card_approval_code ))
            
            #39332
            elif str_line[:5] == "FPGOV":
                int_cc_start = str_line.find('GR')
                if int_cc_start != -1 :
                    str_card_line = str_line[int_cc_start+2:].split('*')[0].lstrip('0')
                    str_card_line = str_card_line.replace('-','') if str_card_line.find('-')!= -1 else str_card_line
                    str_cc_card_no = str_card_line[:20].strip()
                    str_cc_type = ''
                    flt_amount = 0.0
                    str_card_approval_code = ''
                    lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_amount ,str_card_approval_code ))
            
            elif str_line[:4] == "FPGR":
                int_cc_start = str_line.find('GR')
                if int_cc_start != -1 :
                    str_card_line = str_line[int_cc_start+2:].lstrip('0')
                    str_card_line = str_card_line.replace('-','') if str_card_line.find('-')!= -1 else str_card_line
                    str_cc_card_no = str_card_line[:20].strip()
                    str_cc_type = ''
                    flt_amount = 0.0
                    str_card_approval_code = ''
                    lst_card_data.append((str_cc_type ,str_cc_card_no ,flt_amount ,str_card_approval_code ))
                
        except:
            pass
        
        if len(lst_card_data) > 1 :
            str_cc_type = lst_card_data[0][0]
            str_cc_card_no = lst_card_data[0][1]
            flt_uccf_amount = 0
            str_card_approval_code = lst_card_data[0][3]
            for lst_card in lst_card_data :   #refer #41368
                flt_uccf_amount += float(lst_card[2])  #41816
            pass

        
        lst_split_line = str_line.split(';')
        lst_seg_nos = []
        for str_split_line in lst_split_line :
            str_split_line = str_split_line.strip()
            if len(str_split_line) > 1 and str_split_line[0] == 'S' and str_split_line[1].isdigit() :
                str_prev = ''
                for str_value in str_split_line[1:] :
                    
                    if str_prev.isdigit() and str_value.isdigit() :
                        lst_seg_nos[-1]+= str_value
                        
                    elif str_value == '-' and str_prev.isdigit() :
                        lst_seg_nos.append(str_value)
                    elif str_value.isdigit() :
                        lst_seg_nos.append(str_value)
                    str_prev = str_value  

        lst_seg_nos2 = []
        str_prev = ''
        for str_item in lst_seg_nos :
            str_item = str_item.strip()
            if str_prev == '-' :
                int_start = lst_seg_nos2[-1]
                int_end = int(str_item)
                while (int_start<= int_end) :
                    lst_seg_nos2.append(int_start)
                    int_start += 1
            elif str_item.isdigit() :
                lst_seg_nos2.append(int(str_item))
            str_prev =  str_item    

        return (str_customer_code,
                    str_lpo_number,
                    flt_discount_given,
                    flt_discount_given_per,
                    flt_service_fee,
                    flt_service_fee_per,
                    str_card_approval_code,
                    str_cc_type,
                    str_cc_card_no,
                    flt_uccf_amount,
                    bln_partial_uccf,
                    lst_card_data,
                    lst_seg_nos2,
                    flt_credit_amount
                    )


    

    def set_base_currency(self,str_file,bln_from_web_service = False,str_file_content = ''):
        """ File will be captured only if the currency in the file is same as the currency of the system.
            In this function the currency of the file is set into a temperary variable and that variable is
            assumed to be the base currency of the system and at the end the capturing script tests whether this currency is same as the
            currency of the system
            """

        str_currency_code = ''
        str_emd_currency_code = ''
        bln_emd = False
        
        if not bln_from_web_service:
            try:
                fd = open(str_file, 'r')
                lst_file_data = fd.readlines()
                fd.close()

            except:
                pass
        else :
            lst_file_data = str_file_content.split('***#|#|#***')
        
#        if ins_general_methods.ins_capture_base.chr_field_seperator:
#            lst_file_data = lst_file_data[0].split("\r")

        #// capture data from AIR file
        try:
            for str_line in lst_file_data:
                if (str_line.split('-')[0].strip() == "K" and str_line.split('-')[1].strip()):
                    lst_split_line = str_line.split(";")
                    str_currency_code = lst_split_line[12][:3]
                elif (str_line.split('-')[0].strip() == "KS" and str_line.split('-')[1].strip()):
                    lst_split_line = str_line.split(";")
                    str_currency_code = lst_split_line[12][:3]
                elif (str_line.split('-')[0].strip() == "KN" and str_line.split('-')[1].strip()):
                    lst_split_line = str_line.split(";")
                    str_currency_code = lst_split_line[12][:3]
                elif str_line[:3] == "RFD":
                    lst_split_line = str_line.split(";")
                    str_currency_code = lst_split_line[3][:3]
                elif str_line[:3] == "EMD": #36196
                    bln_emd = True
                    lst_split_line = str_line.split(";")
                    if lst_split_line[27].strip() == "R" :
                        str_emd_currency_code = lst_split_line[132][:3].strip()
                    else :
                        str_emd_currency_code = lst_split_line[28][:3].strip()
            if not str_currency_code and bln_emd == True :
                str_currency_code = str_emd_currency_code
                    
        except:
            pass
        return str_currency_code

    def get_category_of_file(self, str_file):
        lst_category = ['','']
        try:
            fd = open(str_file, 'r')
            lst_file_data = fd.readlines()
            fd.close()

        except:
            raise
#        if ins_general_methods.ins_capture_base.chr_field_seperator:
#            lst_file_data = lst_file_data[0].split("\r")
        #// capture data from AIR file
        try:
            for str_line in lst_file_data:
               if (str_line.split('-')[0].strip() == "T" and str_line.split('-')[1].strip()) or\
                                                (str_line.split('-')[0].strip().startswith('TMCD') and str_line.split('-')[1].strip()):
                    lst_category[0] = "T"
               if str_line.split('-')[0] == "RM*TBOVCH":
                    lst_category[1] = "V"
        except:
            raise
        return lst_category

    def get_details_of_file(self, str_file,bln_from_web_service = False,str_file_content = ''):
        
        lst_option = ['','','', 'I']
        
        if not  bln_from_web_service :
            try:
                fd = open(str_file, 'r')
                lst_file_data = fd.readlines()
                fd.close()
            except:
                return 'Error'
        else :
            lst_file_data = str_file_content.split('***#|#|#***')

#        if ins_general_methods.ins_capture_base.chr_field_seperator:
#            lst_file_data = lst_file_data[0].split("\r")

        if len(lst_file_data) > 1:
            lst_option[1] = "F"
        try:
            lst_ticket = []
            for str_line in lst_file_data:
                if str_line.split('-')[0].strip() == "T":
                    lst_split_line = str_line.split("-")
                    str_ticket_number = lst_split_line[2][:10].strip()
                    if str_ticket_number:
                        lst_option[0] = "T"
                        lst_ticket.append(str_ticket_number)
                        pass
                    pass

                elif str_line[:4] == "TMCD":
                    lst_split_line = str_line.split(";")
                    str_base_ticket_number = lst_split_line[0].split("-")[1].strip()
                    if str_base_ticket_number:
                        lst_option[0] = "T"
                        lst_ticket.append(str_ticket_number)

                elif str_line.split('-')[0] == "R":
                    lst_option[3] = 'R'
                pass
            lst_option[2] = lst_ticket
        except:
            pass
        return lst_option

if __name__ == "__main__":
    try:
        # //  CREATE INS
        ins_capture = Capture()

        # // Move not parsed folder files to parent folder
        ins_capture.move_not_parsed_folder_files_to_parent_folder()



        # // START TICKET CAPTURE
        ins_capture = ins_capture.ticket_capture()
    except KeyboardInterrupt:
        print('\nExiting capturing script...')
        pass
