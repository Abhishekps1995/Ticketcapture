"""
Purpose: Wave World span Capture
Owner  : Deethul K
Date   : 
Re. F/M: caprure.py , ticketBase.py, Ticket Invoice and Refund
Last Update: 
"""

import sys
import os
import os.path
import time
#import anydbm
import binascii
import random
import datetime
import copy
import re
 
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

ins_folder_base = ins_general_methods.create_folder_structre('Worldspan')

if ins_general_methods.ins_capture_base.str_traacs_version == 'SAAS':
    ins_save_or_update_data = saveOrUpdateData.captureDB()
else:
    ins_save_or_update_data = saveOrUpdateDataWave.captureDB()
    
global dct_error_messages
dct_error_messages = {}

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

global bln_raise
lst_args = sys.argv
if len(lst_args) > 1 and lst_args[1].strip().upper() == 'RAISE':
    bln_raise = True
else :
    bln_raise = False
    
class Capture:
    def __init__(self, *args):
        self.int_first = 1
        self.str_defult_currency_code = ins_general_methods.str_base_currency
        
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
        
        lst_files = os.listdir(ins_folder_base.str_directory)
        for str_file in lst_files:


            str_new_file = str_file
            str_new_file_tmp = str_file

            try:
                str_new_file_tmp = str_new_file_tmp.replace('.PRT', '')[:70] + '_'  + (datetime.date.today()).strftime("%d%b%Y") + '_' + str(random.randint(0, 999999999)) + '.PRT'

            except:
                pass

            try:
                str_directory_file_name = os.path.join(ins_folder_base.str_directory, str_file)
                if os.path.isdir(str_directory_file_name):
                    continue
                self.extract_ticket_data_from_file(str_directory_file_name)
            except IsDirectory as msg:
                continue
            except InputError as msg:
                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    # // move file to not parsed directory
                    if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))

                    ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                else:
                    ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
            except DuplicationError as msg:
                # // move file to parsed directory
                if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                if sys.platform == "win32":
                    os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                else:
                    os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))

                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                pass
            except OperationalError as msg:
                    ## Its a void Ticket but its Issued file is not available.

                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                    # // move file to not parsed directory
#                        self.insert_error_message()
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))

                    ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                else:
                    ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
            except:
                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                    # // move file to not parsed directory
#                        self.insert_error_message()
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))

                    ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                else:
                    ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
            else:
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
            pass

            # // Move not parsed folder files to parent folder
            ins_folder_base.int_not_parsed_folder_files_controler += 1

            if ins_folder_base.int_not_parsed_folder_files_controler >= ((3600/ins_general_methods.ins_capture_base.int_sleep_time)*1): # 1 hrs - 3600/5(sleep time) = 720
                #print '@@@ Move Not parsed files to parent folder'
                #self.move_not_parsed_folder_files_to_parent_folder()
                ins_folder_base.int_not_parsed_folder_files_controler = 0
                pass
            
            
        if self.int_first :
            #self.move_not_parsed_folder_files_to_parent_folder()
            self.int_first = 0
            self.ticket_capture()
            
    def extract_ticket_data_from_file(self, str_file ,bln_from_web_service = False,str_file_content = '',int_count = 0,bln_start=False):
        # // get file data
        ins_create_ticket_base = createTicketBaseInstance.createInstance()
        
        if bln_from_web_service and int_count:
            ins_general_methods.ins_global.dct_not_parsed_files[str_file] = ''
            
        if bln_start :
            ins_general_methods.reload_data()
#            try :
#                ins_general_methods.set_non_iata_capture_details()
#            except :
##                ins_general_methods.connect_db()
#                ins_general_methods.set_non_iata_capture_details()
                
        if not bln_from_web_service :
            fd = None
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
            
            
#        if ins_general_methods.ins_capture_base.chr_field_seperator:
#            lst_file_data = lst_file_data[0].split("\r")
        
        if '\x0c' not in lst_file_data : #Added code to append special charecter if not exists in file 
            lst_file_data.append('\x0c')
            
        lst_ticket_capture_details = []
        str_message = ''
        # // get capture ticket data ins
        ins_capture_ticket_data = instanceBase.CaptureBase()
        ins_capture_ticket_data.str_ins_db_id = id(ins_general_methods.ins_db)
        ins_capture_ticket_data.str_crs_company = 'Worldspan'
        try:
            for str_line in lst_file_data:
                str_line = str_line.strip('\n')
                if not str_line:
                    continue
                elif str_line[:2] == "QU":
                    ins_capture_ticket_data.str_pnr_first_owner_office_id = str_line[3:6].strip()

                elif str_line[0] == "1":
                #  // Record 1 - PNR File Address (Page 19.1.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    # // PNR No
                    if len(lst_split_line) >2 :
                        ins_capture_ticket_data.str_pnr_no = lst_split_line[2].split('-')[1]
                    if len(lst_split_line) == 3 :
                        ins_capture_ticket_data.bln_refund = True

                elif str_line[0] == "2":
                #// Record 2 -To capture customer code and this customer code is only used if the credit card number and
                #// (7\MS*INV and 7\CA)section are not present ie if no customer code is present.(Page 19.2.1)Refer 9839
                #// 2110669/174805/635-Here 110669 is the customer code.
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    str_customer_account_code = lst_split_line[0].split('/')[0][1:]
                    ins_capture_ticket_data.str_customer_account_code = str_customer_account_code
                    
                elif str_line[0:2] == "3\\" :
                #  // Record 3 - Branch/Agent Sines (Page 19.3.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    if len(lst_split_line) == 2 :
                        str_ticketing_data = lst_split_line[1]
                        ins_capture_ticket_data.str_ticket_refund = str_ticketing_data.split('-')[1][3:].split('/')[1][:7]
                        ins_capture_ticket_data.dat_ticket_refund = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_ticket_refund[:5],'',ins_capture_ticket_data.str_ticket_refund)
                        pass

                    else:
                        str_booking_data = lst_split_line[1]
                        ins_capture_ticket_data.str_booking_agent_code = str_booking_data[-2:]
                        ins_capture_ticket_data.str_booking_agency_office_id = str_booking_data.split('-')[1][:3].strip()
                        ins_capture_ticket_data.str_booking_agency_iata_no = str_booking_data.split('-')[1][3:].split('/')[0]
                        ins_capture_ticket_data.str_ticket_booking_date = str_booking_data.split('-')[1][3:].split('/')[1][:7]
                        ins_capture_ticket_data.str_ticket_booking_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_ticket_booking_date[:5],'',ins_capture_ticket_data.str_ticket_booking_date)
                        lst_booking_date = ins_capture_ticket_data.str_ticket_booking_date.split('/')
                        ins_capture_ticket_data.str_pnr_creation_date = lst_booking_date[2][2:]+lst_booking_date[1]+lst_booking_date[0]

                        str_ticketing_data = lst_split_line[2]
                        ins_capture_ticket_data.str_ticketing_agent_code = str_ticketing_data[-2:]
                        ins_capture_ticket_data.str_ticketing_agency_office_id = str_ticketing_data.split('-')[1][:3].strip()
                        ins_capture_ticket_data.str_pnr_current_owner_office_id = str_ticketing_data.split('-')[1][:3].strip()
                        ins_capture_ticket_data.str_ticketing_agency_iata_no = str_ticketing_data.split('-')[1][3:].split('/')[0]
                        ins_capture_ticket_data.str_ticket_issue_date = str_ticketing_data.split('-')[1][3:].split('/')[1][:7]
                        ins_capture_ticket_data.str_ticket_issue_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_ticket_issue_date[:5],'',ins_capture_ticket_data.str_ticket_issue_date)

                elif str_line[0] == "7":
                #  //Record 7 - Form of Payment(Page 19.7.1)
                    # 7\MS*INVA001
                    # 7\CA
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    #Code modified so that if length of form of payment section greater than 1,then only capturing data from this section.(Refer #10002)
                    if len(lst_split_line) > 1 :
                        str_fp_section = lst_split_line[1]
                        if str_fp_section[:2]== "CA":
#                            ins_capture_ticket_data.str_customer_code = 'CASH' #37364
                            pass
                        elif str_fp_section[:3]== "MS*":
                            try :
                                ins_capture_ticket_data.str_customer_code = str_fp_section.split('INV')[1].strip().upper()
                            except:
                                ins_capture_ticket_data.str_customer_code = ''
                        elif str_fp_section[:2] == 'CC':
                            try:
                                ins_capture_ticket_data.str_cc_type = str_fp_section[2:4]
                                ins_capture_ticket_data.str_cc_card_no = str_fp_section[5:22].strip().replace(' ','')
                                ins_capture_ticket_data.str_card_approval_code = str_fp_section.split(':')[1].strip()
                            except:
                                pass

                elif str_line[0] == "8":
                #  // Record 8 - Air Commission (Page 19.8.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    for str_field in lst_split_line:
                        if str_field.split('-')[0]== 'CP':
                            try:
                                ins_capture_ticket_data.flt_std_commn_percentage_inv = float(str_field.split('-')[1])
                            except:
                                ins_capture_ticket_data.flt_std_commn_percentage_inv = 0.0

                elif str_line[0:2] == "9\\":
                #  // Record 9 - Ticketing Carrier (Page 19.9.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    ins_capture_ticket_data.str_ticketing_airline_character_code = lst_split_line[1].split('-')[1].split('/')[0]
                    ins_capture_ticket_data.str_ticketing_airline_numeric_code = lst_split_line[1].split('-')[1].split('/')[1]

                elif str_line[:2] == "01":
                #  // Record 01/* - Ticketable Segment (Page No 45)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    if len(lst_split_line) > 4:
                        ins_capture_ticket_data.str_class_of_service = lst_split_line[3]
#                    
#                    if lst_split_line [1] == '1':
#                        ins_capture_ticket_data.lst_new_sector.append(lst_split_line[4][:3])
#                        if lst_split_line[13] != '*':
#                            ins_capture_ticket_data.lst_new_sector.extend(lst_split_line[13].split('/'))
#                        ins_capture_ticket_data.lst_new_sector.append(lst_split_line[5][:3])
#                        ins_capture_ticket_data.str_departure_date = lst_split_line[4][3:8]
#                        ins_capture_ticket_data.str_departure_time = lst_split_line[4][8:]
#                        ins_capture_ticket_data.str_prev_dest_code = lst_split_line[5][:3]
#                    elif lst_split_line [1] != '1':
#                        # // 9211 To consider the case of open Jow to save sector
#                        if ins_capture_ticket_data.str_prev_dest_code and not ins_capture_ticket_data.str_prev_dest_code == lst_split_line[4][:3]:
#                            ins_capture_ticket_data.lst_new_sector.append('')
#                            ins_capture_ticket_data.lst_new_sector.append(lst_split_line[4][:3])
#                            pass
#                        if lst_split_line[13] != '*':
#                            ins_capture_ticket_data.lst_new_sector.extend(lst_split_line[13].split('/'))
#                        ins_capture_ticket_data.lst_new_sector.append(lst_split_line[5][:3])
#                        ins_capture_ticket_data.str_prev_dest_code = lst_split_line[5][:3]
#                    pass
                
                    # Sector details
                    str_orgin_airport_code = lst_split_line[4][:3]
                    str_dest_code = lst_split_line[5][:3]
                    str_airline_code = ins_capture_ticket_data.str_ticketing_airline_character_code
                    str_airline_no = ins_capture_ticket_data.str_ticketing_airline_numeric_code
                    str_flight_number = lst_split_line[2][4:]
                    str_class = lst_split_line[3]
                    str_arrival_date =lst_split_line[4][3:8] + ins_capture_ticket_data.str_ticket_issue_date[-2:]
                    str_arrival_date = ins_general_methods.generate_valid_date(str_arrival_date[:5],'',str_arrival_date)
                    
                    if not ins_capture_ticket_data.str_start_port_code:
                        ins_capture_ticket_data.str_start_port_code = str_orgin_airport_code
                    ins_capture_ticket_data.lst_sector.append(str_dest_code)
                    
#                    if not ins_capture_ticket_data.str_ticket_issue_date.count("/"):
#                            ins_capture_ticket_data.str_ticket_issue_date = self.generate_valid_date(ins_capture_ticket_data.str_ticket_issue_date)
#                    else:
#                        ins_capture_ticket_data.str_ticket_issue_date = ins_capture_ticket_data.str_ticket_issue_date
                    
                    if ins_capture_ticket_data.str_ticket_issue_date and str_arrival_date and datetime.datetime.strptime(str_arrival_date,'%d/%m/%Y') < datetime.datetime.strptime(ins_capture_ticket_data.str_ticket_issue_date,'%d/%m/%Y'):
                        str_arrival_date = lst_split_line[4][3:8] + str(int(ins_capture_ticket_data.str_ticket_issue_date[-2:])+1)
                        str_arrival_date = ins_general_methods.generate_valid_date(str_arrival_date[:5],'',str_arrival_date)
                    
                    str_departure_date = lst_split_line[4][3:8] + ins_capture_ticket_data.str_ticket_issue_date[-2:]
                    str_departure_date = ins_general_methods.generate_valid_date(str_departure_date[:5],'',str_departure_date)
                    if ins_capture_ticket_data.str_ticket_issue_date and str_departure_date and datetime.datetime.strptime(str_departure_date,'%d/%m/%Y') < datetime.datetime.strptime(ins_capture_ticket_data.str_ticket_issue_date,'%d/%m/%Y'):
                        str_departure_date = lst_split_line[4][3:8] + str(int(ins_capture_ticket_data.str_ticket_issue_date[-2:])+1)
                        str_departure_date = ins_general_methods.generate_valid_date(str_departure_date[:5],'',str_departure_date)

                    if not ins_capture_ticket_data.str_first_departure_date:
                        dat_departure = datetime.datetime.strptime(str_departure_date,'%d/%m/%Y') 
                        ins_capture_ticket_data.str_first_departure_date = dat_departure.strftime('%d%b').upper()
                    if not ins_capture_ticket_data.str_class_of_service:
                        ins_capture_ticket_data.str_class_of_service = str_class

                    if ins_capture_ticket_data.lst_sector_details and ins_capture_ticket_data.lst_sector_details[0][0] == str_dest_code and not ins_capture_ticket_data.str_return_date:
                        ins_capture_ticket_data.str_return_date = str_arrival_date

                    bln_stopover_permitted = False
                    if lst_split_line[9][-1] == 'O':
                        bln_stopover_permitted = True
                    int_mileage = 0.0
                    if lst_split_line[12] != '*':
                        int_mileage = int(lst_split_line[12])
                    str_baggage_allowance=lst_split_line[8]
                    str_flight_duration = lst_split_line[14]
                    str_departure_time =  lst_split_line[4][8:]
                    str_orgin_airport_details = lst_split_line[4][:3]
                    str_arrival_time = lst_split_line[5][8:]
                    str_dest_airport_details =lst_split_line[5][:3]
                    str_arrival_teminal = lst_split_line[16]
                    bln_open_segment = False
                    
                    ins_capture_ticket_data.lst_sector_details.append([str_orgin_airport_code,
                                                                        str_dest_code,
                                                                        str_airline_code,
                                                                        str_airline_no,
                                                                        str_flight_number,
                                                                        str_class,
                                                                        str_class,
                                                                        str_arrival_date,
                                                                        str_departure_date,
                                                                        bln_stopover_permitted,
                                                                        int_mileage,
                                                                        0.0,
                                                                        str_arrival_time,
                                                                        str_departure_time,
                                                                        bln_open_segment])


                
                
                

                elif str_line[0:2] == "A\\":
                # // Record A - Name/Document Numbers (Page 19.A.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)

                    # // Pax Name
                    ins_capture_ticket_data.str_pax_name = lst_split_line[2]
                    # // Pax Type
                    ins_capture_ticket_data.str_pax_type = lst_split_line[3]

                     #// Ref No- 11548- Tickets moving to non parsed due to wrong length of pax type
                    ins_capture_ticket_data.str_pax_type = ins_capture_ticket_data.str_pax_type[:10]
                    str_ticket_number = ''
                    str_conjection_ticket_number = ''
                    try:
                        # // Ticket Number
                        if len(lst_split_line[6]) == 10 :
                            str_ticket_number = lst_split_line[6]
                        elif len(lst_split_line[6]) == 11 :
                            if lst_split_line[6][:1] == 'V':
                                ins_capture_ticket_data.str_void_date = lst_split_line[7]
                                ins_capture_ticket_data.bln_refund = False
                                ins_capture_ticket_data.str_ticket_refund = lst_split_line[7]
                                ins_capture_ticket_data.dat_ticket_refund = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_ticket_refund[:5],'',ins_capture_ticket_data.str_ticket_refund)
                                str_ticket_number = lst_split_line[6][1:11]
                        elif len(lst_split_line[6]) == 14 :
                            if lst_split_line[6][:1] == 'V':
                                ins_capture_ticket_data.str_void_date = lst_split_line[7]
                                ins_capture_ticket_data.bln_refund = False
                                ins_capture_ticket_data.str_ticket_refund = lst_split_line[7]
                                ins_capture_ticket_data.dat_ticket_refund = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_ticket_refund[:5],'',ins_capture_ticket_data.str_ticket_refund)
                                str_ticket_number = lst_split_line[6][1:11]
                                str_conjection_ticket_number = lst_split_line[6][1:8] + lst_split_line[6][11:]
                        elif len(lst_split_line[6]) == 13 :
                            str_ticket_number = lst_split_line[6][:10]
                            str_conjection_ticket_number = lst_split_line[6][:7]+lst_split_line[6][10:13]

                        if ins_capture_ticket_data.str_ticket_refund and not ins_capture_ticket_data.str_ticket_issue_date: #37364
                            ins_capture_ticket_data.str_ticket_issue_date = lst_split_line[7]
                            ins_capture_ticket_data.str_ticket_issue_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_ticket_issue_date[:5],'',ins_capture_ticket_data.str_ticket_issue_date)
                        if str_ticket_number == '':
                            continue
                            #raise OperationalError,"No Ticket Data Found"

                    except:
                        raise

                    ins_capture_ticket_data.lst_ticket_detls.append([str_ticket_number,
                                                                     str_conjection_ticket_number,
                                                                     'ET',
                                                                     ins_capture_ticket_data.str_ticketing_airline_character_code,
                                                                     ins_capture_ticket_data.str_ticketing_airline_numeric_code,
                                                                     ins_capture_ticket_data.str_pax_name,
                                                                     ins_capture_ticket_data.str_pax_type,
                                                                     0.0,
                                                                     '',
                                                                     '',
                                                                     ''
                                                                     ])

                elif str_line[0] == "C" and str_line[1:3] in ('IT','ET'):                    
                    ins_capture_ticket_data.str_original_issue = str_line[6:16].strip()

                
                
                
                
                elif str_line[:2] == "-E":
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    
                    str_emd_pax_name = ''
                    str_emd_pax_type = 'ADT'
                    str_emd_tkt_no = ''
                    str_emd_airline_numeric_code = ''
                    flt_emd_fare = 0.00
                    str_emd_issuance_reason = ''
                    
                    
                    for str_split_line in lst_split_line :
                        if str_split_line.startswith('NM-') :
                            str_emd_pax_name = str_split_line.split('-')[1]
                            
                        if str_split_line.startswith('EMD-') :
                            str_emd_tkt_no = str_split_line.split('-')[1][3:13]
                            str_emd_airline_numeric_code = str_split_line.split('-')[1][:3]
                        if str_split_line.startswith('AMT-') :
                            flt_emd_fare = float(str_split_line.split('-')[1][3:])
                            
                        if str_split_line.startswith('RFC-') :
                            str_emd_issuance_reason = str_split_line.split('-')[1]
                        
                    ins_capture_ticket_data.dct_emd_ticket_details[str_emd_tkt_no] = [  str_emd_tkt_no,
                                                                                        str_emd_pax_name,
                                                                                        str_emd_pax_type ,
                                                                                        flt_emd_fare ,
                                                                                        0.0 ,#flt_emd_tax
                                                                                        ins_capture_ticket_data.str_defult_currency_code,
                                                                                        [], #lst_emd_tax_details
                                                                                        str_emd_issuance_reason,
                                                                                        '', #str_emd_connection_ticket_number
                                                                                        '', #str_emd_cc_type 
                                                                                        '', #str_emd_cc_no 
                                                                                        '', #str_emd_cc_approval_code 
                                                                                        '', #str_emd_issue_date
                                                                                        ]
                    ins_capture_ticket_data.lst_ticket_detls.append([str_emd_tkt_no,
                                                                     '',
                                                                     'EMD',
                                                                     ins_capture_ticket_data.str_ticketing_airline_character_code,
                                                                     str_emd_airline_numeric_code or ins_capture_ticket_data.str_ticketing_airline_numeric_code,
                                                                     str_emd_pax_name,
                                                                     str_emd_pax_type,
                                                                     0.0,
                                                                     '',
                                                                     '',
                                                                     ''])
                    
                    
                elif str_line[0:2] == "U\\":
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    if len(lst_split_line)<2 :
                        continue
                        
                    if lst_split_line[1].startswith("EMD") :
                        try :
                            ins_capture_ticket_data.str_connection_ticket = lst_split_line[3][5:15]
                        except :
                            pass
                    
                
                elif str_line[0] == "D":
                # // Record D - Original Issue Data (Page 19.D.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    if lst_split_line[0][1] == 'T':
                        ins_capture_ticket_data.str_original_issue = lst_split_line[0][2:12]
                elif str_line[0] == "F":
                # // Record F - Tour Code (Page No 91)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    ins_capture_ticket_data.str_tour_code = lst_split_line[0][1:].strip("*").strip()[:30]

                elif str_line[0] == "G":
                    
                # // Record G - Air Fare (Page 19.G.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    
                    if len(lst_split_line) < 3 :
                        continue
                        
                        
                    if len([item for item in lst_split_line if item.strip().strip('G').strip('*').strip()]) == 0 :
                        continue
                    
                    flt_total_tax = 0.00
                    lst_tax = []
                    try:
                        for str_line in lst_split_line:
                            if str_line.strip() == '*' :
                                continue
                                
                            if str_line[0] == 'G':
                                if str_line[1:4].strip('*') :
                                    ins_capture_ticket_data.str_currency_type_code = str_line[1:4].strip('*')
                                else :
                                    ins_capture_ticket_data.str_currency_type_code = self.str_defult_currency_code
                                    
                                if ins_capture_ticket_data.str_currency_type_code == self.str_defult_currency_code:
                                    ins_capture_ticket_data.flt_fare_amount = float(str_line[4:])
                                else :
                                    ins_capture_ticket_data.flt_fare_amount = float(lst_split_line[-1][4:])
                                    ins_capture_ticket_data.str_currency_type_code = self.str_defult_currency_code
                                
                            elif str_line[0] == 'X':
                                flt_total_tax = flt_total_tax + float(str_line[3:])
#                                lst_tax.append(str_line[1:3] + '=' + str_line[3:].lstrip('0'))
                                lst_tax.append((str_line[3:].lstrip('0'),str_line[1:3],''))
                            pass
                        pass
                        ins_capture_ticket_data.flt_published_fare = ins_capture_ticket_data.flt_fare_amount
                        ins_capture_ticket_data.lst_tax = lst_tax
#                        ins_capture_ticket_data.str_tax_details = str_tax = ','.join(lst_tax)
                        ins_capture_ticket_data.flt_tax = flt_total_tax

                    except:
                        pass

#                    for lst_ticket_detls in ins_capture_ticket_data.lst_ticket_detls:
#                        if not lst_ticket_detls[4] and not lst_ticket_detls[0] in ins_capture_ticket_data.dct_emd_ticket_details:
#                            lst_ticket_detls[4].append(ins_capture_ticket_data.flt_fare_amount)
#                            lst_ticket_detls[4].append(ins_capture_ticket_data.flt_tax)
#                            lst_ticket_detls[4].append(ins_capture_ticket_data.str_tax_details)

                elif str_line[0] == "X":
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    if len(lst_split_line) < 3 :
                        continue
                    flt_total_tax = 0.00
                    lst_tax = []
                    flt_total_amount = 0.00
                    
                    try:
                        for str_split_line in lst_split_line:
                            if str_split_line[0:3] == 'TF-':
                                ins_capture_ticket_data.str_currency_type_code = self.str_defult_currency_code
                                flt_total_amount = float(str_split_line[3:])
                                 
                                
                            elif str_split_line[0] == 'T' and str_split_line.find('PD') == -1 and str_split_line[1].isdigit():
                                flt_total_tax = flt_total_tax + float(str_split_line.split('-')[1][2:].strip())
#                                lst_tax.append(str_split_line.split('-')[1][0:2].strip() + '=' + str_split_line.split('-')[1][2:].strip())
                                lst_tax.append((str_split_line.split('-')[1][2:].strip(),str_split_line.split('-')[1][0:2].strip(),''))
                            pass
                        pass
                        ins_capture_ticket_data.lst_tax = lst_tax
#                        ins_capture_ticket_data.str_tax_details = str_tax = ','.join(lst_tax)
                        ins_capture_ticket_data.flt_tax = flt_total_tax
                        if flt_total_amount :
                            ins_capture_ticket_data.flt_fare_amount = flt_total_amount - flt_total_tax
                            
                        if ins_capture_ticket_data.flt_fare_amount < 0 :
                            ins_capture_ticket_data.flt_fare_amount = 0
                        ins_capture_ticket_data.flt_published_fare = ins_capture_ticket_data.flt_fare_amount
                        
                    except:
                        pass

#                    
#                    for lst_ticket_detls in ins_capture_ticket_data.lst_ticket_detls:
#                        if not lst_ticket_detls[4] and not lst_ticket_detls[0] in ins_capture_ticket_data.dct_emd_ticket_details:
#                            lst_ticket_detls[4].append(ins_capture_ticket_data.flt_fare_amount)
#                            lst_ticket_detls[4].append(ins_capture_ticket_data.flt_tax)
#                            lst_ticket_detls[4].append(ins_capture_ticket_data.str_tax_details)
                            

                elif str_line[0] == "J":
                # // Record J - Refund Values (Page 19.J.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    flt_total_tax = 0.00
                    lst_tax_details_rfd = []
                    lst_tax = []
                    try:
                        for str_line in lst_split_line:
                            if str_line[:4] == 'RFC-':
                                ins_capture_ticket_data.str_currency_type_code = str_line[4:]
                            elif str_line[:3] == 'RF-':
                                ins_capture_ticket_data.flt_fare_refund = float(str_line[3:])
                            elif str_line[:3] == 'AP-':
                                ins_capture_ticket_data.flt_cancellation_fee = float(str_line[3:])
                            elif str_line[:3] == 'T1-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                            elif str_line[:3] == 'T2-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))

                            elif str_line[:3] == 'T3-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                            elif str_line[:3] == 'T4-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                            elif str_line[:3] == 'T5-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                            elif str_line[:3] == 'T6-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                            elif str_line[:3] == 'T7-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                            elif str_line[:3] == 'T8-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                            elif str_line[:3] == 'T9-':
                                flt_total_tax = flt_total_tax + float(str_line[5:])
#                                lst_tax_details_rfd.append(str_line[3:5].strip()+"="+str_line[5:].strip())
                                lst_tax_details_rfd.append((str_line[5:].strip(),str_line[3:5].strip(),''))
                                
                        ins_capture_ticket_data.lst_tax = lst_tax_details_rfd
#                        ins_capture_ticket_data.str_tax_details_rfd = ','.join(lst_tax_details_rfd)
                        ins_capture_ticket_data.flt_tax_refund = flt_total_tax
                    except:
                        pass
                    
                elif str_line[0] == "W":
                    #37362
                    # // Record W - Net/Selling Fare Data (Page 19.W.1)
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    
                    if len(lst_split_line) > 2 and lst_split_line[2].find("NET-") != -1:
                        ins_capture_ticket_data.flt_net_fare_inv = lst_split_line[2].split('NET-')[1].strip()
                    else:
                        ins_capture_ticket_data.flt_net_fare_inv = 0
                    ins_capture_ticket_data.flt_market_fare_inv = ins_capture_ticket_data.flt_net_fare_inv
                    
                elif str_line[0] == "N":
                # // Record N - General Remarks (Page 19.N.1)
                    str_line = str_line[1:]
                    lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                    for str_line in lst_split_line:
                        if 'AGENCY_COST CENTRE_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][3]):
                            ins_capture_ticket_data.str_cost_centre = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][3],'').strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_auto_invoice_location = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][3],'').strip()[:20].strip().split(';')[0]
                        
                        if 'AGENCY_DEPARTMENT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][3]):
                            ins_capture_ticket_data.str_auto_invoice_branch_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][3],'').strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_branch_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][3],'').strip()[:20].strip().split(';')[0]
                        
                        elif 'CUST_EMPLOYEE_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][3]) and len(str_line)>6 and str_line[6].strip() != '-':
                            ins_capture_ticket_data.str_employee_number = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][3],'').strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_cust_employee_no = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][3],'').strip()[:20].strip().split(';')[0]
                            
                        elif 'CUST_JOB_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][3]) and len(str_line)>5 and str_line[5].strip() != '-':
                            ins_capture_ticket_data.str_job_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][3],'').strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_cust_job_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][3],'').strip()[:20].strip().split(';')[0]
                            
                        elif 'FARE_SERVICE_FEE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][3]):
                            
                            ins_capture_ticket_data.bln_adt_svf = True
                            try:
                                # refer 30961
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_service_fee_sector_wise[starting_segment] =  float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_service_charge = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                
                                    
                            except:
                                ins_capture_ticket_data.flt_service_charge = 0.0


                        elif 'FARE_SERVICE_FEE_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][3]):
                            ins_capture_ticket_data.bln_chd_svf = True
                            try:
                                # refer 30961
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_child_service_fee_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_service_fee_child = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                
                            except:
                                ins_capture_ticket_data.flt_service_fee_child = 0.0

                            
                        elif 'FARE_SERVICE_FEE_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][3]):
                            
                            ins_capture_ticket_data.bln_inf_svf = True
                            try:
                                # refer 30961
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_infant_service_fee_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_service_fee_infant = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][3],'').replace('*','').strip().split(';')[0].split('/')[0])

                            except:
                                ins_capture_ticket_data.flt_service_fee_infant = 0.0



                        elif 'CUST_PAX_EMAIL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][3]):
                            ins_capture_ticket_data.str_cust_pax_email = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][3],'').strip()[:20].strip().split(';')[0]
                            
                        elif 'AGENCY_INTERNAL_REMARKS' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][3]):
                            ins_capture_ticket_data.str_agency_internal_remarks = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGENCY_TICKETING_STAFF' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][3]):
                            try:   #  Refer #40193
                                ins_capture_ticket_data.str_agency_ticketing_staff = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][3],'').strip()[:20].strip().split(';')[0]
                                if re.match('[^@]+@[^@]+\.[^@]+',ins_capture_ticket_data.str_agency_ticketing_staff): #45305
                                    ins_capture_ticket_data.str_ticketing_agent_code, ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_general_methods.get_staff_code_from_email(ins_capture_ticket_data.str_agency_ticketing_staff, 'World Span')
                                elif ins_capture_ticket_data.str_agency_ticketing_staff[:4].isdigit(): 
                                    ins_capture_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_agency_ticketing_staff[4:6]
                                    ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_agency_ticketing_staff[:4]
                                else:
                                    ins_capture_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_agency_ticketing_staff[:2]
                                    ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_agency_ticketing_staff[2:6]
                            except:
                                ins_capture_ticket_data.str_ticketing_agent_code = ''
                                ins_capture_ticket_data.str_ticketing_agent_numeric_code = ''
                                
                        elif 'CUST_PAX_MOBILE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][3]):
                            ins_capture_ticket_data.str_cust_pax_mobile = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_PURPOSE_OF_TRAVEL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][3]):
                            ins_capture_ticket_data.str_cust_purpose_of_travel = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_PURPOSE_OF_TRAVEL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][3]):
                            ins_capture_ticket_data.str_cust_purpose_of_travel = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'PARTY_LPO_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][3]):
                            ins_capture_ticket_data.str_party_lpo_no = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'PARTY_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_CODE'][3]):
                            ins_capture_ticket_data.str_party_code = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_CODE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_ENGAGEMENT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][3]):
                            ins_capture_ticket_data.str_cust_engagement_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_RESOURCE_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][3]):
                            ins_capture_ticket_data.str_cust_resource_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][3],'').strip()[:20].strip().split(';')[0]
                            
                        elif 'CUST_COMMITMENT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][3]):
                            ins_capture_ticket_data.str_cust_commitment_no = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][3],'').strip()[:20].strip().split(';')[0]
                            
                        elif 'CUST_ACCOUNTING_UNIT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][3]):
                            ins_capture_ticket_data.str_cust_accounting_unit = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_INTERNAL_AC_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][3]):
                            ins_capture_ticket_data.str_cust_internal_ac_no = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_PROJECT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][3]):
                            ins_capture_ticket_data.str_cust_project_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_ACTION_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][3]):
                            ins_capture_ticket_data.str_cust_action_no = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_APPROVER_NAME' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][3]):
                            ins_capture_ticket_data.str_cust_approver_name = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_APPROVER_EMAIL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][3]):
                            ins_capture_ticket_data.str_cust_approver_email = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_EMPLOYEE_GRADE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][3]):
                            ins_capture_ticket_data.str_cust_employee_grade = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_COST_CENTRE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][3]):
                            ins_capture_ticket_data.str_cust_cost_centre = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CUST_DEPARTMENT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][3]):
                            ins_capture_ticket_data.str_cust_department = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'PARTY_MULTIPLE_FOP_YES_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][3]):
                            ins_capture_ticket_data.str_party_multiple_fop_yes_no = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'PARTY_ADDITIONAL_AR' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][3]):
                            ins_capture_ticket_data.str_party_additional_ar = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'PARTY_FILE_JOB_CARD_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][3]):
                            ins_capture_ticket_data.str_party_file_job_card_no = str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGENCY_SALES_MAN' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][3]):
                            ins_capture_ticket_data.str_agency_sales_man = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGENCY_TRAACS_USER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][3]):
                            ins_capture_ticket_data.str_agency_traacs_user = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGENCY_ADV_RECEIPT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][3]):
                            ins_capture_ticket_data.str_agency_adv_receipt_no = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGENCY_PRODUCT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][3]):
                            ins_capture_ticket_data.str_agency_product_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGENCY_SUB_PRODUCT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_SUB_PRODUCT_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_SUB_PRODUCT_CODE'][3]):
                            ins_capture_ticket_data.str_agency_sub_product_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_SUB_PRODUCT_CODE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGENCY_AUTO_INVOICE_YES_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][3]):
                            ins_capture_ticket_data.str_agency_auto_invoice_yes_no = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'FARE_REASON_FOR_CHOOSE_HIGHER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][3]):
                            ins_capture_ticket_data.str_reason_for_choose_higher_ext = str_line.replace(ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'COMPLIANCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['COMPLIANCE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['COMPLIANCE'][3]):
                            ins_capture_ticket_data.str_compliance_ext = str_line.replace(ins_general_methods.dct_capturing_settings['COMPLIANCE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'CORPORATE_CARD_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][3]):
                            ins_capture_ticket_data.str_corp_card_code_ext = str_line.replace(ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'AGAINST_DOCUMENT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][3]):
                            ins_capture_ticket_data.str_against_doc_ext = str_line.replace(ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][3],'').strip()[:20].strip().split(';')[0]

                        elif 'PNR_TYPE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PNR_TYPE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['PNR_TYPE'][3]):
                            ins_capture_ticket_data.str_pnr_type_ext = str_line.replace(ins_general_methods.dct_capturing_settings['PNR_TYPE'][3],'').strip()[:20].strip().split(';')[0]
                        elif 'CUST_SUB_CUSTOMER_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][3]):
                            ins_capture_ticket_data.str_sub_customer_code = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][3],'').strip()[:20].strip().split(';')[0]
                            pass
                        elif 'FARE_DISCOUNT_GIVEN' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][3]):
                            ins_capture_ticket_data.bln_discount_adt = True
                            try:
                                #38119
                                ins_capture_ticket_data.flt_discount_given_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_discount_given_ext = 0.0
                        elif 'FARE_DISCOUNT_GIVEN_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][3]):
                            ins_capture_ticket_data.bln_discount_chd = True
                            try:
                                ins_capture_ticket_data.flt_discount_given_chd = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_discount_given_chd = 0.0
                        elif 'FARE_DISCOUNT_GIVEN_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][3]):
                            ins_capture_ticket_data.bln_discount_inf = True
                            try:
                                ins_capture_ticket_data.flt_discount_given_inf = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_discount_given_inf = 0.0
                        #45196        
                        elif 'FARE_PLB_DISCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][3]):
                            try:
                                ins_capture_ticket_data.flt_rm_plb_discount = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_plb_discount = 0.0  
                        elif 'FARE_DEAL_DISCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][3]):
                            try:
                                ins_capture_ticket_data.flt_rm_deal_discount = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_deal_discount = 0.0

                        elif 'FARE_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][3]):
                            try:
                                ins_capture_ticket_data.flt_selling_price_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_selling_price_ext = 0.0

                        elif 'FARE_PUBLISHED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][3]):
                            ins_capture_ticket_data.bln_published_fare_adt = True
                            try:
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_published_fare_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_published_fare_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_published_fare_ext = 0.0
                        elif 'FARE_PUBLISHED_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][3]):
                            ins_capture_ticket_data.bln_published_fare_chd = True
                            try:
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_child_published_fare_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_published_fare_chd = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_published_fare_chd = 0.0
                        elif 'FARE_PUBLISHED_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][3]):
                            ins_capture_ticket_data.bln_published_fare_inf = True
                            try:
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_infant_published_fare_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_published_fare_inf =float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_published_fare_inf = 0.0
#                                
                        elif 'FARE_ORIGINAL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][3]):
                            try:
                                ins_capture_ticket_data.flt_original_fare_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_original_fare_ext = 0.0
                                
                        elif 'FARE_PRINTING' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PRINTING'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PRINTING'][3]):
                            try:
                                ins_capture_ticket_data.flt_printing_fare_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PRINTING'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_printing_fare_ext = 0.0
                                
                        elif 'FARE_EXTRA_EARNING' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][3]):
                            ins_capture_ticket_data.bln_extra_earning_adt = True
                            try:
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_extra_earning_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_extra_earning_ext = 0.0
                        elif 'FARE_EXTRA_EARNING_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][3]):
                            ins_capture_ticket_data.bln_extra_earning_chd = True
                            try:
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_child_extra_earning_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_chd = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][3],'').replace('*','').strip().split(';')[0].split('/')[0]) 
                            except:
                                ins_capture_ticket_data.flt_extra_earning_chd = 0.0
                        elif 'FARE_EXTRA_EARNING_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][3]):
                            ins_capture_ticket_data.bln_extra_earning_inf = True
                            try:
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_line.split('$')[1][:3]
                                    dct_infant_extra_earning_sector_wise[starting_segment] = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_inf = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][3],'').replace('*','').strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_extra_earning_inf = 0.0
                                
                        elif 'FARE_PAYBACK_COMMISSION' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][3]):
                            try:
                                ins_capture_ticket_data.flt_payback_commission_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_payback_commission_ext = 0.0
                                
                        elif 'FARE_CC_CHARGE_COLLECTED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][3]):
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_ext = 0.0
                        #45305        
                        elif 'FARE_CC_CHARGE_COLLECTED_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][3]): #45305
                            ins_capture_ticket_data.bln_cc_charge_collected_chd = True
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_chd = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_chd = 0.0
                                
                        elif 'FARE_CC_CHARGE_COLLECTED_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][3]):
                            ins_capture_ticket_data.bln_cc_charge_collected_inf = True
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_inf = ins_general_methods.rm_str_to_flt(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_inf = 0.0
                                
                        elif 'FARE_LOWEST_OFFERED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][3]):
                            try:
                                ins_capture_ticket_data.flt_lowest_offered_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_lowest_offered_ext = 0.0
                                
                        elif 'FARE_ACCEPTED_OR_PAID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][3]):
                            try:
                                ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = float(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = 0.0

                        elif 'PARTY_MAXIMUM_TICKETS' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][3]):
                            try:
                                ins_capture_ticket_data.int_party_maximum_tickets = int(str_line.replace(ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.int_party_maximum_tickets = 1
                        
                        elif 'FARE_LOST_AMOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][3]):
                            try:
                                ins_capture_ticket_data.flt_fare_lost_amount_ext = int(str_line.replace(ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][3],'').replace('*','').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_fare_lost_amount_ext = 1
                                
                        elif 'AGENCY_PAYBACK_ACCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][3]):
                            try:
                                ins_capture_ticket_data.str_pay_back_account_code = str_line.replace(ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][3],'').replace('*','').strip().split(';')[0]
                            except:
                                ins_capture_ticket_data.str_pay_back_account_code = ''
                                
                        elif 'OPTION_1' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['OPTION_1'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['OPTION_1'][3]):
                            ins_capture_ticket_data.str_quot_option_1 = str_line.replace(ins_general_methods.dct_capturing_settings['OPTION_1'][3],'').strip()[:49].strip().split(';')[0]
                                
                        elif 'OPTION_2' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['OPTION_2'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['OPTION_2'][3]):
                            ins_capture_ticket_data.str_quot_option_2 = str_line.replace(ins_general_methods.dct_capturing_settings['OPTION_2'][3],'').strip()[:49].strip().split(';')[0]

                        elif 'CUST_POS_ID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_POS_ID'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_POS_ID'][3]): # refer 20879
                            ins_capture_ticket_data.int_credit_card_pos_id = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_POS_ID'][3],'')[:50].strip().strip('\n').strip() .split(';')[0]
                
                        elif 'CUST_CC_NUMBER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][3]): # refer 20879
                            ins_capture_ticket_data.str_cc_number = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][3],'')[:50].strip().strip('\n').strip().split(';')[0]

                        elif 'CRM_REFERENCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][3]): # refer 23236
                            ins_capture_ticket_data.str_crm_reference = str_line.replace(ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][3],'')[:500].strip().strip('\n').strip().split(';')[0]


                        elif 'MASTER_REFERENCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][3]): # refer 28673
                            ins_capture_ticket_data.str_master_refrence = str_line.replace(ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][3],'')[:49].strip().strip('\n').strip().split(';')[0]

                        elif 'MASTER_NARRATION' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][3]): # refer 28673
                            ins_capture_ticket_data.str_master_narration = str_line.replace(ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][3],'')[:49].strip().strip('\n').strip().split(';')[0]

                        elif 'LPO_DATE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['LPO_DATE'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['LPO_DATE'][3]):  # Refer 34857
                            ins_capture_ticket_data.dat_rm_lpo_date = str_line.replace(ins_general_methods.dct_capturing_settings['LPO_DATE'][3],'')[:49].strip().strip('\n').strip().split(';')[0]
                            
                        elif 'CUST_TRAVELLER_ID_LOYALTY_ID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][3] and str_line.startswith(ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][3]): #refer 45745
                            ins_capture_ticket_data.str_cust_traveller_id = str_line.replace(ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][3],'')[:49].strip().strip('\n').strip().split(';')[0]

                        elif ins_general_methods.dct_worldspan_optional_fields:#39282
                            for code in ins_general_methods.dct_worldspan_optional_fields:
                                if code and str_line[:len(code)]== code :
                                    if ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS' :
                                        setattr(ins_capture_ticket_data,ins_general_methods.dct_worldspan_optional_fields[code],str_line[len(code):].strip().upper()[:50])
                                    else :
                                        if isinstance(ins_general_methods.dct_worldspan_optional_fields[code], list) and \
                                                ins_general_methods.dct_worldspan_optional_fields[code][0] == 'JSON': #43533
                                            ins_capture_ticket_data.json_user_defined_remark[ins_general_methods.dct_worldspan_optional_fields[code][1]] = str_line[len(code):].strip().upper()
                                            break
                                        else:
                                            setattr(ins_capture_ticket_data,ins_general_methods.dct_worldspan_optional_fields[code],str_line[len(code):].strip().upper()[:50])
                                    break
                        if str_line.strip() not in ins_capture_ticket_data.lst_rm_field_data :
                            ins_capture_ticket_data.lst_rm_field_data.append(str_line.strip())




                elif str_line[:1] == '\x0c':
                # // END
                    lst_ticket_capture_details = []
                    lst_tickets = []

                    if not ins_capture_ticket_data.lst_ticket_detls :
                        print ("No Ticket Data ..")
#                        raise OperationalError
                    ins_capture_ticket_data.str_defult_currency_code = ins_capture_ticket_data.str_currency_type_code
                       
                    lst_ticket_capture_details , lst_tickets = ins_create_ticket_base.create_ticket_data_to_save(ins_capture_ticket_data , str_file)
                    if lst_ticket_capture_details not in (None,[],[None]) :
                        ins_save_or_update_data.save_captured_ticket_data(lst_ticket_capture_details)
                        if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                            ins_general_methods.ins_db.commit()
                        else:
                            ins_general_methods.ins_db.rollback()
                            raise Exception('Database instance commit failed')
                        if lst_tickets:
                            print(('Saved Worldspan Tickets ' + ', '.join(lst_tickets)))
                            str_message += '\n' + 'Saved Worldspan Tickets ' + ', '.join(lst_tickets)
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
                    
                else :
                    for code in ins_general_methods.dct_worldspan_optional_fields:
                        if code and str_line[:len(code)]== code :
                            setattr(ins_capture_ticket_data,ins_general_methods.dct_worldspan_optional_fields[code],str_line[len(code):].strip().upper())
                            break

#                    refer 30961
                    lst_keys = ins_general_methods.ins_global.dct_service_fee_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_service_fee_sector_wise.pop(key)

                    lst_keys = ins_general_methods.ins_global.dct_child_service_fee_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_child_service_fee_sector_wise.pop(key)

                    lst_keys = ins_general_methods.ins_global.dct_infant_service_fee_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_infant_service_fee_sector_wise.pop(key)
                    # refre 34029
                    lst_keys = ins_general_methods.ins_global.dct_extra_earning_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_extra_earning_sector_wise.pop(key)

                    lst_keys = ins_general_methods.ins_global.dct_child_extra_earning_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_child_extra_earning_sector_wise.pop(key)

                    lst_keys = ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise.pop(key)

                    lst_keys = ins_general_methods.ins_global.dct_published_fare_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_published_fare_sector_wise.pop(key)

                    lst_keys = ins_general_methods.ins_global.dct_child_published_fare_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_child_published_fare_sector_wise.pop(key)

                    lst_keys = ins_general_methods.ins_global.dct_infant_published_fare_sector_wise.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_infant_published_fare_sector_wise.pop(key)

                    #38116
                    lst_keys = ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector.keys()
                    for key in lst_keys :
                        ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector.pop(key) 
                    
                    pass
        except:
            try:
                str_file_name = os.path.split(str_file)[1].split('.')[0]
                lst_option = self.get_details_of_file(str_file,bln_from_web_service = bln_from_web_service,str_file_content = str_file_content)
                if lst_option == 'Error':
                    if not str_file_name + ':' in dct_error_messages:
                        dct_error_messages[str(str_file_name) + ':'] = ['', '', '', 'Error while opening file']
                elif lst_option[1] != "F":
                    if not str_file_name + ':' in dct_error_messages:
                        dct_error_messages[str(str_file_name) + ':'] = ['', '', '', 'The Field seperator given is wrong']
                elif lst_option[0] != "T":
                    if not str_file_name + ':' in dct_error_messages:
                        dct_error_messages[str(str_file_name) + ':'] = ['', lst_option[3], '', 'The GDS has no Ticket No']
                else:
                    for int_ticket_count in range(len(lst_option[2])):
                        str_file_name_ticket_num = str(str_file_name) + ':' + str(lst_option[2][int_ticket_count])
                        if not str_file_name_ticket_num in dct_error_messages:
                            str_line_no = str(sys.exc_info()[2].tb_lineno)
                            str_message = str(sys.exc_info()[1])
                            dct_error_messages[str_file_name_ticket_num] = [str(lst_option[2][int_ticket_count]), lst_option[3], str_line_no, str_message]
                            pass
            except :
                pass
            ins_general_methods.ins_db.rollback()
            raise
        
        #save auto invoice details
        ins_save_or_update_data.save_auto_invoice_data(ins_capture_ticket_data, lst_tickets, lst_ticket_capture_details)
            
        return str_message

    def get_details_of_file(self, str_file,bln_from_web_service = False,str_file_content = ''):
        lst_option = ['','','', 'I']
        
        if not bln_from_web_service :
            fd = None
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
        
        
        lst_file_data = lst_file_data[0].split('\r')

        try:
            lst_ticket = []
            for str_line in lst_file_data:
                str_ticket_number = ''
                if str_line:
                    if str_line[0] == "1":
                        lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                        if len(lst_split_line) == 3 :
                            lst_option[3] = 'R'
                    elif str_line[0] == "A":

                        lst_split_line = str_line.split(ins_general_methods.ins_capture_base.chr_field_seperator)
                        if len(lst_split_line) > 1:
                            lst_option[1] = "F"

                        if len(lst_split_line[6]) == 10 :
                                str_ticket_number = lst_split_line[6]
                        elif len(lst_split_line[6]) == 11 :
                            if lst_split_line[6][:1] == 'V':
                                str_ticket_number = lst_split_line[6][1:11]
                        elif len(lst_split_line[6]) == 14 :
                            if lst_split_line[6][:1] == 'V':
                                str_ticket_number = lst_split_line[6][1:11]
                        elif len(lst_split_line[6]) == 13 :
                            str_ticket_number = lst_split_line[6][:10]

                        if not str_ticket_number == '':
                            lst_option[0] = "T"
                            lst_ticket.append(str_ticket_number)
                            pass
                        pass
                    pass
            lst_option[2] = lst_ticket
        except:
            pass
        return lst_option

# //  CREATE INS
if __name__ == "__main__":
    ins_capture = Capture()

    # // Move not parsed folder files to parent folder
    #print '@@@ Move Not parsed files to parent folder'
    ins_capture.move_not_parsed_folder_files_to_parent_folder()

    # // START TICKET CAPTURE
    ins_capture = ins_capture.ticket_capture()

