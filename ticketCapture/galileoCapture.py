"""
Purpose: Wave Galileo Capture
Owner  : Deethul K
Date   : 
Re. F/M: caprure.py , ticketBase.py, Ticket Invoice and Refund
Last Update: 
"""

import sys
import os
import time
#import mx.DateTime
import binascii
import random
import copy
import datetime
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

ins_folder_base = ins_general_methods.create_folder_structre('Galileo')


if 'HOME' not in os.environ:
    os.environ['HOME'] = 'C:\\'
    
global bln_raise
lst_args = sys.argv
if len(lst_args) > 1 and lst_args[1].strip().upper() == 'RAISE':
    bln_raise = True
else :
    bln_raise = False

class IsDirectory(Exception):
    pass

class DuplicationError(Exception):
    pass

class OperationalError(Exception):
    pass

class InputError(Exception):
    pass

class TraacsError(Exception):
    pass

class Capture:
    def __init__(self, *args):
        self.int_first = 1

        self.str_defult_currency_code = ins_general_methods.str_base_currency

    def move_not_parsed_folder_files_to_parent_folder(self, *args):
        lst_files = os.listdir(ins_folder_base.str_not_parsed_dir)
        for str_file in lst_files:
            
            if os.path.isdir(os.path.join(ins_folder_base.str_not_parsed_dir, str_file)):
                continue
            if sys.platform == "win32":
                os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_not_parsed_dir, str_file), os.path.join(ins_folder_base.str_directory, str_file)))
            else:
                os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_not_parsed_dir, str_file), os.path.join(ins_folder_base.str_directory, str_file)))
                pass
            pass
        pass

    def ticket_capture(self, *args):

        self.move_not_parsed_folder_files_to_parent_folder()

        lst_files = os.listdir(ins_folder_base.str_directory)
        for str_file in lst_files:
            if str_file == 'mirfile.txt':
                continue

            # // Rename file
            str_new_file = str_file
            str_new_file_tmp = str_file
            try:
                str_new_file_tmp = str_new_file_tmp.replace('.MIR', '')[:70] + '_'  + (datetime.date.today()).strftime("%d%b%Y") + '_' + str(random.randint(0, 999999999)) + '.MIR'
                #if sys.platform == "win32":
                    #os.system("rename '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_directory, str_new_file_tmp)))
                #else:
                    #os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_directory, str_new_file_tmp)))
                    #pass
                #pass
            except:
                pass

            try:
                str_directory_file_name = os.path.join(ins_folder_base.str_directory, str_file)

                if os.path.isdir(str_directory_file_name):
                    continue

                if ins_general_methods.ins_capture_base.bln_multi_currency:
                    str_currency_code = self.set_base_currency(str_directory_file_name)
                    if str_currency_code:
                        self.str_defult_currency_code = str_currency_code
                    else :
                        self.str_defult_currency_code = ins_general_methods.str_base_currency

                self.extract_ticket_data_from_file(str_directory_file_name , str_new_file_tmp)
            except IsDirectory as msg:
                #print '@1 Directory - ', str_file
                continue
            except InputError as msg:
                if bln_raise :
                    raise
                if str(msg):
                    print('@2 Input Error - ', str_file, ' -> ', str(msg))
                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
#                        self.insert_error_message()
                    # // move file to not parsed directory
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                        pass
                    ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                else:
                    ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
            except DuplicationError as msg:
                print('@3 Duplication - ', str(msg))
                if os.access(os.path.join(ins_folder_base.str_parsed_dir, str_new_file), os.F_OK):
                    str_new_file = str_new_file_tmp
                # // move file to parsed directory
                if sys.platform == "win32":
                    os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                else:
                    os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                    pass
                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
            except OperationalError as msg:
                if bln_raise :
                    raise
#                    print msg
#                    print '@5  ', sys.exc_info()[2].tb_lineno, sys.exc_info()[1]
#                    print '@4 Operational Error - ', str(msg)
                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
#                        self.insert_error_message()
                    # // move file to not parsed directory
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                        pass
                    ins_general_methods.ins_global.dct_not_parsed_files.pop(str_directory_file_name)
                else:
                    ins_general_methods.ins_global.dct_not_parsed_files[str_directory_file_name] = None
            except Exception as msg:
                if bln_raise :
                    raise
#                    print msg
#                    print '@5  ', sys.exc_info()[2].tb_lineno, sys.exc_info()[1]
                #print '@5 Some Error in ', str_file
                if str_directory_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp

                    try:
                        str_file_name = os.path.split(str_file)[1].split('.')[0]
                        lst_option = self.get_details_of_file(str_directory_file_name)

                        if lst_option == 'Error':
                            if str_file_name + ':' not in dct_error_messages:
                                dct_error_messages[str(str_file_name) + ':'] = ['', '', '', 'Error while opening file']
                        elif lst_option[1] != "F":
                            if str_file_name + ':' not in dct_error_messages:
                                dct_error_messages[str(str_file_name) + ':'] = ['', '', '', 'The GDS File Format given is wrong']
                        elif lst_option[0] != "T":
                            if str_file_name + ':' not in dct_error_messages:
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
#                            print '@5  ', sys.exc_info()[2].tb_lineno, sys.exc_info()[1]
                        pass
#                        self.insert_error_message()
                    # // move file to not parsed directory
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                        pass
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

        time.sleep(ins_general_methods.ins_capture_base.int_sleep_time)

        # // Move not parsed folder files to parent folder
        ins_folder_base.int_not_parsed_folder_files_controler += 1

        if ins_folder_base.int_not_parsed_folder_files_controler >= ((3600/ins_general_methods.ins_capture_base.int_sleep_time)*1): # 1 hrs - 3600/5(sleep time) = 720
            #print '@@@ Move Not parsed files to parent folder'
            self.move_not_parsed_folder_files_to_parent_folder()
            ins_folder_base.int_not_parsed_folder_files_controler = 0
            pass

        if self.int_first :
            self.move_not_parsed_folder_files_to_parent_folder()
            self.int_first = 0
            self.ticket_capture()
        pass

    def extract_ticket_data_from_file(self, str_file , str_new_file_tmp, bln_from_web_service = False,str_file_content = '',int_count = 0,bln_start=False):
        # // get file data
        str_message = ''
        if bln_from_web_service and int_count:
            ins_general_methods.ins_global.dct_not_parsed_files[str_file] = ''
            
        if bln_start :
            ins_general_methods.reload_data()
#            try :
#                ins_general_methods.set_non_iata_capture_details()
#            except :
#                ins_general_methods.connect_db()
#                ins_general_methods.set_non_iata_capture_details()    
                
        if not bln_from_web_service  : 
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
            pass
        
        
        if ins_general_methods.ins_capture_base.bln_multi_currency:
            str_currency_code = self.set_base_currency(str_file,bln_from_web_service = bln_from_web_service,str_file_content = str_file_content)
            if str_currency_code:
                self.str_defult_currency_code = str_currency_code
            else :
                self.str_defult_currency_code = ins_general_methods.str_base_currency

        # // get capture ticket data ins
        ins_capture_ticket_data = instanceBase.CaptureBase()
        ins_capture_ticket_data.str_ins_db_id = id(ins_general_methods.ins_db)
        if ins_general_methods.dct_conf_data['TRAACS_VERSION'] == 'SAAS':
            ins_save_or_update_data = saveOrUpdateData.captureDB()
        else:
            ins_save_or_update_data = saveOrUpdateDataWave.captureDB()
        ins_create_ticket_base = createTicketBaseInstance.createInstance()
        
        ins_capture_ticket_data.str_defult_currency_code = self.str_defult_currency_code
        ins_capture_ticket_data.str_crs_company  = 'Galileo'
        try:
            str_line = ''.join(lst_file_data)
            str_line = str_line.replace('\n','\r')
#            str_line = lst_file_data[0]

            # // Get General Part
            str_general_part = str_line[:343].replace('\r', '~') #38730

            # // Get Ticket Details
            lst_ticket_details1 = str_line[343:].replace('\r\r', '~').split('~')

            # // Split General Valid data
            ins_capture_ticket_data.str_mir_basic_id = str_general_part[0:2].strip()
            ins_capture_ticket_data.str_transmitting_system = str_general_part[2:4].strip()
            ins_capture_ticket_data.str_iata_assigned_numaric_code = str_general_part[4:8].strip()
            ins_capture_ticket_data.str_mir_type_indicator = str_general_part[8:10].strip()
            ins_capture_ticket_data.str_total_record_size = str_general_part[10:15].strip()
            ins_capture_ticket_data.str_sequence_number = str_general_part[15:20].strip()
            try:
                ins_capture_ticket_data.str_file_creation_date = time.strftime("%d/%m/%Y", time.strptime(str_general_part[20:27].strip(), "%d%b%y"))
            except:
                #print 'Wrong str_file_creation_date ', str_general_part[20:27].strip()
                pass
            ins_capture_ticket_data.str_mir_creation_time = str_general_part[27:32].strip()
            ins_capture_ticket_data.str_ticketing_airline_character_code = str_general_part[32:34].strip()
            ins_capture_ticket_data.str_ticketing_airline_numeric_code = str_general_part[34:37].strip()
            ins_capture_ticket_data.str_ticketing_airline_name = str_general_part[37:61].strip()
            try:
                ins_capture_ticket_data.str_first_travel_date = time.strftime("%d/%m/%Y", time.strptime(str_general_part[61:68].strip(), "%d%b%y"))
            except:
                #print 'Wrong str_first_travel_date ', str_general_part[61:68].strip()
                pass
            ins_capture_ticket_data.str_input_gtid = str_general_part[68:74].strip()
            ins_capture_ticket_data.str_output_gtid = str_general_part[74:80].strip()
            #if str_general_part[82:83].isdigit():
                #ins_capture_ticket_data.str_booking_agent_code = str_general_part[83:85].strip()
            #else:
                #ins_capture_ticket_data.str_booking_agent_code = str_general_part[82:84].strip()
            #ins_capture_ticket_data.str_ticketing_agent_code = str_general_part[87:89].strip()
            ins_capture_ticket_data.str_booking_agent_code = str_general_part[117:119].strip()
            ins_capture_ticket_data.str_ticketing_agent_code = str_general_part[120:122].strip()
            ins_capture_ticket_data.str_ticketing_agency_iata_no = str_general_part[89:98].strip() # // IATA Number
            ins_capture_ticket_data.str_booking_agency_iata_no = str_general_part[89:98].strip() # // IATA Number
            ins_capture_ticket_data.str_pnr_no = ins_capture_ticket_data.str_record_locator = str_general_part[98:104].strip() # // PNR Number, If Refund/Void = 'ZZZZZZ'
            ins_capture_ticket_data.str_agent_duty_code = str_general_part[122:124].strip() # // AG=>Agent, TA=>Training(GCS), OU=>Outside User, MA=>Meshing Agent, SU=>GI/NDC user (GCS)

            ins_capture_ticket_data.str_booking_agency_office_id =  str_general_part[81:85].strip()
            ins_capture_ticket_data.str_ticketing_agency_office_id = str_general_part[85:89].strip()
            ins_capture_ticket_data.str_pnr_first_owner_office_id = str_general_part[113:117].strip()
            ins_capture_ticket_data.str_pnr_current_owner_office_id = str_general_part[85:89].strip()

            try:
                #J-404
                ins_capture_ticket_data.flt_std_commn_percentage_inv = float(str_general_part[222:224].strip() + "." + str_general_part[224:226].strip() )
            except:
                try:
                    ins_capture_ticket_data.flt_std_commn_percentage_inv = float(str_general_part[222:224].strip())
                except:
                    pass
                pass

            try:
                ins_capture_ticket_data.str_pnr_file_creation_date = time.strftime("%d/%m/%Y", time.strptime(str_general_part[124:131].strip(), "%d%b%y"))
            except:
                #print 'Wrong str_pnr_file_creation_date ', str_general_part[124:131].strip()
                pass
            try:
                ins_capture_ticket_data.str_pnr_last_changed_date = time.strftime("%d/%m/%Y", time.strptime(str_general_part[134:141].strip(), "%d%b%y"))
            except:
                #print 'Wrong str_pnr_last_changed_date ', str_general_part[134:141].strip()
                pass
            ins_capture_ticket_data.str_number_of_changes_in_pnr_file = str_general_part[141:144].strip()
            ins_capture_ticket_data.str_currency_type_code = str_general_part[145:148].strip()
            ins_capture_ticket_data.str_tour_code = str_general_part[226:241].strip()[:30]
            if str_general_part[247:248].strip() == '0':
                ins_capture_ticket_data.str_ticket_type == 'NON ATB' # // NON ATB (TAT, OPTAT)
            elif str_general_part[247:248].strip() == '1':
                ins_capture_ticket_data.str_ticket_type == 'ATB' # // ATB (ATB1,OPTAT2)
            elif str_general_part[247:248].strip() == '2':
                ins_capture_ticket_data.str_ticket_type == 'APO' # // REPRINTED ATB HQNN(APO)
            elif str_general_part[247:248].strip() == '3':
                ins_capture_ticket_data.str_ticket_type == 'APO' # // MINI-ITIN (APO)
            elif str_general_part[247:248].strip() == '4':
                ins_capture_ticket_data.str_ticket_type == 'APO' # // BOARDING PASS ONLY (ATB OR OPTAT)
            elif str_general_part[247:248].strip() == '5':
                ins_capture_ticket_data.str_ticket_type == 'ET' # // ELECTRONICTICKETING (Electronic Ticketing, not ETDN)
            elif str_general_part[247:248].strip() == '6':
                ins_capture_ticket_data.str_ticket_type == 'APO' # // COUPON PRINT FOR A ELECTRONIC TICKET (APO)
            elif str_general_part[247:248].strip() == '7':
                ins_capture_ticket_data.str_ticket_type == 'ATB2' # // ATB2
            elif str_general_part[247:248].strip() == '8':
                ins_capture_ticket_data.str_ticket_type == 'OTHER' # // OTHER
                pass
            ins_capture_ticket_data.str_conjunction_ticket_is_required = str_general_part[249:250].strip()
            ins_capture_ticket_data.str_ticket_status = str_general_part[253:254].strip()
            ins_capture_ticket_data.str_number_of_pax_name = str_general_part[300:303].strip()
            ins_capture_ticket_data.str_no_of_airline_segments = str_general_part[306:309].strip()
            ins_capture_ticket_data.str_number_of_fare_selection = str_general_part[315:318].strip()



            # // Split Ticket Details
            for str_ticket_details in lst_ticket_details1:
                str_ticket_details = str_ticket_details.strip()
                str_ticket_details1 = str_ticket_details

                if str_ticket_details[:3] == 'A00': # // CUSTOMER REMARK (APO)
                    ins_capture_ticket_data.str_customer_remark = str_ticket_details[3:46]
                elif str_ticket_details[:3] == 'A01': # // CORPORATE NAME
                    ins_capture_ticket_data.str_corporate_name = str_ticket_details[3:36]
                elif str_ticket_details[:3] == 'A02': # // PASSENGER DATA
                    str_ticket_details = '\r' + str_ticket_details     #36568
                    lst_ticket_details = str_ticket_details.split('\rA02')
#                    str_tmp_data = ''
                    for str_pax_data in lst_ticket_details:
                        # This changes are done to avoid issues when A02 section comes like
                        #A02THEKKUMPURATHKALATHIL/MOHAMEDNAVA025736534332453311754901         ADT   01  N
                        #ie, A02 section contain the string A02
                        if not str_pax_data : #36568
                            continue
#                        if len(str_pax_data) <= 37 and str_tmp_data == '':
#                            str_tmp_data = str_pax_data
#                            continue
#                        elif str_tmp_data:
#                            str_pax_data = str_tmp_data + 'A02' + str_pax_data
#                            pass
#
#                        if str_pax_data == '':
#                            continue
                        str_pax_data = 'A02' + str_pax_data

                        str_pax_name = str_pax_data[3:36].strip()
                        str_transaction_number = str_pax_data[36:47].strip()
                        str_ticket_issued_y = str_pax_data[47:48].strip()
                        str_ticket_number = str_pax_data[48:58].strip()
                        
                        ## refer 35090
                        if str_ticket_number in ins_capture_ticket_data.lst_added_tickets :
                            str_tmp_data = ''
                            continue
                        ins_capture_ticket_data.lst_added_tickets.append(str_ticket_number)
                        
                        str_number_of_tickets_booked = str_pax_data[58:60].strip()
                        str_invoice_number = str_pax_data[60:69].strip()
                        str_pax_type = str_pax_data[69:75].strip()
                        
                        str_ntd_issue_date = ''   # refer 34431 ISSUE DATE IS DIFF 
                        if len(str_pax_data.split('NTD:')) > 1:
                            ins_capture_ticket_data.str_ntd_issue_date = str_pax_data.split('NTD:')[1][:7]
                        
                        if str_pax_type and str_pax_type[0] == "C":
                            if str_pax_type[1:].isdigit():

                                chd_age = int(str_pax_type[1:])
                                if chd_age < 2:
                                    str_pax_type = "INF" #36195
                                else:
                                    str_pax_type = "CHD"
                        if str_pax_type not in ('ADT','CHD','INF') :
                            str_pax_type = 'ADT'
                        str_pax_item_number = str_pax_data[75:77].strip()
                        str_pax_exchange_item_numder = str_pax_data[77:79].strip()
                        if ins_capture_ticket_data.str_ticket_status == 'F' and not str_ticket_number:
                            continue
                        ins_capture_ticket_data.lst_pax_details.append((
                                                                str_pax_name,
                                                                str_transaction_number,
                                                                str_ticket_issued_y,
                                                                str_ticket_number,
                                                                str_number_of_tickets_booked,
                                                                str_invoice_number,
                                                                str_pax_type,
                                                                str_pax_item_number,
                                                                str_pax_exchange_item_numder,
                                                                ins_capture_ticket_data.str_ntd_issue_date,
                                                                ins_capture_ticket_data.str_ticket_type))
                        str_tmp_data = ''
                        pass
                
                elif str_ticket_details[:3] == 'A03': # // FREQUENT FLYER DATA
                    ins_capture_ticket_data.str_flyer_pax_name = str_ticket_details[3:24].strip()
                    ins_capture_ticket_data.str_flyer_carrier_code = str_ticket_details[24:26].strip()
                    ins_capture_ticket_data.str_frequent_flyer_number = str_ticket_details[27:47].strip()
                    pass
                elif str_ticket_details[:3] == 'A04': # // AIRLINE / AMTRAK DATA
                    str_ticket_details = '\r' + str_ticket_details
                    lst_ticket_details = str_ticket_details.split('\rA04')
                    str_departure_dd_mmm_yy = ''    #40776
                    str_append_string = ''
                    for str_airline_data in lst_ticket_details:
                        str_airline_data = str_airline_data.strip()
                        if str_airline_data == '':
                            continue
                        if len(str_airline_data) == 3: # // A0403KA043DRAGONAIR    605J HK07FEB1945 2110 2XMNXIAMEN       H
                            str_append_string = 'A04' + str_airline_data
                            continue
                        str_airline_data = 'A04' + str_append_string + str_airline_data
                        str_append_string = ''

                        str_segment_number = str_airline_data[3:5].strip()
                        str_airline_character_code = str_airline_data[5:7].strip()
                        str_airline_numeric_code = str_airline_data[7:10].strip()
                        str_airline_name = str_airline_data[10:22].strip()
                        str_flight_number = str_airline_data[22:26].strip()
                        str_booking_class_of_service = str_airline_data[26:28].strip()
                        str_booking_status = str_airline_data[28:30].strip() # // HK=>Holding Confirmed, HL=>Holding Waitlist, BK=>Passive Sold Segment, or Booked outside, BL=>Booked outside and Waitlisted, BN=>Booked outside and Requested, AK=>Confirmed outside, AL=>Waitlist outside, AN=>Requested outside, GK=>Passive Sold Segment ,NO=>Open Segment, PB=>Holding Waitlist
                        str_departure_dd_mmm = str_airline_data[30:35].strip()
                        if str_departure_dd_mmm == '' :
                            continue
                        str_departure_time = str_airline_data[35:40].strip()
                        str_arrival_time = str_airline_data[40:45].strip()
                        str_next_day_arrival_indicator = str_airline_data[45].strip() # // 1=>PREVIOUS DAY ARRIVAL, 2=>SAME DAY ARRIVAL, 3=>NEXT DAY ARRIVAL, 4=>2 DAYS LATER ARRIVAL
                        str_origin_code = str_airline_data[46:49].strip()
                        str_origin_city_name = str_airline_data[49:62].strip()
                        str_destination_city_code = str_airline_data[62:65].strip()
                        str_destination_city_name = str_airline_data[65:78].strip()
                        str_dom_or_int = str_airline_data[78].strip() # // D=>DOMESTIC, I=>INTERNATIONAL
                        str_stopover_indicators = str_airline_data[84:85].strip() # // X=>(Connection) No Stopover, O=>Stopover
                        str_number_of_stops = str_airline_data[85:86].strip()
                        str_baggage_allowance = str_airline_data[86:89].strip()
                        str_flight_coupon_indicator = str_airline_data[101:102].strip() # // F=>AIR TRANSPORTATION, BLANK=>COUPON NOT USED
                        str_number_of_coupons = str_airline_data[102:103].strip() # 1, 2, 3, 4 otherwise BLANK is GCS
                        if str_airline_data.find('DDL:') != -1:   #40776
                            str_departure_dd_mmm_yy = str_airline_data[str_airline_data.index('DDL:')+4:str_airline_data.index('DDL:')+13]
                        if ins_capture_ticket_data.int_number_of_segments == 0:
                            ins_capture_ticket_data.str_start_port_code = str_origin_code
                        ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments + 1
                        
                        ins_capture_ticket_data.lst_airline_data.append(
                                               (str_segment_number,
                                                str_airline_character_code,
                                                str_airline_numeric_code,
                                                str_airline_name,
                                                str_flight_number,
                                                str_booking_class_of_service,
                                                str_booking_status,
                                                str_departure_dd_mmm,
                                                str_departure_time,
                                                str_arrival_time,
                                                str_next_day_arrival_indicator,
                                                str_origin_code,
                                                str_origin_city_name,
                                                str_destination_city_code,
                                                str_destination_city_name,
                                                str_dom_or_int,
                                                str_stopover_indicators,
                                                str_number_of_stops,
                                                str_baggage_allowance,
                                                str_flight_coupon_indicator,
                                                str_number_of_coupons,
                                                str_departure_dd_mmm_yy))    #40776
                        pass
                    pass
                elif str_ticket_details[:3] == 'A05': # // WAITLIST/OTHER AIR DATA
                    lst_ticket_details = str_ticket_details.split('A05')
                    for str_waitlist_other_air_data in lst_ticket_details:
                        str_waitlist_other_air_data = str_waitlist_other_air_data.strip()
                        if str_waitlist_other_air_data == '':
                            continue
                        str_waitlist_other_air_data = 'A05' + str_waitlist_other_air_data

                        str_segment_number = str_waitlist_other_air_data[3:5].strip()
                        str_airline_character_code = str_waitlist_other_air_data[5:7].strip()
                        str_airline_numeric_code = str_waitlist_other_air_data[7:10].strip()
                        str_airline_name = str_waitlist_other_air_data[10:22].strip()
                        str_flight_number = str_waitlist_other_air_data[22:26].strip()
                        str_booking_class_of_service = str_waitlist_other_air_data[26:28].strip()
                        str_booking_status = str_waitlist_other_air_data[28:30].strip() # // HK=>Holding Confirmed, HL=>Holding Waitlist, BK=>Passive Sold Segment, or Booked outside, BL=>Booked outside and Waitlisted, BN=>Booked outside and Requested, AK=>Confirmed outside, AL=>Waitlist outside, AN=>Requested outside, GK=>Passive Sold Segment ,NO=>Open Segment, PB=>Holding Waitlist
                        str_departure_dd_mmm = str_waitlist_other_air_data[30:35].strip()
                        str_departure_time = str_waitlist_other_air_data[35:40].strip()
                        str_arrival_time = str_waitlist_other_air_data[40:45].strip()
                        str_next_day_arrival_indicator = str_waitlist_other_air_data[45:46].strip() # // 1=>PREVIOUS DAY ARRIVAL, 2=>SAME DAY ARRIVAL, 3=>NEXT DAY ARRIVAL, 4=>2 DAYS LATER ARRIVAL
                        str_origin_code = str_waitlist_other_air_data[46:49].strip()
                        str_origin_city_name = str_waitlist_other_air_data[49:62].strip()
                        str_destination_city_code = str_waitlist_other_air_data[62:65].strip()
                        str_destination_city_name = str_waitlist_other_air_data[65:78].strip()
                        str_stopover_indicators = str_waitlist_other_air_data[82:83].strip() # // X=>(Connection) No Stopover, O=>Stopover

                        ins_capture_ticket_data.lst_waitlist_other_air_data.append(
                                                               (str_segment_number,
                                                                str_airline_character_code,
                                                                str_airline_numeric_code,
                                                                str_airline_name,
                                                                str_flight_number,
                                                                str_booking_class_of_service,
                                                                str_booking_status,
                                                                str_departure_dd_mmm,
                                                                str_departure_time,
                                                                str_arrival_time,
                                                                str_next_day_arrival_indicator,
                                                                str_origin_code,
                                                                str_origin_city_name,
                                                                str_destination_city_code,
                                                                str_destination_city_name,
                                                                str_stopover_indicators))
                        pass
                    pass
                elif str_ticket_details[:3] == 'A06': # // APOLLO SEAT DATA (APO)
                    lst_ticket_details = str_ticket_details.split('A06')
                    for str_seat_data in lst_ticket_details:
                        str_seat_data = str_seat_data.strip()
                        if str_seat_data == '':
                            continue
                        str_seat_data = 'A06' + str_seat_data

                        str_pax_item_number = str_seat_data[3:5].strip()
                        str_seat_number = str_seat_data[5:8].strip()
                        str_smoking_indicators = str_seat_data[8:9].strip() # // N=>NON-SMOKING , Y=>SMOKING, U=>UNKNOWN

                        ins_capture_ticket_data.dct_seat_details[int(str_pax_item_number)] = [
                                                                str_pax_item_number,
                                                                str_seat_number,
                                                                str_smoking_indicators]
                        pass
                    pass
                elif str_ticket_details[:3] == 'A07': # // FARE VALUE DATA
                    lst_ticket_details = str_ticket_details.split('A07')

                    for str_fare_data in lst_ticket_details:
                        str_fare_data = str_fare_data.strip()
                        if str_fare_data == '':
                            continue
                        str_fare_data = 'A07' + str_fare_data

                        str_pax_item_number = str_fare_data[3:5].strip()
                        str_basic_fare_currency_code = str_fare_data[5:8].strip()
                        str_basic_fare = str_fare_data[8:20].strip()
                        str_total_amount_currency_code = str_fare_data[20:23].strip()
                        str_total_amount = str_fare_data[23:35].strip()
                        str_equivalent_amount_currency_code = str_fare_data[35:38].strip()
                        str_equivalent_amount = str_fare_data[38:50].strip()
                        str_tax_currency_code = str_fare_data[50:53].strip()
                        if str_basic_fare_currency_code != self.str_defult_currency_code:
                            if str_equivalent_amount_currency_code:
                                str_basic_fare_currency_code = str_equivalent_amount_currency_code
                            if str_equivalent_amount:
                                str_basic_fare = str_equivalent_amount
                            pass
                        try:
                            float(str_basic_fare or 0)
                        except:
                            str_basic_fare = '0.00'
                        # // Tax
                        (str_tax_string,
                            str_total_tax,str_vat) = self.get_tax_details(str_fare_data[53:].strip())

                        ins_capture_ticket_data.dct_fare_details[int(str_pax_item_number)] = [
                                                                    str_basic_fare_currency_code,
                                                                    str_basic_fare,
                                                                    str_total_amount_currency_code,
                                                                    str_total_amount,
                                                                    str_equivalent_amount_currency_code,
                                                                    str_equivalent_amount,
                                                                    str_tax_currency_code,
                                                                    str_tax_string,
                                                                    str_total_tax,
                                                                    float(str_vat or 0)
                                                                    ]
#                        if str_vat:
#                            ins_capture_ticket_data.flt_vat_in_inv = float(str_vat)
                                                                    
                        pass
                    pass
                elif str_ticket_details[:3] == 'A08': # // FARE BASIS DATA
                    lst_ticket_details = str_ticket_details.split('A08')

                    for str_fare_basis_data in lst_ticket_details:
                        str_fare_basis_data = str_fare_basis_data.strip()
                        if str_fare_basis_data == '':
                            continue
                        str_fare_basis_data = 'A08' + str_fare_basis_data

                        str_pax_item_number = str_fare_basis_data[3:5].strip()
                        str_segment_number = str_fare_basis_data[5:7].strip()
                        str_fare_basis_code = str_fare_basis_data[7:15].strip()

                        ins_capture_ticket_data.dct_fare_basis_details[int(str_pax_item_number)] = [
                                                            str_pax_item_number,
                                                            str_segment_number,
                                                            str_fare_basis_code]
                        pass
                    pass
                elif str_ticket_details[:3] == 'A09': # // FARE CONSTRUCTION DATA
                    pass
                elif str_ticket_details[:3] == 'A10': # // EXCHANGE TICKET INFORMATION
                    lst_ticket_details = str_ticket_details.split('A10')

                    for str_original_ticket_data in lst_ticket_details:
                        str_original_ticket_data = str_original_ticket_data.strip()
                        if str_original_ticket_data == '':
                            continue
                        str_original_ticket_data = 'A10' + str_original_ticket_data

                        str_pax_exchange_item_number = str_original_ticket_data[3:5].strip()   #41208
                        try:
                            int_index = str_original_ticket_data.index('TI:')
                        except:
                            str_original_ticket_number = str_original_ticket_data[8:18].strip()
                            str_number_of_tickets_booked = str_original_ticket_data[19:21].strip()
                            pass
                        else:
                            str_original_ticket_number = str_original_ticket_data[int_index+3+3:int_index+3+3+10].strip()
                            pass
                        #41208
                        ins_capture_ticket_data.dct_original_ticket_data[int(str_pax_exchange_item_number)] = [
                                                                    str_pax_exchange_item_number,
                                                                    str_original_ticket_number]
                        pass
                    pass
                elif str_ticket_details[:3] == 'A11': # // FORM OF PAYMENT DATA
                    str_ticket_details = '\r' + str_ticket_details
                    lst_ticket_details = str_ticket_details.split('\rA11')
                    
                    for str_payment_data in lst_ticket_details:
                        str_payment_data = 'A11'+ str_payment_data
                        
                        ins_capture_ticket_data.str_cc_type = str_payment_data[3:5].strip() # // S=>CASH, CK=>CHECK, CC=>CREDIT CARD, X=>CHECK OVERRIDE, EX=>EXCHANGE, MS=>MISCELLANEOUS, IN=>INVOICE, NO=>NON REFUNDABLE, MR=>MULTIPLE, FR=>FREE
                        try :
                            flt_amount = float(str_payment_data[5:17].strip())
                            if ins_capture_ticket_data.str_cc_type == 'CC' :
                                ins_capture_ticket_data.flt_cc_collected_amount += flt_amount
                            ins_capture_ticket_data.flt_total_amount_collected = str(float(ins_capture_ticket_data.flt_total_amount_collected or 0) + flt_amount)
                        except :
                            pass
                        ins_capture_ticket_data.str_refund_indicator = str_payment_data[17:18].strip() # // Y=>REFUND, N=>NOT A REFUND
                        if not ins_capture_ticket_data.str_credit_card_code:
                            ins_capture_ticket_data.str_credit_card_code = str_payment_data[18:20].strip()
                        ins_capture_ticket_data.str_credit_card_number = str_payment_data[20:40].strip()
                        if not ins_capture_ticket_data.str_cc_card_no:
                            ins_capture_ticket_data.str_cc_card_no = str_payment_data[20:40].strip()
                        ins_capture_ticket_data.str_credit_card_expiration_mm_yy = str_payment_data[40:44].strip()
                        if not ins_capture_ticket_data.str_credit_card_approval_code:
                            ins_capture_ticket_data.str_credit_card_approval_code = str_payment_data[44:52].strip()
                        ins_capture_ticket_data.str_payment_plan_options = str_payment_data[52:55].strip()
                        
                        #A11IN      280.00N                                     P:01F: 1007-12787-DSP3
                        #A11MS      280.00N                                     P:01F:-DSP2
                        if str_payment_data[:5] == 'A11MS' or str_payment_data[:4] == 'A11S':
#                            ins_capture_ticket_data.str_customer_code = 'CASH' #36773
                            str_line = str_payment_data[str_payment_data.find('P:01F:'):]
                        elif str_payment_data[:5] == 'A11IN':
                            str_line = str_payment_data[str_payment_data.find('P:01F:'):]

                            pass

                        # // Discount Given or Service Fee
                        try:
                            lst_discount_data = str_line.split('-')

                            for str_discount_data in lst_discount_data:
                                if str_discount_data[:3] == 'DSA': # // Discount Amount
                                    ins_capture_ticket_data.str_service_charge_string = str_discount_data
                                    break

                                if str_discount_data[:3] == 'DSP': # // Discount %
                                    ins_capture_ticket_data.str_service_charge_string = str_discount_data
                                    break

                                if str_discount_data[:3] == 'SFA': # // Service Fee Amount
                                    ins_capture_ticket_data.str_service_charge_string = str_discount_data
                                    break

                                if str_discount_data[:3] == 'SFP': # // Service Fee %
                                    ins_capture_ticket_data.str_service_charge_string = str_discount_data
                                    break
                                pass
                        except:
                            pass
                    pass
                elif str_ticket_details[:3] == 'A12': # // PHONE DATA
                    #38600
                    lst_phone_data = str_ticket_details.split('\r')
                    try:
                        #Refer #39210 #47435
                        str_phone_data = [str_phone_data.split('*')[1][:49].strip() for str_phone_data in lst_phone_data if str_phone_data.find('*') != -1 and str_phone_data[:3] == 'A12' and str_phone_data[6:7] == 'M'][-1]
                    except:
                        str_phone_data = ''
                    ins_capture_ticket_data.str_cust_pax_mobile = str_phone_data
                    pass
                elif str_ticket_details[:3] == 'A13': # // ADDRESS DATA
                    pass
                elif str_ticket_details[:3] == 'A14': # // BOS/TICKET REMARKS
#                    A14FT-CC425825
#                    A14FT-EMP100590
#                    A14FT-JCRUH125
#                    A14FT-TRN106425
#                    A14FT-SVF25
#                    A14VL-150125MAYMUCRM1AX6JGDC
#                    A14FT-FOP/CX/AX376655220016213/2038.00
                    lst_remarks_data = str_ticket_details.split('\r')
                    for str_ticket_details in lst_remarks_data :
                        if 'AGENCY_COST CENTRE_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][1]):
                            ins_capture_ticket_data.str_cost_centre = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][1],'').strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_auto_invoice_location = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_COST CENTRE_CODE'][1],'').strip()[:20].strip().split(';')[0]
                        
                        if 'AGENCY_DEPARTMENT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][1]):
                            ins_capture_ticket_data.str_auto_invoice_branch_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][1],'').strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_branch_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_DEPARTMENT_CODE'][1],'').strip()[:20].strip().split(';')[0]
                        
                        elif 'CUST_EMPLOYEE_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][1]):
                            ins_capture_ticket_data.str_employee_number = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][1],'').strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_cust_employee_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_NO'][1],'').strip()[:20].strip().split(';')[0] #38730
                            
                        elif 'CUST_JOB_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][1]):
                            ins_capture_ticket_data.str_job_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_JOB_CODE'][1],'').strip()[:20].strip().split(';')[0]
                        
                        elif 'FARE_SERVICE_FEE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][1]):
                            try:
                                ins_capture_ticket_data.bln_adt_svf = True
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3: # refer 30961
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_service_fee_sector_wise[starting_segment] =  float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_service_charge = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_service_charge = 0.0
                                
                        elif 'FARE_SERVICE_FEE_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][1]):
                            try:
                                ins_capture_ticket_data.bln_inf_svf = True
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3: # refer 30961
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_infant_service_fee_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_service_fee_infant = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_INFANT'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                                                                                                  
                            except:
                                ins_capture_ticket_data.flt_service_fee_infant = 0.0
                                
                            

                        elif 'FARE_SERVICE_FEE_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][1]):
                            try:
                                ins_capture_ticket_data.bln_chd_svf = True
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3: # refer 30961
                        
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.ins_general_methods.ins_global.dct_child_service_fee_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                                else:
                                    ins_capture_ticket_data.flt_service_fee_child = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_SERVICE_FEE_CHILD'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                                                                                                   
                            except:
                                ins_capture_ticket_data.flt_service_fee_child = 0.0
                                
                        elif 'PARTY_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['PARTY_CODE'][1]):
                            ins_capture_ticket_data.str_rm_customer_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PARTY_CODE'][1],'').strip()[:50].strip().split(';')[0]
                        
                        elif 'CUST_PAX_EMAIL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][1]):
                            ins_capture_ticket_data.str_cust_pax_email = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_PAX_EMAIL'][1],'').strip()[:50].strip().split(';')[0]
                        
                        elif 'AGENCY_INTERNAL_REMARKS' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][1]):
                            ins_capture_ticket_data.str_agency_internal_remarks = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_INTERNAL_REMARKS'][1],'').strip()[:50].strip().split(';')[0]
                        
                        elif 'PARTY_MAXIMUM_TICKETS' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][1]):
                            try:
                                ins_capture_ticket_data.int_tickets_count_in_inv = int(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][1],'').strip()[:20].strip().split(';')[0])
                                ins_capture_ticket_data.int_party_maximum_tickets = int(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PARTY_MAXIMUM_TICKETS'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.int_tickets_count_in_inv = 1                    
                                ins_capture_ticket_data.int_party_maximum_tickets = 1                    
                        
                        
                        elif 'AGENCY_TICKETING_STAFF' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][1]):
                            try:   #  Refer #40193
                                ins_capture_ticket_data.str_agency_ticketing_staff = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_TICKETING_STAFF'][1],'').strip()[:50].strip().split(';')[0]
                                if re.match('[^@]+@[^@]+\.[^@]+',ins_capture_ticket_data.str_agency_ticketing_staff):  #45305
                                    ins_capture_ticket_data.str_ticketing_agent_code = ins_general_methods.get_staff_code_from_email(ins_capture_ticket_data.str_agency_ticketing_staff, 'Galileo')[0]
                                else:
                                    ins_capture_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_agency_ticketing_staff[:2]
                            except:
                                ins_capture_ticket_data.str_agency_ticketing_staff = ''
                                ins_capture_ticket_data.str_ticketing_agent_code = ''
                                
                        elif 'CUST_PAX_MOBILE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][1]):
                            ins_capture_ticket_data.str_cust_pax_mobile = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_PAX_MOBILE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_PURPOSE_OF_TRAVEL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][1]):
                            ins_capture_ticket_data.str_cust_purpose_of_travel = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_PURPOSE_OF_TRAVEL'][1],'').strip()[:50].strip().split(';')[0]
                        
                        elif 'PARTY_LPO_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][1]):
                            ins_capture_ticket_data.str_party_lpo_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PARTY_LPO_NO'][1],'').strip()[:50].strip().split(';')[0]
                            ins_capture_ticket_data.str_rm_lpo_number = ins_capture_ticket_data.str_party_lpo_no
                        elif 'CUST_ENGAGEMENT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][1]):
                            ins_capture_ticket_data.str_cust_engagement_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_ENGAGEMENT_CODE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_RESOURCE_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][1]):
                            ins_capture_ticket_data.str_cust_resource_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_RESOURCE_CODE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_COMMITMENT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][1]):
                            ins_capture_ticket_data.str_cust_commitment_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_COMMITMENT_NO'][1],'').strip()[:50].strip().split(';')[0]
                        
                        elif 'CUST_ACCOUNTING_UNIT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][1]):
                            ins_capture_ticket_data.str_cust_accounting_unit = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_ACCOUNTING_UNIT'][1],'').strip()[:50].strip().split(';')[0]
                        
                        elif 'CUST_INTERNAL_AC_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][1]):
                            ins_capture_ticket_data.str_cust_internal_ac_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_INTERNAL_AC_NO'][1],'').strip()[:50].strip().split(';')[0]
                        
                        elif 'CUST_PROJECT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][1]):
                            ins_capture_ticket_data.str_cust_project_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_PROJECT_CODE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_ACTION_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][1]):
                            ins_capture_ticket_data.str_cust_action_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_ACTION_NO'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_APPROVER_NAME' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][1]):
                            ins_capture_ticket_data.str_cust_approver_name = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_APPROVER_NAME'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_APPROVER_EMAIL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][1]):
                            ins_capture_ticket_data.str_cust_approver_email = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_APPROVER_EMAIL'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_EMPLOYEE_GRADE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][1]):
                            ins_capture_ticket_data.str_cust_employee_grade = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_EMPLOYEE_GRADE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_COST_CENTRE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][1]):
                            ins_capture_ticket_data.str_cust_cost_centre = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_COST_CENTRE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CUST_DEPARTMENT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][1]):
                            ins_capture_ticket_data.str_cust_department = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_DEPARTMENT'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'PARTY_MULTIPLE_FOP_YES_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][1]):
                            ins_capture_ticket_data.str_party_multiple_fop_yes_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PARTY_MULTIPLE_FOP_YES_NO'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'PARTY_ADDITIONAL_AR' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][1]):
                            ins_capture_ticket_data.str_party_additional_ar = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PARTY_ADDITIONAL_AR'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'PARTY_FILE_JOB_CARD_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][1]):
                            ins_capture_ticket_data.str_party_file_job_card_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PARTY_FILE_JOB_CARD_NO'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'AGENCY_SALES_MAN' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][1]):
                            ins_capture_ticket_data.str_agency_sales_man = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_SALES_MAN'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'AGENCY_TRAACS_USER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][1]):
                            ins_capture_ticket_data.str_agency_traacs_user = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_TRAACS_USER'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'AGENCY_ADV_RECEIPT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][1]):
                            ins_capture_ticket_data.str_agency_adv_receipt_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_ADV_RECEIPT_NO'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'AGENCY_PRODUCT_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][1]):
                            ins_capture_ticket_data.str_agency_product_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_PRODUCT_CODE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'AGENCY_AUTO_INVOICE_YES_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][1]):
                            ins_capture_ticket_data.str_agency_auto_invoice_yes_no = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_AUTO_INVOICE_YES_NO'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'FARE_REASON_FOR_CHOOSE_HIGHER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][1]):
                            ins_capture_ticket_data.str_reason_for_choose_higher_ext = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_REASON_FOR_CHOOSE_HIGHER'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'COMPLIANCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['COMPLIANCE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['COMPLIANCE'][1]):
                            ins_capture_ticket_data.str_compliance_ext = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['COMPLIANCE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'CORPORATE_CARD_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][1]):
                            ins_capture_ticket_data.str_corp_card_code_ext = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CORPORATE_CARD_CODE'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'AGAINST_DOCUMENT_NO' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][1]):
                            ins_capture_ticket_data.str_against_doc_ext = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGAINST_DOCUMENT_NO'][1],'').strip()[:50].strip().split(';')[0]

                        elif 'PNR_TYPE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['PNR_TYPE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['PNR_TYPE'][1]):
                            ins_capture_ticket_data.str_pnr_type_ext = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['PNR_TYPE'][1],'').strip()[:50].strip().split(';')[0]
                        elif 'CUST_SUB_CUSTOMER_CODE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][0] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][1]):
                            ins_capture_ticket_data.str_sub_customer_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_SUB_CUSTOMER_CODE'][1],'').strip()[:50].strip().split(';')[0]
                        
                        
                        
                        elif 'FARE_DISCOUNT_GIVEN' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][1]):
                            ins_capture_ticket_data.bln_discount_adt = True
                            try:
                                ins_capture_ticket_data.flt_discount_given_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_discount_given_ext = 0.0
                        elif 'FARE_DISCOUNT_GIVEN_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][1]):
                            ins_capture_ticket_data.bln_discount_chd = True
                            try:
                                ins_capture_ticket_data.flt_discount_given_chd = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_CHILD'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_discount_given_chd = 0.0
                        elif 'FARE_DISCOUNT_GIVEN_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][1]):
                            ins_capture_ticket_data.bln_discount_inf = True
                            try:
                                ins_capture_ticket_data.flt_discount_given_inf = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_DISCOUNT_GIVEN_INFANT'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_discount_given_inf = 0.0
                                
                        #45196
                        elif 'FARE_PLB_DISCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][1]):
                            try:
                                ins_capture_ticket_data.flt_rm_plb_discount = ins_general_methods.rm_str_to_flt(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PLB_DISCOUNT'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_plb_discount = 0.0
                                
                        elif 'FARE_DEAL_DISCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][1]):
                            try:
                                ins_capture_ticket_data.flt_rm_deal_discount = ins_general_methods.rm_str_to_flt(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_DEAL_DISCOUNT'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_deal_discount = 0.0
                                
                        elif 'FARE_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][1]):
                            try:
                                ins_capture_ticket_data.bln_adt_selling_price = True
                                ins_capture_ticket_data.flt_selling_price_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_SELLING_PRICE'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_selling_price_ext = 0.0
                        
                        elif 'FARE_CHD_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CHD_SELLING_PRICE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_CHD_SELLING_PRICE'][1]):
                            try:
                                ins_capture_ticket_data.bln_chd_selling_price = True
                                ins_capture_ticket_data.flt_selling_price_child = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_CHD_SELLING_PRICE'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_selling_price_child = 0.0
                        
                        elif 'FARE_INF_SELLING_PRICE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_INF_SELLING_PRICE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_INF_SELLING_PRICE'][1]):
                            try:
                                ins_capture_ticket_data.bln_inf_selling_price = True
                                ins_capture_ticket_data.flt_selling_price_infant = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_INF_SELLING_PRICE'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_selling_price_infant = 0.0
                        
                        elif 'FARE_PUBLISHED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][1]):
                            ins_capture_ticket_data.bln_published_fare_adt = True
                            try:
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3:
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_published_fare_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][1],'').strip()[:20].strip().split(';')[0].split('/')[0]) 
                                else:
                                    ins_capture_ticket_data.flt_published_fare_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_published_fare_ext = 0.0
                        
                        elif 'FARE_PUBLISHED_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][1]):
                            ins_capture_ticket_data.bln_published_fare_chd = True
                            try:
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3:
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_child_published_fare_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][1],'').strip()[:20].strip().split(';')[0].split('/')[0]) 
                                else:
                                    ins_capture_ticket_data.flt_published_fare_chd = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_CHILD'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_published_fare_chd = 0.0
                        elif 'FARE_PUBLISHED_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][1]):
                            ins_capture_ticket_data.bln_published_fare_inf = True
                            try:
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_infant_published_fare_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][1],'').strip()[:20].strip().split(';')[0].split('/')[0]) 
                                else:
                                    ins_capture_ticket_data.flt_published_fare_inf = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PUBLISHED_INFANT'][1],'').strip()[:20].strip().split(';')[0].split('/')[0])
                            except:
                                ins_capture_ticket_data.flt_published_fare_inf = 0.0
                        
                        elif 'FARE_ORIGINAL' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][1]):
                            try:
                                ins_capture_ticket_data.flt_original_fare_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_ORIGINAL'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_original_fare_ext = 0.0
                        
                        
                        elif 'FARE_PRINTING' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PRINTING'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_PRINTING'][1]):
                            try:
                                ins_capture_ticket_data.flt_printing_fare_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PRINTING'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_printing_fare_ext = 0.0
                        
                        elif 'FARE_EXTRA_EARNING' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][1]):
                            ins_capture_ticket_data.bln_extra_earning_adt = True
                            try:
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3: # rfefer 34029
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_extra_earning_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][1],'').strip()[:20].strip().split(';')[0]) 
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_extra_earning_ext = 0.0
                        elif 'FARE_EXTRA_EARNING_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][1]):
                            ins_capture_ticket_data.bln_extra_earning_chd = True
                            try:
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_child_extra_earning_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][1],'').strip()[:20].strip().split(';')[0]) 
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_chd = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_CHILD'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_extra_earning_chd = 0.0
                        elif 'FARE_EXTRA_EARNING_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][1]):
                            ins_capture_ticket_data.bln_extra_earning_inf = True
                            try:
                                if len(str_ticket_details.split('$'))>1 and len(str_ticket_details.split('$')[1]) >= 3: # refer 34029
                                    starting_segment = str_ticket_details.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise[starting_segment] = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][1],'').strip()[:20].strip().split(';')[0]) 
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_inf = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_EXTRA_EARNING_INFANT'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_extra_earning_inf = 0.0
                        
                        elif 'FARE_PAYBACK_COMMISSION' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][1]):
                            try:
                                ins_capture_ticket_data.flt_payback_commission_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_PAYBACK_COMMISSION'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_payback_commission_ext = 0.0
                        
                        elif 'FARE_CC_CHARGE_COLLECTED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][1]):
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_ext = 0.0
                                
                        elif 'FARE_CC_CHARGE_COLLECTED_CHILD' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][1]):  #45305
                            ins_capture_ticket_data.bln_cc_charge_collected_chd = True
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_chd = ins_general_methods.rm_str_to_flt(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_CHILD'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_chd = 0.0
                                
                        elif 'FARE_CC_CHARGE_COLLECTED_INFANT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][1]):
                            ins_capture_ticket_data.bln_cc_charge_collected_inf = True
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_inf = ins_general_methods.rm_str_to_flt(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_CC_CHARGE_COLLECTED_INFANT'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_inf = 0.0
                        
                        elif 'FARE_LOWEST_OFFERED' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][1]):
                            try:
                                ins_capture_ticket_data.flt_lowest_offered_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_LOWEST_OFFERED'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_lowest_offered_ext = 0.0
                        
                        elif 'FARE_ACCEPTED_OR_PAID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][1]):
                            try:
                                ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_ACCEPTED_OR_PAID'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = 0.0
                        
                        elif 'FARE_LOST_AMOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][1]):
                            try:
                                ins_capture_ticket_data.flt_fare_lost_amount_ext = float(str_ticket_details.replace(ins_general_methods.dct_capturing_settings['FARE_LOST_AMOUNT'][1],'').strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_fare_lost_amount_ext = 0.0
                                
                        elif 'AGENCY_PAYBACK_ACCOUNT' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][1]):
                            try:
                                ins_capture_ticket_data.str_pay_back_account_code = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['AGENCY_PAYBACK_ACCOUNT'][1],'').strip()[:20].strip().split(';')[0]
                            except:
                                ins_capture_ticket_data.str_pay_back_account_code = ''
                        
                        elif 'OPTION_1' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['OPTION_1'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['OPTION_1'][1]):
                            ins_capture_ticket_data.str_quot_option_1 = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['OPTION_1'][1],'').strip()[:49].strip().split(';')[0]
                        
                        elif 'OPTION_2' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['OPTION_2'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['OPTION_2'][1]):
                            ins_capture_ticket_data.str_quot_option_2 = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['OPTION_2'][1],'').strip()[:49].strip().split(';')[0]
                        
                        elif 'CUST_POS_ID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_POS_ID'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_POS_ID'][1]): # refer 20879
                            ins_capture_ticket_data.int_credit_card_pos_id = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_POS_ID'][1],'')[:50].strip().strip('\n').strip().split(';')[0]
             
                        elif 'CUST_CC_NUMBER' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][1]): # refer 20879
                            ins_capture_ticket_data.str_cc_number = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_CC_NUMBER'][1],'')[:50].strip().strip('\n').strip().split(';')[0]
    
                        elif 'A14FT-FOP/CX/AX' in ins_general_methods.dct_capturing_settings and str_ticket_details[:15] == 'A14FT-FOP/CX/AX':
                            ins_capture_ticket_data.str_cc_card_no = str_ticket_details[15:30].strip().split(';')[0]
                            #//Refer #9517
                        
                        elif 'CRM_REFERENCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][1]): # Refer 23236
                            ins_capture_ticket_data.str_crm_reference = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CRM_REFERENCE'][1],'')[:500].strip().strip('\n').strip().split(';')[0]
                    
                    
                        elif 'MASTER_REFERENCE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][1]): # Refer 28673
                            ins_capture_ticket_data.str_master_refrence = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['MASTER_REFERENCE'][1],'')[:49].strip().strip('\n').strip().split(';')[0]
                            
                        elif 'MASTER_NARRATION' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][1]): # Refer 28673
                            ins_capture_ticket_data.str_master_narration = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['MASTER_NARRATION'][1],'')[:49].strip().strip('\n').strip().split(';')[0]
                        
                        elif 'LPO_DATE' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['LPO_DATE'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['LPO_DATE'][1]): # Refer 28673
                            # Refer 34857
                            ins_capture_ticket_data.dat_rm_lpo_date = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['LPO_DATE'][1],'')[:49].strip().strip('\n').strip().split(';')[0]
                            
                        #45745
                        elif 'CUST_TRAVELLER_ID_LOYALTY_ID' in ins_general_methods.dct_capturing_settings and ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][1] and str_ticket_details.startswith(ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][1]):
                            ins_capture_ticket_data.str_cust_traveller_id = str_ticket_details.replace(ins_general_methods.dct_capturing_settings['CUST_TRAVELLER_ID_LOYALTY_ID'][1],'')[:49].strip().strip('\n').strip().split(';')[0]
                            
                        if str_ticket_details.strip() not in ins_capture_ticket_data.lst_rm_field_data :
                            ins_capture_ticket_data.lst_rm_field_data.append(str_ticket_details.strip())
                        
                        
                elif str_ticket_details[:3] == 'A15': # // ASSOCIATED/UNASSOCIATED REMARKS
                    pass
                elif str_ticket_details[:3] == 'A16': # // AUXILIARY DATA (CARS,HOTELS, TOURS etc.)
                    #39488 EMD data also comes in A16 lines.
                    lst_a16_data = str_ticket_details.split('\r') #40352
                    for str_emd_ticket_details in lst_a16_data :
                        if str_emd_ticket_details[55:57].isdigit():
                            str_pax_item_number = str_emd_ticket_details[55:57]
                            str_reason_sector = str_emd_ticket_details[25:55].strip()
                            str_pax_name = str_emd_ticket_details[57:112].strip()
                            str_emd_number = str_emd_ticket_details[115:125].strip()
                            str_base_currency_code = str_emd_ticket_details[168:171].strip()
                            str_base_amount = str_emd_ticket_details[171:182].strip()

                            ins_capture_ticket_data.str_ticket_type = 'EMD'
                            ins_capture_ticket_data.str_connection_ticket = ''
                            ins_capture_ticket_data.str_currency_type_code = str_base_currency_code
                            if str_base_currency_code and ins_general_methods.ins_capture_base.bln_multi_currency:
                                ins_capture_ticket_data.str_defult_currency_code = str_base_currency_code
                            if str_emd_number and str_emd_number.isdigit():
                                ins_capture_ticket_data.dct_emd_ticket_details[str_emd_number] = [  str_emd_number,
                                                                                        str_pax_name,
                                                                                        'ADT' ,#to check
                                                                                        str_base_amount ,
                                                                                        0.0,#str_total_tax
                                                                                        str_base_currency_code,
                                                                                        [], #lst_emd_tax_details
                                                                                        str_reason_sector,
                                                                                        '', #str_emd_connection_ticket_number
                                                                                        '', #str_emd_cc_type 
                                                                                        '', #str_emd_cc_no 
                                                                                        '', #str_emd_cc_approval_code 
                                                                                        '', #str_emd_issue_date
                                                                                        ]
                                ins_capture_ticket_data.lst_pax_details.append((
                                                                    str_pax_name,
                                                                    '',
                                                                    '',
                                                                    str_emd_number,
                                                                    '',
                                                                    '',
                                                                    '',
                                                                    str_pax_item_number,
                                                                    '',
                                                                    '',
                                                                    'EMD'))

#                                ins_capture_ticket_data.dct_fare_details[int(str_pax_item_number)] = [
#                                                                    str_base_currency_code,
#                                                                    str_base_amount,
#                                                                    '',
#                                                                    '',
#                                                                    '',
#                                                                    '',
#                                                                    '',
#                                                                    '',
#                                                                    0.0,
#                                                                    0]
                            pass
                elif str_ticket_details[:3] == 'A17': # // LEISURESHOPPER DATA
                    pass
                elif str_ticket_details[:3] == 'A18': # // ETDN INFORMATION
                    pass
                elif str_ticket_details[:3] == 'A19': # // Not available - INTERNATIONAL
                    pass
                elif str_ticket_details[:3] == 'A20': # // Not available - VAT
                    pass
                
                #14941 *** EMD Not capturing
                elif str_ticket_details[:3] == 'A29': #EMD
                    try:
                        if not str_tax_string:
                            str_tax_string = ''
                    except:
                        str_tax_string = ''
                    str_pax_item_number = str_ticket_details[3:5]
                    str_segment_number = str_ticket_details[5:7]
                    str_type_of_service = str_ticket_details[7:11]#SVC or ASVC
                    
                    str_booking_indicator = str_ticket_details[11:12]#A = automated ,M = Manual

                    str_emd_identifier = str_ticket_details[12:13]#S=EMD-S ,A=EMD-A

                    str_reason_for_issuance_code = str_ticket_details[13:14].strip()
                    str_pax_name = str_ticket_details[14:69].strip()
                    str_emd_airline = str_ticket_details[69:72].strip()
                    str_emd_number = str_ticket_details[72:82].strip()  
                    str_emd_number_conjective = str_ticket_details[82:95].strip()  
                    str_validating_carrier = str_ticket_details[95:97].strip()  
                    str_base_currency_code = str_ticket_details[97:100].strip()
                    str_base_amount = str_ticket_details[100:112].strip()
                    str_equivalent_currency_code = str_ticket_details[112:115].strip()
                    str_equivalent_amount = str_ticket_details[115:127].strip()
                    str_tax_currency_code = str_ticket_details[127:130].strip()
                    str_tax_amount = str_ticket_details[130:142].strip()
                    str_total_currency_code = str_ticket_details[142:145].strip()
                    str_total_amount = str_ticket_details[145:157].strip()
                    str_bankers_selling_rate = str_ticket_details[157:170].strip()
                    str_form_of_payment_type = str_ticket_details[170:172].strip()
                    str_form_of_payment_amount = str_ticket_details[172:184].strip()
                    str_approval_code_indicator = str_ticket_details[184:185].strip()
                    str_approval_code = str_ticket_details[185:193].strip()
                    
                    try:
                        float(str_base_amount)
                    except:
                        str_base_amount = '0.00'
                    try:
                        float(str_tax_amount)
                    except:
                        str_tax_amount = '0.00'
                    ins_capture_ticket_data.str_ticket_type = 'EMD'
                    
                    
                    ins_capture_ticket_data.str_currency_type_code = str_base_currency_code
                    if str_base_currency_code and ins_general_methods.ins_capture_base.bln_multi_currency:
                        ins_capture_ticket_data.str_defult_currency_code = str_base_currency_code
                    
                    #38116
                    if str_ticket_details.find('\rED-') != -1 and str_ticket_details.find('RL-') != -1 : #40634
                        lst_emd_line = str_ticket_details.split('\r')[1].split('/')
                        str_connection_ticket = [str_emd_line[6:].strip() for str_emd_line in lst_emd_line if  str_emd_line[:3] == 'RL-' ][0]
                        ins_capture_ticket_data.str_connection_ticket = str_connection_ticket       
                    
                    #39488 In case of Both A16 and A29 there will be two same pax details so A16 should be removed.
                    lst_pax_details_temp = ins_capture_ticket_data.lst_pax_details
                    for tpl_pax_details in lst_pax_details_temp:
                        if tpl_pax_details[3] == str_emd_number:
                            ins_capture_ticket_data.lst_pax_details.remove(tpl_pax_details)
                            ins_capture_ticket_data.dct_emd_ticket_details.pop(str_emd_number)
                            
                    ins_capture_ticket_data.dct_emd_ticket_details[str_emd_number] = [  str_emd_number,
                                                                                        str_pax_name,
                                                                                        'ADT' ,#to check
                                                                                        str_base_amount ,
                                                                                        str_tax_amount,
                                                                                        str_base_currency_code,
                                                                                        [], #lst_emd_tax_details
                                                                                        '',
                                                                                        ins_capture_ticket_data.str_connection_ticket, #str_emd_connection_ticket_number
                                                                                        '', #str_emd_cc_type 
                                                                                        '', #str_emd_cc_no 
                                                                                        '', #str_emd_cc_approval_code 
                                                                                        ''] #str_emd_issue_date]
                    ins_capture_ticket_data.lst_pax_details.append((
                                                                str_pax_name,
                                                                '',
                                                                str_reason_for_issuance_code,
                                                                str_emd_number,
                                                                '',
                                                                '',
                                                                '',
                                                                str_pax_item_number,
                                                                '',
                                                                '',
                                                                'EMD'))
                    
                    
                    
#                    ins_capture_ticket_data.dct_fare_details[int(str_pax_item_number)] = [
#                                                                    str_base_currency_code,
#                                                                    str_base_amount,
#                                                                    str_total_currency_code,
#                                                                    str_total_amount,
#                                                                    str_equivalent_currency_code,
#                                                                    str_equivalent_amount,
#                                                                    str_tax_currency_code,
#                                                                    str_tax_string,
#                                                                    str_tax_amount,
#                                                                    0]
                    pass
                
                elif str_ticket_details[:3] == 'A30': 
                    str_pax_number = str_ticket_details[3:5].strip()
                    str_segment_number = str_ticket_details[5:7].strip()
                    str_coupon_number = str_ticket_details[7:8].strip() # //  SVC or ASVC

                    str_emd_number = str_ticket_details[8:21].strip()#A = Automated,M = Manual
                    str_service_provider = str_ticket_details[21:23].strip()#S = EMD-S,A = EMD-A
                    str_origin_code = str_ticket_details[23:26].strip()  
                    str_departure_date = str_ticket_details[26:31].strip()  
                    str_no_of_service_requested = str_ticket_details[31:33].strip()  
                    str_rfi_sub_code = str_ticket_details[33:36].strip()
                    str_commercial_name = str_ticket_details[36:66].strip()
                    str_currency_code_per_unit = str_ticket_details[66:69].strip()
                    str_unit_no_for_excess_baggage = str_ticket_details[69:72].strip()
                    str_rate_for_excess_baggage = str_ticket_details[72:84].strip()
                    str_qualifier_for_excess_baggage = str_ticket_details[84:85].strip()  
                    
                    str_base_currency_code = str_ticket_details[85:88].strip()
                    str_base_amount = str_ticket_details[88:100].strip()
                    str_equivalent_currency_code = str_ticket_details[100:103].strip()
                    str_equivalent_amount = str_ticket_details[103:115].strip()
                    str_tax_currency_code = str_ticket_details[115:118].strip()
                    str_tax1_amount = str_ticket_details[118:126].strip()
                    str_tax1_code = str_ticket_details[126:128].strip()
                    str_tax2_amount = str_ticket_details[128:136].strip()
                    str_tax2_code = str_ticket_details[136:138].strip()
                    str_tax3_amount = str_ticket_details[138:146].strip()
                    str_tax3_code = str_ticket_details[146:148].strip()

                    str_tax_string = ''
                    lst_tax = []
                    flt_vat = 0
                    
                    if str_tax1_code :
                        lst_tax.append((str_tax1_amount,str_tax1_code,''))
                        if ins_general_methods.ins_auto_inv.str_input_vat_code and str_tax1_code == ins_general_methods.ins_auto_inv.str_input_vat_code :
                            flt_vat += float(str_tax1_amount or 0)
                            
                    if str_tax2_code :
                        lst_tax.append((str_tax2_amount,str_tax2_code,''))
                        if ins_general_methods.ins_auto_inv.str_input_vat_code and str_tax2_code == ins_general_methods.ins_auto_inv.str_input_vat_code :
                            flt_vat += float(str_tax2_amount or 0)
                    if str_tax3_code :
                        lst_tax.append((str_tax3_amount,str_tax3_code,''))
                        if ins_general_methods.ins_auto_inv.str_input_vat_code and str_tax3_code == ins_general_methods.ins_auto_inv.str_input_vat_code :
                            flt_vat += float(str_tax3_amount or 0)
                        
#                    if lst_tax :
#                        str_tax_string = ','.join(lst_tax)
                        
                    if str_emd_number[3:] and str_emd_number[3:] in ins_capture_ticket_data.dct_emd_ticket_details :
                        ins_capture_ticket_data.dct_emd_ticket_details[str_emd_number[3:]][6] = lst_tax
                        ins_capture_ticket_data.dct_emd_ticket_details[str_emd_number[3:]][7] = str_commercial_name
                        
                    str_total_currency_code = str_ticket_details[148:151].strip()
                    str_total_amount = str_ticket_details[151:163].strip()
                    str_associated_ticket_number = str_ticket_details[163:176].strip()

                    pass
                elif str_ticket_details[:3] == 'A21': # // NET REMIT (GCS)
                    # Special fare capturing
                    try:
                        flt_special_fare = float(str_ticket_details[6:18])
                    except:
                        flt_special_fare = 0.00
                    # // if special fare is not in the 6th position , we will take the data from 37
                    if not flt_special_fare:
                        lst_special_fare_data = [str_fare_data for str_fare_data in str_ticket_details[37:58] if str_fare_data.isdigit() or str_fare_data == '.' ]
                        try:
                            flt_special_fare = float("".join(lst_special_fare_data))
                        except:
                            flt_special_fare = 0.00
                        
                    ins_capture_ticket_data.flt_special_fare = flt_special_fare

                    lst_tour_code = str_ticket_details[18:].split(' ')
                    for str_tour_code in lst_tour_code:
                        if str_tour_code.strip():
                            ins_capture_ticket_data.str_tour_code = str_tour_code.strip()[:30]
                            break
                        pass
                    pass
                elif str_ticket_details[:3] == 'A22': # // GALILEO SEAT DATA (GCS)
                    pass
                elif str_ticket_details[:3] == 'A23': # // REFUND DATA (GCS)
                    str_ticket_details = '\r' + str_ticket_details  #46750
                    lst_ticket_details = str_ticket_details.split('\rA23')

                    for str_refund_data in lst_ticket_details:
                        str_refund_data = str_refund_data.strip()
                        if str_refund_data == '':
                            continue
                        str_refund_data = 'A23' + str_refund_data

                        str_ticket_number = str_refund_data[6:16].strip()
                        try:
                            str_issued_date_of_refund_document = time.strftime("%d/%m/%Y", time.strptime(str_refund_data[32:39].strip(), "%d%b%y"))
                        except:
                            print('Wrong str_issued_date_of_refund_document ', str_refund_data[32:39].strip())
                            pass
                        str_pax_name = str_refund_data[48:81].strip()
                        try:
                            int_index = str_refund_data.index('PF:')
                        except:
                            str_cancellation_fee = 0.00
                        else:
                            str_cancellation_fee = str_refund_data[int_index+3:int_index+3+8].strip()
                            pass
                        
                        try:
                            int_index = str_refund_data.index('CR:')
                        except:
                            str_std_commission_rfd = 0.00
                        else:
                            str_std_commission_rfd = str_refund_data[int_index+3:int_index+3+5].strip()
                            pass
                        try:
                            int_index = str_refund_data.index('RA:')
                        except:
                            str_refund_amount = 0.00
                        else:
                            str_refund_amount = str_refund_data[int_index+3+35:int_index+3+35+8].strip()
                            pass

                        
                        try:  # refer 35409
                            int_index = str_refund_data.index('FP1:')
                            
                        except:
                            
                            str_credit_card_code = ''
                            str_cc_card_no = ''
                            str_credit_card_expiration_mm_yy = ''
                        else:
                            
                            str_credit_card_code = str_refund_data[int_index+4:int_index+4+2].strip()
                            str_cc_card_no = str_refund_data[int_index+4+2:int_index+4+3+27].strip()
                            str_credit_card_expiration_mm_yy = str_refund_data[int_index+4+30:int_index+4+30+4].strip()
                            str_refund_amount = str_refund_data[int_index+4+35:int_index+4+35+8].strip()
                            
                            pass
                        
                        
                        # // For capturing Refund Tax Refer #9845
                        # // BF:SAR    0.00T1:          T2:  319.00YQ
                        # // AU: 126HQF8VABA0I     
                        str_tax_string_rfd = ''
                        str_t3_tax_amount = ''
                        lst_it_tax = []
                        try:
                            int_index = str_refund_data.index('BF:')
                            str_tax_bf_string = str_refund_data[int_index+3+3+8:].split('\r')[0].strip()
                            int_t3_tax_index = str_tax_bf_string.index('T3:') #42325
                            str_t3_tax_amount = str_tax_bf_string[int_t3_tax_index+3:].strip()
                        except:
                            str_total_tax_rfd = '0.00'
                        else:
                            # // Tax
                            (str_tax_string_rfd,
                                str_total_tax_rfd,str_vat) = self.get_tax_details(str_tax_bf_string)
                            pass
                        
                        #If there is no tax amount in T3 then it is has more than 3 tax components 
                            #and it shown in 'IT:' -- As per specification #42325
                        try:
                            if not str_t3_tax_amount :
                                int_index = str_refund_data.index('\rIT:')
                                str_tax_it_string = str_refund_data[int_index+4:].split('\r')[0].strip()
                                if str_tax_it_string:
                                    for str_it_tax in str_tax_it_string.split():
                                        lst_it_tax.append(str_it_tax[-2:]+'='+str_it_tax[:-2])
                                        str_total_tax_rfd = str(float(str_total_tax_rfd) + float(str_it_tax[:-2]))
                                    str_tax_string_rfd = str_tax_string_rfd +','+  ','.join(lst_it_tax)
                        except:
                            pass
                            
                        # If ticket no not available from A02 section ,take from A23 section
                        (str_pax_name,
                            str_transaction_number,
                            str_ticket_issued_y,
                            str_ticket_number_from_a02,
                            str_number_of_tickets_booked,
                            str_invoice_number,
                            str_pax_type,
                            str_pax_item_number,
                            str_pax_exchange_item_numder,
                            str_ntd_issue_date,
                            str_ticket_type) = ins_capture_ticket_data.lst_pax_details[-1]

                        if str_ticket_number_from_a02 == '':
                            ins_capture_ticket_data.lst_pax_details[-1] =  (str_pax_name,
                                                                            str_transaction_number,
                                                                            str_ticket_issued_y,
                                                                            str_ticket_number,
                                                                            str_number_of_tickets_booked,
                                                                            str_invoice_number,
                                                                            str_pax_type,
                                                                            str_pax_item_number,
                                                                            str_pax_exchange_item_numder,
                                                                            str_ntd_issue_date or '',
                                                                            str_ticket_type or '')

                        ins_capture_ticket_data.dct_refund_data[str_ticket_number] = [
                                                            str_ticket_number,
                                                            str_issued_date_of_refund_document,
                                                            str_pax_name,
                                                            float(str_cancellation_fee or 0),
                                                            float(str_std_commission_rfd or 0),
                                                            float(str_refund_amount or 0),
                                                            float(str_total_tax_rfd or 0),
                                                            str_tax_string_rfd,
                                                            float(str_vat or 0),
                                                            str_credit_card_code,
                                                            str_cc_card_no,
                                                            str_credit_card_expiration_mm_yy]
                        
                        pass
                    pass
                 
                lst_opt_data = str_ticket_details1.split('\r')

                for str_line in lst_opt_data :
                    for code in ins_general_methods.dct_galileo_optional_fields:
                        if code and str_line[:len(code)] == code :
                            if ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS' :
                                setattr(ins_capture_ticket_data,ins_general_methods.dct_galileo_optional_fields[code],str_line[len(code):].strip().upper()[:50])
                            else:
                                ins_capture_ticket_data.dct_extra_capturing_data.update({ins_general_methods.dct_galileo_optional_fields[code] : str_line[len(code):].strip().upper()})
                            break
            pass
        except Exception as msg:
            raise Exception('Unable to parse file..' + str(msg))

        # // If Refund or Void
        str_type = ''
        lst_ticket_capture_details = []
        lst_tickets = []
        for tpl_pax_details in ins_capture_ticket_data.lst_pax_details:
            (str_pax_name,
                str_transaction_number,
                str_ticket_issued_y,
                str_ticket_number,
                str_number_of_tickets_booked,
                str_invoice_number,
                str_pax_type,
                str_pax_item_number,
                str_pax_exchange_item_numder,
                str_ntd_issue_date,
                str_ticket_type) = tpl_pax_details
                
            # ** Conjection Ticket Number
            str_conjection_ticket_number = ''
            try:
                #if ins_capture_ticket_data.str_conjunction_ticket_is_required == 'Y':
                if int(str_number_of_tickets_booked) > 1:
                    str_conjection_ticket_number = str(int(str_ticket_number) + (int(str_number_of_tickets_booked) - 1))
                    pass
            except:
                pass
            
            ins_capture_ticket_data.lst_ticket_detls.append([str_ticket_number,
                                                                     str_conjection_ticket_number,
                                                                     str_ticket_type,
                                                                     ins_capture_ticket_data.str_ticketing_airline_character_code,
                                                                     ins_capture_ticket_data.str_ticketing_airline_numeric_code,
                                                                     str_pax_name,
                                                                     str_pax_type,
                                                                     ins_capture_ticket_data.flt_service_charge,
                                                                     '',
                                                                     '',
                                                                     int(str_pax_item_number or 1)])
        
        
#        if ins_capture_ticket_data.str_record_locator.upper() == 'ZZZZZZ':
        if ins_capture_ticket_data.dct_refund_data or ins_capture_ticket_data.str_ticket_status == 'R' or ins_capture_ticket_data.str_ticket_status == 'D':  #42425 # //refer Bug 9542 
            ins_capture_ticket_data.bln_refund = True
            ins_capture_ticket_data.dat_ticket_refund = ins_capture_ticket_data.str_file_creation_date
            str_type = 'REFUND'
        elif ins_capture_ticket_data.str_ticket_status == 'V' or ins_capture_ticket_data.str_ticket_status == 'Z':  #refer 34716      # //refer Bug 9542 
            ins_capture_ticket_data.bln_is_void = True
            ins_capture_ticket_data.str_void_date = ins_capture_ticket_data.str_file_creation_date
            str_type = 'VOID'
            pass

        # // Display Ticket Data
        #self.display_all_capture_data(ins_capture_ticket_data, str_type)
        
        if not ins_capture_ticket_data.lst_pax_details and not ins_capture_ticket_data.dct_refund_data:
            raise InputError("Ticket Data Not found !!!!")
        # // Save Ticket Data
        lst_ticket_capture_details , lst_tickets = ins_create_ticket_base.create_ticket_data_to_save(ins_capture_ticket_data , str_file)
        
        if lst_ticket_capture_details not in (None,[],[None]):
            ins_save_or_update_data.save_captured_ticket_data(lst_ticket_capture_details)
            if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                ins_general_methods.ins_db.commit()
            else:
                ins_general_methods.ins_db.rollback()
                raise Exception('Database instance commit failed')
            if lst_tickets:
                print(('Saved Galileo Tickets ' + ', '.join(lst_tickets)))
                str_message += '\n' + 'Saved Galileo Tickets ' + ', '.join(lst_tickets)

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
        #save auto invoice
        ins_save_or_update_data.save_auto_invoice_data(ins_capture_ticket_data, lst_tickets, lst_ticket_capture_details)
#        if ins_capture_ticket_data.bln_is_refund:
#            str_message = self.save_refund_ticket_details(ins_capture_ticket_data, str_file, str_new_file_tmp)
#        elif ins_capture_ticket_data.bln_is_void:
#            str_message = self.save_void_ticket_details(ins_capture_ticket_data, str_file, str_new_file_tmp)
#            #37364
#            if ins_general_methods.bln_enable_non_iata_capture : 
#                for ins_ticket_base in ins_general_methods.ins_global.lst_process_list_void :
#                    thread = Thread(target = ins_general_methods.create_void_json_and_upload,args = [ins_ticket_base])
#                    thread.start()
#                    thread.join()
#
#                for ins_ticket_base in ins_general_methods.ins_global.lst_process_list_void :
#                    ins_general_methods.ins_global.lst_process_list_void.remove(ins_ticket_base)
#            
#        else:
#            
#            for ins_ticket_base in ins_general_methods.ins_global.lst_process_list :
#                thread = Thread(target = ins_general_methods.create_json_and_upload,args = [ins_ticket_base])
#                thread.start()
#                thread.join()
#            pass
        
        

        for ins_ticket_base in ins_general_methods.ins_global.lst_process_list :
            ins_general_methods.ins_global.lst_process_list.remove(ins_ticket_base)
#        
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
            
        pass
    
        return str_message

    def get_tax_details(self, str_data):
        flt_total_tax = 0.00
        lst_tax = []
        flt_vat = 0.00
        flt_xt_tax = 0

        lst_tax_id = ['T1:', 'T2:', 'T3:', 'T4:', 'T5:']
        for str_tax_id in lst_tax_id:
            try:
                int_index = str_data.index(str_tax_id)
            except:
                continue
            
            str_tax_amount = str_data[int_index+3:int_index+3+8].strip()
            str_tax_code = str_data[int_index+3+8:int_index+3+8+2].strip()

            if str_tax_amount:
                
                # // For refund tax details in A23 section will be like 
                # // BF:SAR 2620.00T1:   50.00URT2:  140.00RGT3:
                # // IT:   99.00SP    8.00XZ   99.00YD
                # // Here the tax 3 is divided into individual taxes we want to capture it.
                # // refer #9845
                
                if str_tax_amount.find('IT:') != -1:
                    int_index = str_data.index('IT:')
                    
                    flt_tax_amount = 0.0
                    str_tax_code = ''
                    
                    while True:
                        str_induvidual_tax_amount = str_data[int_index+3:int_index+3+8].strip()
                        try:
                            flt_tax_amount += float(str_induvidual_tax_amount)
                        except:
                            break
                        str_tax_code += str_data[int_index+3+8:int_index+3+8+2].strip()
                        int_index = int_index+8+2
                        pass

                    str_tax_amount = str(flt_tax_amount)
                    pass
                try :
                    flt_total_tax = flt_total_tax + float(str_tax_amount)
                except :
                    break
                pass
            
            if str_tax_code == 'XT' :
                flt_xt_tax = float(str_tax_amount)
           
            if ins_general_methods.ins_auto_inv.str_input_vat_code and str_tax_code == ins_general_methods.ins_auto_inv.str_input_vat_code:
                flt_vat += float(str_tax_amount or 0)
            
            if str_tax_code :
                lst_tax.append(str_tax_code+'='+str_tax_amount)
            

        lst_xt_tax = []
        if flt_xt_tax and len(str_data.split('\r')) > 1:
            str_tax_split_up = str_data.split('\r')[1].split(':')[1]  #37022
            int_index = 10
            lst_tax_split_up = [str_tax_split_up[i:i+int_index] for i in range(0, len(str_tax_split_up), int_index)]
            for str_item in lst_tax_split_up:
                try:
                    str_tmp_tax_code = str_item.strip()[-2:]
                    flt_tmp_tax_amount = float(str_item.strip()[:-2])
                except:
                    continue
                lst_xt_tax.append(str_tmp_tax_code + '=' + str(flt_tmp_tax_amount))
                
        if lst_xt_tax :
            for str_item in lst_tax :
                if str_item.find('XT') != -1 :
                    lst_tax.remove(str_item)
                    
            lst_tax.extend(lst_xt_tax)

        return (','.join(lst_tax), str(flt_total_tax), flt_vat)
                            
    def set_base_currency(self,str_file,bln_from_web_service = False,str_file_content = ''):
        """ File will be captured only if the currency in the file is same as the currency of the system.
            In this function the currency of the file is set into a temperary variable and that variable is 
            assumed to be the base currency of the system and at the end the capturing script tests whether this currency is same as the 
            currency of the system
            """
            
        str_currency_code = ''
        
        str_tran_currency_rfd = ''
        if not bln_from_web_service:
            try:
                fd = open(str_file, 'r')
                lst_file_data = fd.readlines()
                fd.close()
            except:
                pass
        else :
            lst_file_data = str_file_content.split('***#|#|#***')
        
        str_line = ''.join(lst_file_data)
        str_line = str_line.replace('\n','\r')

        str_general_part = str_line[:343].replace('\r', '~')
            
        str_currency_code = str_general_part[161:164].strip()    
        
        lst_ticket_details = str_line[343:].replace('\r\r', '~').split('~')
        for str_ticket_details in lst_ticket_details:
                str_ticket_details = str_ticket_details.strip()
                if str_ticket_details[:3] == 'A23': # // REFUND DATA (GCS)
                    lst_ticket_details = str_ticket_details.split('A23')
                    for str_refund_data in lst_ticket_details:
                        str_refund_data = str_refund_data.strip()
                        if str_refund_data == '':
                            continue
                        str_refund_data = 'A23' + str_refund_data
                        
                        try:
                            int_index = str_refund_data.index('RA:')
                        except:
                            str_refund_amount = 0.00
                            str_tran_currency_rfd = ''
                        else:
                            str_refund_amount = str_refund_data[int_index+3+35:int_index+3+35+8].strip()
                            str_tran_currency_rfd = str_refund_data[int_index+35:int_index+3+35].strip() # refer 35497
        
        return str_currency_code or str_tran_currency_rfd
    
    
    def get_details_of_file(self, str_file):
        lst_option = ['','','' , 'I']
        try:
            fd = open(str_file, 'r')
            lst_file_data = fd.readlines()
            fd.close()
        except:
            return 'Error'
        try:
            str_line = lst_file_data[0]
            str_general_part = str_line[:343].replace('\r', '~')
            str_record_locator = str_general_part[98:104].strip()# // PNR Number, If Refund/Void = 'ZZZZZZ'
            str_ticket_status = str_general_part[253:254].strip() #42425
            if str_ticket_status in ('R','D','V','Z'): #str_record_locator == 'ZZZZZZ':
                lst_option[3] = 'R'

            lst_ticket_details = lst_file_data[0][343:].replace('\r\r', '~').split('~')

            if len(lst_ticket_details) > 1:
                lst_option[1] = "F"
            lst_ticket = []
            for str_ticket_details in lst_ticket_details:
                str_ticket_details = str_ticket_details.strip()
                if str_ticket_details[:3] == 'A02': # // PASSENGER DATA
                    lst_ticket_details = str_ticket_details.split('A02')
                    str_tmp_data = ''
                    for str_pax_data in lst_ticket_details:
                        if len(str_pax_data) <= 37 and str_tmp_data == '':
                            str_tmp_data = str_pax_data
                            continue
                        elif str_tmp_data:
                            str_pax_data = str_tmp_data + 'A02' + str_pax_data
                            pass

                        if str_pax_data == '':
                            continue
                        str_pax_data = 'A02' + str_pax_data
                        str_ticket_number = str_pax_data[48:58].strip()
                        if str_ticket_number:
                            lst_option[0] = "T"
                            lst_ticket.append(str_ticket_number)
                            pass
                        pass
                    pass
                pass
            lst_option[2] = lst_ticket
        except:
            pass
        return lst_option


if __name__ == "__main__":
    ins_capture = Capture()
    # // CREATE DIRECTORY
    

    # // Move not parsed folder files to parent folder
    #print '@@@ Move Not parsed files to parent folder'
    ins_capture.move_not_parsed_folder_files_to_parent_folder()

    #// if auto invoice is active then changing Path and Woking Directory to parent
    # //Directory to avoid problems of relative import in the case of auto invoice
    #str_current_path = os.getcwd()
    #str_module_path = str_current_path.replace( 'ticketCapture','')
    #sys.path.append( str_module_path )
    #os.chdir(str_module_path)
    #str_conf_path = 'ticketCapture/capture.conf'

    #from traacslib import ins_dictionary
    #if ins_capture.ins_capture_db.get_auto_invoice_admin_settings():
    #        from traacslib import ins_date_time
    #        ins_dictionary.initialise_dictionary()
    #        from src.autoInvoice import autoInvoiceCaptureLG
    #else:
    #        ins_dictionary.set_capturing_settings()
    #        ins_dictionary.set_admin_settings()
    #        ins_dictionary.set_branch()

    # // START TICKET CAPTURE
    ins_capture = ins_capture.ticket_capture()
