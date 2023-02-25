"""
Purpose: Wave Sabre Capture
Owner  : KISHOR PS
Date   : 01/10/2019
Re. F/M: caprure.py , ticketBase.py, Ticket Invoice and Refund
Last Update: 
"""


import sys
import os
import time
import re
import binascii
import random
import copy
import datetime
import copy

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

ins_folder_base = ins_general_methods.create_folder_structre('Sabre')
    
if 'HOME' not in os.environ:
    os.environ['HOME'] = 'C:\\'

global dct_error_messages
dct_error_messages = {}

class IsDirectory(Exception):
    pass

class DuplicationError(Exception):
    pass

class RefundVoidTicket(Exception):
    pass

class OperationalError(Exception):
    pass

class InputError(Exception):
    pass

class MirTicketData(object):
    pass

class CaptureDB:
    def __init__(self, *args):
        pass

    def convert_amount(self,flt_amount,flt_currency_roe) :
        """Convert amount to base curency """
        
        return ins_general_methods.convert_foreign_to_based_currency_amt(flt_amount, flt_currency_roe, int_roe_round = 0)
        pass    

class Capture:
    def __init__(self, *args):

        self.str_defult_currency_code = ins_general_methods.str_base_currency
        self.int_first = 1
        

    def move_not_parsed_folder_files_to_parent_folder(self, *args):
        lst_files = os.listdir(ins_folder_base.str_not_parsed_dir)
        for str_file in lst_files:
            #code to avoid file replace by moving from not parsed to parent directory
            str_new_file = str_file
            str_new_file_tmp = str_file
            try:
                str_new_file_tmp = str_new_file_tmp.replace('.PNR', '')[:70] + '_'  + datetime.datetime.now().strftime("%d%b%Y") + '_' + str(random.randint(0, 999999999)) + '.PNR'
            except:
                pass
            if os.access(os.path.join(ins_folder_base.str_directory, str_new_file), os.F_OK):
                os.rename(os.path.join(ins_folder_base.str_directory, str_new_file),os.path.join(ins_folder_base.str_directory, str_new_file_tmp))
            if sys.platform == "win32":
                os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_not_parsed_dir, str_file), os.path.join(ins_folder_base.str_directory, str_file)))
            else:
                os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_not_parsed_dir, str_file), os.path.join(ins_folder_base.str_directory, str_file)))
                pass
            pass
        pass

    def ticket_capture(self, *args):

            #self.move_not_parsed_folder_files_to_parent_folder()
            
#        while True:
            lst_files = os.listdir(ins_folder_base.str_directory)

            for str_file in lst_files:
                if str_file == 'QWORK.SF':
                    continue
                    
                str_directory_file_name = os.path.join(ins_folder_base.str_directory, str_file)
                
                if os.path.isdir(str_directory_file_name):
                    continue
                    
                str_new_file = str_file
                str_new_file_tmp = str_file
                
                try:
                    str_new_file_tmp = str_new_file_tmp.replace('.PNR', '')[:70] + '_'  + datetime.datetime.now().strftime("%d%b%Y") + '_' + str(random.randint(0, 999999999)) + '.PNR'
                    
                except:
                    pass
                
                
                try:
                    
                    self.extract_ticket_data_from_file(os.path.join(ins_folder_base.str_directory, str_file))
                except IsDirectory:
                    continue
                except InputError:
                    continue
                except DuplicationError:
                    # // move file to parsed directory
                    if os.access(os.path.join(ins_folder_base.str_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                    
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                    pass
                except RefundVoidTicket:
                    if str_file in ins_general_methods.ins_global.dct_not_parsed_files:
                        if os.access(os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file), os.F_OK):
                            str_new_file = str_new_file_tmp
                        
                        if sys.platform == "win32":
                            os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                        else:
                            os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_not_parsed_dir, str_new_file)))
                        ins_general_methods.ins_global.dct_not_parsed_files.pop(str_file)
                    else:
                        ins_general_methods.ins_global.dct_not_parsed_files[str_file] = None    
                        
                except OperationalError:
                    
                    if str_file in ins_general_methods.ins_global.dct_not_parsed_files:
                        
                        if str_directory_file_name in ins_general_methods.ins_global.dct_no_ticket_files :
                            str_move_to_dir = ins_folder_base.str_no_ticket_files_dir
                            ins_general_methods.ins_global.dct_no_ticket_files.pop(str_directory_file_name)
                        else :
                            str_move_to_dir = ins_folder_base.str_not_parsed_dir
                        
                        
                        if os.access(os.path.join(str_move_to_dir, str_new_file), os.F_OK):
                            str_new_file = str_new_file_tmp
                        
                        if sys.platform == "win32":
                            os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))
                        else:
                            os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))

                        ins_general_methods.ins_global.dct_not_parsed_files.pop(str_file)
                    else:
                        ins_general_methods.ins_global.dct_not_parsed_files[str_file] = None

                except Exception as msg:
                    
                    if str_file in ins_general_methods.ins_global.dct_not_parsed_files:

                        if str_directory_file_name in ins_general_methods.ins_global.dct_no_ticket_files :
                            str_move_to_dir = ins_folder_base.str_no_ticket_files_dir
                            ins_general_methods.ins_global.dct_no_ticket_files.pop(str_directory_file_name)
                        else :
                            str_move_to_dir = ins_folder_base.str_not_parsed_dir

                        if os.access(os.path.join(str_move_to_dir, str_new_file), os.F_OK):
                            str_new_file = str_new_file_tmp
                        
                        if sys.platform == "win32":
                            os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))
                        else:
                            os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(str_move_to_dir, str_new_file)))
                        ins_general_methods.ins_global.dct_not_parsed_files.pop(str_file)
                    else:
                        ins_general_methods.ins_global.dct_not_parsed_files[str_file] = None

                else:
                    
                    if os.access(os.path.join(ins_folder_base.str_parsed_dir, str_new_file), os.F_OK):
                        str_new_file = str_new_file_tmp
                    # // move file to parsed directory
                    if sys.platform == "win32":
                        os.system('move "%s" "%s"'%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                    else:
                        os.system("mv '%s' '%s'"%(os.path.join(ins_folder_base.str_directory, str_file), os.path.join(ins_folder_base.str_parsed_dir, str_new_file)))
                    pass
                pass

            time.sleep(ins_general_methods.ins_capture_base.int_sleep_time)

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
            pass
        
    def set_base_currency(self,str_file,bln_from_web_service = False, str_file_content = ''):
        """ File will be captured only if the currency in the file is same as the currency of the system.
            In this function the currency of the file is set into a temperary variable and that variable is 
            assumed to be the base currency of the system and at the end the capturing script tests whether this currency is same as the 
            currency of the system
            """
            
        str_currency_code = ''
        
        if not bln_from_web_service :
            try:
                fd = open(str_file, 'r')
                lst_file_data = fd.readlines()
                fd.close()

            except:
                return str_currency_code
                pass
        else :
            lst_file_data = str_file_content.split('***#|#|#***')
        
        if len(lst_file_data) > 1:
                str_line = ''.join(lst_file_data)
                str_line = str_line.replace('\r\n','\r')
                str_line = str_line.replace('\n','\r')
        else:
            str_line = lst_file_data[0]
        if not str_line.strip():
            return str_currency_code
        
        
        mess_id = '\rM2'
        m1_pos = str_line.find(mess_id)
        if m1_pos != -1 :
            str_line = str_line[m1_pos+1 :]
        else : return str_currency_code
        
        while 1 :
            try :    
                str_currency_code = str_line[76:79].strip() or str_line[34:37].strip()
                break
            except :
                break
            
        return str_currency_code
        
        
    def extract_ticket_data_from_file(self, str_file ,bln_from_web_service = False, str_file_content = '', int_count = 0,bln_start=False):
        # // get file data
        if bln_from_web_service and int_count:
            ins_general_methods.ins_global.dct_not_parsed_files[str_file] = ''
        if ins_general_methods.dct_conf_data['TRAACS_VERSION'] == 'SAAS':
            ins_save_or_update_data = saveOrUpdateData.captureDB()
        else:
            ins_save_or_update_data = saveOrUpdateDataWave.captureDB()
        ins_create_ticket_base = createTicketBaseInstance.createInstance()
        lst_tickets = []
        lst_ticket_capture_details = []
#        if bln_start :
#            ins_general_methods.reload_data()
#            try :
#                ins_general_methods.set_non_iata_capture_details()
#            except :
#                ins_general_methods.connect_db()
#                ins_general_methods.set_non_iata_capture_details()
            
        if not  bln_from_web_service  :
            try:
                fd = open(str_file, 'r')
            except IOError:

                raise IsDirectory(str_file + ' Is a directory')
                pass
            (str_directory,str_file_name) = os.path.split(str_file)
            
            lst_file_data = fd.readlines()
            fd.close()
            
            if len(lst_file_data) > 1:
                str_line = ''.join(lst_file_data)
                str_line = str_line.replace('\r\n','\r')
                str_line = str_line.replace('\n','\r')
            else:
                str_line = lst_file_data[0]
                
            str_line = str_line.lstrip('\r')
            
        else :
            str_file_name = str_file
            lst_file_data = str_file_content.split('***#|#|#***')
            str_line = str_file_content

        str_currency_code = ''
        if ins_general_methods.ins_capture_base.bln_multi_currency:
            str_currency_code = self.set_base_currency(str_file ,bln_from_web_service , str_file_content)
        if str_currency_code:
            self.str_defult_currency_code = str_currency_code
        
        self.lst_months = ["", "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                                "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        

        ins_capture_ticket_data = instanceBase.CaptureBase()
        ins_capture_ticket_data.str_ins_db_id = id(ins_general_methods.ins_db)
        ins_capture_ticket_data.str_crs_company = 'Sabre'
        # // get capture ticket data ins
        ins_capture_ticket_data.dct_pax_name = {}
        ins_capture_ticket_data.bln_sector_start = False
        ins_capture_ticket_data.bln_got_fare_basis = False
        ins_capture_ticket_data.int_sectors_total_count = -1
        ins_capture_ticket_data.lst_agent_details = []
        ins_capture_ticket_data.lst_order_details = []
        ins_capture_ticket_data.lst_temp_sector_details = []
#        ins_general_methods.dct_pax_wise_remark = {} 
        ins_capture_ticket_data.str_file_name = str_file_name
        str_message = ''
        mess_id = None
        ins_capture_ticket_data.str_defult_currency_code =  self.str_defult_currency_code
        try:
            int_count = 1
            #9340 - Checking if File consist of multiple lines.then replace '\n' from file
            
            while 1:
                if int_count == 10:
                    break

                if not str_line.strip():
                    continue
                if str_line[:2] == "AA" and str_line[11:13] == "M0":
                    ins_capture_ticket_data = self.parse_line(str_line,ins_capture_ticket_data)
                    if ins_capture_ticket_data.bln_void :
                        try:
                            rst = ins_general_methods.get_ticket_details(ins_capture_ticket_data.str_ticket_number)
                            if not rst:
                                print ("Can't Save sabre void file without issuance...!!!")
                                raise Exception("Can't Save sabre void file without issuance...!!!")
                            else:
                                ins_capture_ticket_data.str_void_date = ins_capture_ticket_data.str_ticket_refund_date #format - '%d/%m/%Y'
                                if ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS':
                                    str_conjection = rst[0]['vchr_last_conjection_ticket_number'] #spelling change in saas
                                else:
                                    str_conjection = rst[0]['vchr_last_conjunction_ticket_number']
                                    
                                ins_capture_ticket_data.lst_ticket_detls.append([
                                            ins_capture_ticket_data.str_ticket_number,
                                            str_conjection,
                                            'ET',
                                            '',
                                            '',
                                            '',
                                            '',
                                            0.0,
                                            '',
                                            '',
                                            ''
                                            ])
                                lst_ticket_capture_details , lst_tickets = ins_create_ticket_base.create_ticket_data_to_save(ins_capture_ticket_data , str_file)
                                if lst_ticket_capture_details not in (None,[],[None]):
                
                                    ins_save_or_update_data.save_captured_ticket_data(lst_ticket_capture_details)
                                    if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                        ins_general_methods.ins_db.commit()
                                    else:
                                        ins_general_methods.ins_db.rollback()
                                        raise Exception('Database instance commit failed')
                                    if lst_tickets:
                                        print(('Saved Sabre Tickets ' + ', '.join(lst_tickets)))
                                        str_message += '\n' + 'Saved Sabre Tickets ' + ', '.join(lst_tickets)

                            #37364
                            if ins_general_methods.bln_enable_non_iata_capture :
                                for ins_ticket_base_void in ins_general_methods.ins_global.lst_process_list_void :
                                    thread = Thread(target = ins_general_methods.create_void_json_and_upload,args = [ins_ticket_base_void])
                                    thread.start()
                                    thread.join()

                                for ins_ticket_base_void in ins_general_methods.ins_global.lst_process_list_void :
                                    ins_general_methods.ins_global.lst_process_list_void.remove(ins_ticket_base_void)
                            
                            #save auto invoice
                            ins_save_or_update_data.save_auto_invoice_data(ins_capture_ticket_data, lst_tickets, lst_ticket_capture_details)

                        except Exception as msg:
                            ins_general_methods.ins_db.rollback()
                            raise RefundVoidTicket(" Cannot parse refund files without issue side")
                        else:
                            print ("\nvoid ticket : %s  refund date %s\n"%(ins_capture_ticket_data.str_ticket_number,ins_capture_ticket_data.str_ticket_refund_date))

                            if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                                ins_general_methods.ins_db.commit()
                            else:
                                ins_general_methods.ins_db.rollback()
                                raise Exception('Database instance commit failed')
                            return str_message
                        
                    if ins_capture_ticket_data.bln_refund:

                        raise RefundVoidTicket(" This is refund ticket")
                        return
                    mess_id = 'M' + str(int_count)
                    m1_pos = str_line.find(mess_id)
                    temp_mess_id = 'M' + str(int_count + 1)
                    temp_mess_pos = str_line.find('\r'+temp_mess_id)
                    # Some time there is M1 like hedears in data

                    if temp_mess_pos !=-1:
                        temp_mess_pos = temp_mess_pos + 1
                        chr_temp_char = str_line[temp_mess_pos -1 :temp_mess_pos]
                        if chr_temp_char == '\r':
                            if temp_mess_pos < m1_pos :
                                mess_id = temp_mess_id
                                int_count = int_count + 1
                                continue

                    if m1_pos != -1 and str_line[m1_pos -1 :m1_pos] == '\r':

                        str_line = str_line[m1_pos:]
                        ins_capture_ticket_data = self.parse_line(str_line,ins_capture_ticket_data)
                        str_line = str_line[2:]
                    elif m1_pos !=-1 and  str_line[m1_pos -1 :m1_pos] != '\r':
                        str_line = str_line[m1_pos + 2:]# some time heading may come in data part
                        continue
                    else: #18642
                        int_count = int_count + 1
                        if int_count == 10:
                            break

                else:
                    if not mess_id:
                        raise OperationalError ("No mess_id")


                    m1_pos = str_line.find('\r'+mess_id)
                    if m1_pos != -1 :
                        m1_pos +=1
                        
                    temp_mess_id = 'M' + str(int_count + 1)
                    #// Ref No - 11024- Sabre Tickets are not captured
                    #// Searching of M1, M2, M3 must include \r followed by  M1, M2, M3 
                    temp_mess_pos = str_line.find('\r'+temp_mess_id)
                    if temp_mess_pos != -1:
                        temp_mess_pos = temp_mess_pos + 1
                        chr_temp_char = str_line[temp_mess_pos -1 :temp_mess_pos]
                        if temp_mess_pos !=-1:
                            chr_temp_char = str_line[temp_mess_pos -1 :temp_mess_pos]
                            if chr_temp_char == '\r':

                                if temp_mess_pos < m1_pos:
                                    mess_id = temp_mess_id
                                    int_count = int_count + 1
                                    continue

                    if m1_pos != -1 and str_line[m1_pos -1 :m1_pos] == '\r':

                        str_line = str_line[m1_pos:]
                        ins_capture_ticket_data = self.parse_line(str_line,ins_capture_ticket_data)

                        str_line = str_line[2:]
                    elif m1_pos != -1 and str_line[m1_pos -1 :m1_pos] != '\r':
                        str_line = str_line[m1_pos + 2:]# some time heading may come in data part
                        continue

                    else:
                        int_count = int_count + 1
                        if int_count == 10:
                            #13684 - EMD capture
                            if ins_capture_ticket_data.bln_emd :
                                
                                lst_index = [m.start() for m in re.finditer('\rMG', str_line)]
#                                m1_pos = str_line.find('\rMG')
                                
                                for m1_pos in lst_index :
                            
#                                    str_line = str_line[m1_pos+1:]
                                    
                                    ins_capture_ticket_data = self.parse_line(str_line[m1_pos+1:],ins_capture_ticket_data)
                                    
                            if [m.start() for m in re.finditer('\rMA', str_line)]:
                                        
                                lst_index = [m.start() for m in re.finditer('\rMA', str_line)]
                                
                                for m1_pos in lst_index :
                                    ins_capture_ticket_data = self.parse_line(str_line[m1_pos+1:],ins_capture_ticket_data)
                            
                                        
                            if [m.start() for m in re.finditer('\rMX', str_line)]:

                                lst_index = [m.start() for m in re.finditer('\rMX', str_line)]

                                for m1_pos in lst_index :
                                    ins_capture_ticket_data = self.parse_line(str_line[m1_pos+1:],ins_capture_ticket_data)
                            break
                            
                    mess_id = 'M' + str(int_count)
            
#            ticket_issue_date  = ins_capture_ticket_data.str_ticket_issue
            ticket_issue_date  = ins_capture_ticket_data.str_mir_rec_created_date or ins_capture_ticket_data.str_ticket_issue
            issue_day = ticket_issue_date[:2]
            issue_month = ticket_issue_date[2:]
            int_issue_month = self.lst_months.index(issue_month)
            int_dep_month = self.lst_months.index(ins_capture_ticket_data.str_first_dep_month)
            ticket_booking_date = ins_capture_ticket_data.str_pnr_creation_date # PNR creation date is taken as booking date
            booking_day = ticket_booking_date[:2]
            booking_month = ticket_booking_date[2:]
            int_booking_month = self.lst_months.index(booking_month)

#            if int_issue_month > int_dep_month: #pnr month dec and dep month = jan
#                                          #like that
#                int_issue_year = int(ins_capture_ticket_data.str_flight_year) - 1
#            else:
#                int_issue_year = int(ins_capture_ticket_data.str_flight_year)
            current_year = datetime.datetime.now().strftime("%d/%m/%Y").split('/')[2]
            
            #37216
            if ins_capture_ticket_data.str_first_travel_year and ins_capture_ticket_data.str_flight_year != ins_capture_ticket_data.str_first_travel_year:
                ins_capture_ticket_data.str_flight_year = ins_capture_ticket_data.str_first_travel_year
                
            if not ins_capture_ticket_data.str_flight_year:#refer 22023
                ins_capture_ticket_data.str_flight_year = current_year
            if int(ins_capture_ticket_data.str_flight_year) > int(current_year):
                str_issue_year = str(current_year)
                str_booking_year = str(current_year)
            else:
                str_issue_year = ins_capture_ticket_data.str_flight_year
                str_booking_year = ins_capture_ticket_data.str_flight_year

            #departure date
            dep_day = ins_capture_ticket_data.str_first_departure_date or issue_day
            dep_month = ins_capture_ticket_data.str_first_dep_month
            int_dep_month = self.lst_months.index(dep_month) or int_issue_month
            dep_year = ins_capture_ticket_data.str_flight_year
            #converting departure date to common form '%d%b' eg:19MAR
            ins_capture_ticket_data.str_first_departure_date = datetime.datetime.strptime(dep_day.zfill(2) + str(int_dep_month).zfill(2) + dep_year,'%d%m%Y')
            ins_capture_ticket_data.str_first_departure_date = datetime.datetime.strftime(ins_capture_ticket_data.str_first_departure_date,'%d%b').upper()

            #rechecking Issue Date is Less than Travel Date
            if int_dep_month < int_issue_month and int(str_issue_year) == int(ins_capture_ticket_data.str_flight_year):
                    str_issue_year = str(int(str_issue_year) - 1)
            elif int_dep_month == int_issue_month and int(dep_day) < int(issue_day) and int(str_issue_year) == int(ins_capture_ticket_data.str_flight_year):
                str_issue_year = str(int(str_issue_year) - 1)
            else:
                str_issue_year = str_issue_year
                pass

            #rechecking Booking Date is Less than Travel Date
            if int_dep_month < int_booking_month and int(str_booking_year) == int(ins_capture_ticket_data.str_flight_year):
                    str_booking_year = str(int(str_booking_year) - 1)
            elif int_dep_month == int_booking_month and int(dep_day) < int(booking_day) and int(str_booking_year) == int(ins_capture_ticket_data.str_flight_year):
                str_booking_year = str(int(str_booking_year) - 1)
            else:
                str_booking_year = str_booking_year
                pass
            if ins_capture_ticket_data.dct_refund_data:
                for lst_refund_key in ins_capture_ticket_data.dct_refund_data.values():
                    lst_refund_key[1] = str(issue_day).zfill(2) + '/' + str(int_issue_month).zfill(2) + '/' + str(str_issue_year)
#            ins_capture_ticket_data.dat_ticket_issue = issue_day + '/' + str(int_issue_month) + '/' + str_issue_year
#            ins_capture_ticket_data.dat_ticket_issue = issue_day + '/' + str(int_issue_month) + '/' + str_issue_year
            ins_capture_ticket_data.str_file_creation_date = str_issue_year[2:] + str(int_issue_month).zfill(2) + issue_day.zfill(2)
            ins_capture_ticket_data.str_pnr_creation_date = str_booking_year[2:] + str(int_booking_month).zfill(2) + booking_day.zfill(2)
            ###############33
            
#            ins_capture_ticket_data.str_ticket_category = 'S'
#            ins_capture_ticket_data.str_ticket_type =  'ET'
            
            if 'REFUND' in ins_capture_ticket_data.dct_refund_tickets :
                print ("Can't Save sabre refund file without issuance...!!!")
                raise Exception("Can't Save sabre refund file without issuance...!!!")
            
            if ins_capture_ticket_data.dct_refund_tickets :
                for str_ref_ticket_number,lst_ref_values in  ins_capture_ticket_data.dct_refund_tickets.items() :
                    if lst_ref_values[0]=='D' :
                        print('Duplication  Sabre refund Tickets ' + str_ref_ticket_number)
                        str_message += '\n' + 'Saved Sabre Tickets ' + str_ref_ticket_number
            
            lst_ticket_capture_details , lst_tickets = ins_create_ticket_base.create_ticket_data_to_save(ins_capture_ticket_data , str_file)
            if lst_ticket_capture_details not in (None,[],[None]):
                
                ins_save_or_update_data.save_captured_ticket_data(lst_ticket_capture_details)
                if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                    ins_general_methods.ins_db.commit()
                else:
                    ins_general_methods.ins_db.rollback()
                    raise Exception('Database instance commit failed')
                if lst_tickets:
                    print(('Saved Sabre Tickets ' + ', '.join(lst_tickets)))
                    str_message += '\n' + 'Saved Sabre Tickets ' + ', '.join(lst_tickets)
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
            
            lst_hv = []
            lst_car = []
            lst_other = []
            if ins_capture_ticket_data.lst_voucher_numbers: #43565
                lst_hv,lst_car,lst_other = ins_create_ticket_base.create_voucher_data_to_save(ins_capture_ticket_data,str_file_name)
                lst_hotel_vouchers = []
                if lst_hv not in (None,[],[None]):
                    lst_hotel_vouchers = ins_save_or_update_data.save_captured_hotel_voucher_data(lst_hv)
                    if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                        ins_general_methods.ins_db.commit()
                    else:
                        ins_general_methods.ins_db.rollback()
                        raise Exception('Database instance commit failed')
                    if lst_hotel_vouchers:
                        print ('Saved Hotel Vouchers ' + ', '.join(lst_hotel_vouchers) )
                        str_message += '\n' + 'Saved Hotel Vouchers ' + ', '.join(lst_hotel_vouchers)

                lst_car_vouchers = []
                if lst_car not in (None,[],[None]):
                    lst_car_vouchers = ins_save_or_update_data.save_captured_car_voucher_data(lst_car)
                    if ins_capture_ticket_data.str_ins_db_id and ins_capture_ticket_data.str_ins_db_id == id(ins_general_methods.ins_db) :
                        ins_general_methods.ins_db.commit()
                    else:
                        ins_general_methods.ins_db.rollback()
                        raise Exception('Database instance commit failed')
                    if lst_car_vouchers:
                        print ('Saved Car Vouchers ' + ', '.join(lst_car_vouchers))
                        str_message += '\n' + 'Saved Car Vouchers ' + ', '.join(lst_car_vouchers)
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

            lst_keys = list(ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise.keys())
            for key in lst_keys :
                ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise.pop(key)

            lst_keys = list(ins_general_methods.ins_global.dct_child_extra_earning_sector_wise.keys())
            for key in lst_keys :
                ins_general_methods.ins_global.dct_child_extra_earning_sector_wise.pop(key)

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

        except DuplicationError as msg:
            raise DuplicationError("Duplication")

        except Exception as msg:
            try:
                str_file_name = os.path.split(str_file)[1].split('.')[0]
                lst_option = self.get_details_of_file(str_file)
                if lst_option == 'Error':
                    if str_file_name + ':' not in dct_error_messages:
                        dct_error_messages[str(str_file_name) + ':'] = ['', '', '', 'Error while opening file']
                elif lst_option[1] != "F":
                    if str_file_name + ':' not in dct_error_messages:
                        dct_error_messages[str(str_file_name) + ':'] = ['','','The GDS File Format given is wrong']
                elif lst_option[0] != "T":
                    if str_file_name + ':' not in dct_error_messages:
                        ins_general_methods.ins_global.dct_no_ticket_files[str_file] = 'No ticket No'
                        dct_error_messages[str(str_file_name) + ':'] = ['','','The GDS has no Ticket No']
                else:
                    for int_ticket_count in range(len(lst_option[2])):
                        str_file_name_ticket_num = str(str_file_name) + ':' + str(lst_option[2][int_ticket_count])
                        if str_file_name_ticket_num not in dct_error_messages:
                            str_line_no = str(sys.exc_info()[2].tb_lineno)
                            str_message = str(msg)
                            dct_error_messages[str_file_name_ticket_num] = [str(lst_option[2][int_ticket_count]), str_line_no, str_message]
                            pass
            except :
                pass
#            print msg
            raise Exception(msg)  
        
        #save auto invoice
        ins_save_or_update_data.save_auto_invoice_data(ins_capture_ticket_data, lst_tickets, lst_ticket_capture_details)
        
        return str_message
    
    def parse_line(self,str_line,ins_capture_ticket_data):
        #constant data
        try:
            if str_line[:2] == "AA" and str_line[11:13] == "M0":


                if str_line[13:14].strip() in ('5','C')  : #void


                    str_ticket_number = str_line[29:40].strip()

                    no_of_tickets = str_line[40:42]
                    ins_capture_ticket_data.str_pnr_no = str_line[42:50].strip()
                    ins_capture_ticket_data.bln_void = True
                    str_ticket_issue_day = str_line[2:4]
                    str_ticket_issue_month = str_line[4:7]
                    ins_capture_ticket_data.str_ticket_issue = str_line[2:7]
                    ins_capture_ticket_data.str_ticket_number = str_ticket_number
                    int_issue_month = self.lst_months.index(str_ticket_issue_month)
                    issue_year = datetime.datetime.now().strftime("%d/%m/%Y").split('/')[2]
                    ins_capture_ticket_data.str_ticket_refund_date = str_ticket_issue_day.zfill(2) + '/' + str(int_issue_month).zfill(2) + '/' + issue_year
                    return ins_capture_ticket_data

                elif str_line[13:14] == 'D':

                    ins_capture_ticket_data.bln_refund = True

                    #ins_capture_ticket_data.str_refund_date = str_line[2:7]
                    return ins_capture_ticket_data

                else:

                    if str_line[13:14] == 'A' and not ins_capture_ticket_data.bln_emd:
                        ins_capture_ticket_data.bln_emd = True

                    elif str_line[13:14] == 'B' and not ins_capture_ticket_data.bln_emd_and_ticket:
                        # There is emd ticket and normal ticket in the file.
                        ins_capture_ticket_data.bln_emd = True
                        ins_capture_ticket_data.bln_emd_and_ticket = True
                    elif str_line[13:14] == '2':
                        ins_capture_ticket_data.bln_advance_deposit_file = True


                        """
                                        A = INVOICE/EMD
                                        B = INVOICE/TICKET/EMD
                                        C = VOID EMD

                            If the Transaction Type = A, INVOICE/EMD the M5
                            and MG records will be created.
                            If the Transaction Type = B, INVOICE/TICKET/EMD
                                             the M2, M4, and M5 for the e-ticket as well as a M5 and
                                            MG for EMD will be created.

                        """


                    ins_capture_ticket_data.str_ticket_issue = str_line[2:7]
                    ins_capture_ticket_data.invoice_number = str_line[36:43]
                    iata_no = str_line[43:53]
                    iata_no = iata_no.replace(' ', '')
                    iata_no = iata_no.strip()
                    ins_capture_ticket_data.str_booking_agency_iata_no = iata_no
                    ins_capture_ticket_data.str_ticketing_agency_iata_no = iata_no
                    ins_capture_ticket_data.str_pnr_no = str_line[53:61].strip()
                    ins_capture_ticket_data.str_booking_agent = str_line[88:98].strip()
#                    ins_capture_ticket_data.str_booking_agent_code = str_line[93:95].strip() #contains *
                    ins_capture_ticket_data.str_booking_agent_code = str_line[95:98].strip()
                    ins_capture_ticket_data.str_ln_iata = str_line[98:106].strip()
                    ins_capture_ticket_data.str_pnr_creation_date = str_line[116:121] #DDMMM in m3
                    ins_capture_ticket_data.pnr_creatd_time = str_line[121:126] #in m3
                    ins_capture_ticket_data.str_ticketing_agent = str_line[126:136].strip()
                    ins_capture_ticket_data.str_ticketing_agent_duty_code = str_line[131:133].strip()
                    ins_capture_ticket_data.str_ticketing_agent_code = str_line[133:136].strip()
                    ins_capture_ticket_data.departure_date = str_line[141:146] #DDMMM
                    ins_capture_ticket_data.origin_city_code = str_line[146:149]
                    ins_capture_ticket_data.origin_city_name = str_line[149:166]
                    ins_capture_ticket_data.dest_city_code = str_line[166:169]
                    ins_capture_ticket_data.dest_city_name = str_line[169:186]
    #                no_of_passangers = str_line[186:189]
    #                no_of_ticketed_passangers= str_line[189:192]
                    ins_capture_ticket_data.str_mir_rec_created_time = str_line[220 : 225]
                    ins_capture_ticket_data.str_mir_rec_created_date = str_line[225 : 230]
                    ins_capture_ticket_data.str_booking_agency_office_id =  str_line [88:92].strip()
                    ins_capture_ticket_data.str_ticketing_agency_office_id = str_line [126:131].strip()


            elif str_line[:2] == 'MG' :
                #refer #12811 
                lst_data = str_line.split("\r")

                str_pax_name = ins_capture_ticket_data.dct_pax_name.get(int(lst_data[0][2:4].strip()),'') or ins_capture_ticket_data.dct_pax_name.get(1,'')
                str_emd_passenger_type = lst_data[0][4:7].strip()

                if str_emd_passenger_type and str_emd_passenger_type[0] == "C":
                    if str_emd_passenger_type[1:].isdigit():

                        chd_age = int(str_emd_passenger_type[1:])
                        if chd_age < 2: #36195
                            str_emd_passenger_type = "INF"
                        else:
                            str_emd_passenger_type = "CHD"

                if str_emd_passenger_type not in ('ADT','CHD','INF') :
                    str_emd_passenger_type = 'ADT'
                str_emd_issue_date = lst_data[0][7:16]       
                str_emd_creation_date = lst_data[0][7:16].strip()
                str_emd_creation_time = lst_data[0][16:20].strip()
                str_emd_airline_char_code = lst_data[0][22:24].strip()
                str_emd_airline_numeric_code = lst_data[0][25:28].strip()
                str_emd_ticket_number = lst_data[0][28:38].strip()
                int_emd_conj_ticket_count = lst_data[0][39:40].strip()
                str_emd_connection_ticket_airline_num_code = lst_data[0][40:43].strip()
                str_emd_connection_ticket_number = lst_data[0][43:53].strip()

                str_emd_type = lst_data[0][54:55].strip()
                str_emd_fare_calculation_indicator = lst_data[0][60:61].strip()
                str_emd_connection_ticket_number = lst_data[0][43:53].strip()
                str_emd_tour_code = lst_data[0][73:88].strip()
                str_emd_total_fare_currency = lst_data[0][88:91].strip()

                try :
                    flt_emd_fare = float(lst_data[0][91:109].strip())
                except :
                    flt_emd_fare = 0.00

                try :    
                    flt_emd_tax = float(lst_data[0][109:127].strip())
                except :
                    flt_emd_tax = 0.0


                if str_emd_total_fare_currency != ins_capture_ticket_data.str_defult_currency_code:
                    str_emd_total_fare_currency = lst_data[0][145:149].strip()

                    try :
                        flt_emd_fare = float(lst_data[0][148:166].strip())
                    except :
                        flt_emd_fare = 0.00


                str_emd_payment_type = lst_data[0][166:168].strip()
                str_emd_cc_type = lst_data[0][168:170].strip()
                str_emd_cc_no = lst_data[0][171:189].strip()
                str_emd_cc_exp_date = lst_data[0][193:197].strip()
                str_emd_cc_approval_code = lst_data[0][201:210].strip()
                int_emd_count = lst_data[0][236:238].strip()


                #36789
                str_emd_tax_split_code = lst_data[3][20:23].strip()
                str_emd_tax_split_fare = float(lst_data[3][23:41].strip() or 0)
                str_emd_remarks = lst_data[3][73:123].strip()
                lst_emd_tax_details = []
                if str_emd_tax_split_fare <= flt_emd_tax :
                    lst_emd_tax_details.append((str(flt_emd_tax),str_emd_tax_split_code,''))
                
                ins_capture_ticket_data.dct_emd_ticket_details[str_emd_ticket_number] = [
                                                        str_emd_ticket_number,
                                                        str_pax_name,
                                                        str_emd_passenger_type,
                                                        flt_emd_fare,
                                                        flt_emd_tax,
                                                        str_emd_total_fare_currency,
                                                        lst_emd_tax_details,
                                                        str_emd_remarks,
                                                        str_emd_connection_ticket_number,
                                                        str_emd_cc_type ,
                                                        str_emd_cc_no ,
                                                        str_emd_cc_approval_code ,
                                                        str_emd_issue_date
                                                        ]

                str_emd_last_conj_ticket = ''
                try :
                    if int(int_emd_conj_ticket_count) :
                        int_emd_conj_ticket_count = int(int_emd_conj_ticket_count)
                        str_emd_last_conj_ticket = str(int(str_emd_ticket_number)+int_emd_conj_ticket_count)

                except :
                    pass
                # refer 25702                                    
#                ins_capture_ticket_data.dct_ticket_details[str_emd_ticket_number] = [str_emd_ticket_number ,
#                                                    str_emd_last_conj_ticket ,
#                                                    str_emd_airline_char_code ,
#                                                    str_pax_name ,
#                                                    str_emd_passenger_type ,
#                                                    flt_emd_fare ,
#                                                    flt_emd_fare ,
#                                                    flt_emd_tax ,
#                                                    lst_emd_tax_details ,
#                                                    str_emd_tax_details , #36789
#                                                    str_emd_total_fare_currency ,
#                                                    '' ,
#                                                    str_emd_total_fare_currency ,
#                                                    '' ,
#                                                    str_emd_cc_approval_code,
#                                                    0,
#                                                    0,
#                                                    0,
#                                                    0,
#                                                    ''
#                                                    ]
                ins_capture_ticket_data.lst_ticket_detls.append([str_emd_ticket_number ,
                                                    str_emd_last_conj_ticket ,
                                                    'EMD' ,
                                                    str_emd_airline_char_code ,
                                                    str_emd_airline_numeric_code ,#airline numeric code
                                                    str_pax_name ,
                                                    str_emd_passenger_type ,
                                                    0.0 ,#service fee
                                                    '' ,#amadeus i section
                                                    '' ,#amadeus i section
                                                    0 #due to no pax item number
                                                    ])

            elif str_line[:2] == 'M1':


                int_interface_no = str_line[2:4].strip()
                str_pax_name = str_line[4:68].strip() #surname/First Name
                ins_capture_ticket_data.dct_pax_name[int(int_interface_no)] = str_pax_name
            elif str_line[:2] == 'M2':
                tax1_id = 'T1'
                flt_tax1= 0.0
    #            str_tax1_code = ''
                tax2_id = 'T2'
                flt_tax2 = 0.0
    #            str_tax2_code = ''
                tax3_id = 'T3'
                flt_tax3 = 0.0
    #            str_tax3_code = 'T3'
                flt_total_tax = 0.0
    #            flt_total_fare_amount = 0.0
                int_interface_no = str_line[2:4].strip()
                str_pax_type = str_line[4:7]
                if str_pax_type[0] == "C":
                    if str_pax_type[1:].isdigit():

                        chd_age = int(str_pax_type[1:])
                        if chd_age < 2: #36195
                            str_pax_type = "INF"
                        else:
                            str_pax_type = "CHD"
                if str_pax_type == "CNN": #35677
                     str_pax_type = "CHD"

                if str_pax_type not in ('ADT','CHD','INF') :
                    str_pax_type = 'ADT'
                if str_line[18:19] == "X":
                    str_int_dom = "INT"
                else:
                    str_int_dom = "DOM"

                ins_capture_ticket_data.str_pnr_first_owner_office_id = str_line[186:190].strip()
                ins_capture_ticket_data.str_pnr_current_owner_office_id = str_line[196:201].strip()

                ins_capture_ticket_data.str_suppression_indicator = str_line[32:33].strip()

                str_fare_currency_code = str_line[34:37].strip() 

                try :
                    flt_base_fare = float(str_line[37:45].strip())
                except :
                    flt_base_fare = 0.00


                try:
                    flt_tax1 = float(str_line[45:53].strip())
                except ValueError:
                    #tax id before tax amount
                    #print "tax1 " ,str_line[45:53].strip()
                    str_tax = str_line[45:53].strip().replace(' ', '0')      #refer #40310
                    try:
                        if str_tax.strip():
                            p = re.compile("(\D+)")
                            str_part = p.search(str_tax).groups()[0]
                            str_part = str_part.strip()
                            tax1_id = str_part
                            intex = str_tax.find(str_part)
                            flt_tax1 = str_tax[intex + len(str_part):]
                            flt_tax1 = float(flt_tax1)
                    except:
                        flt_tax1 = 0.0

                except:
                    flt_tax1 = 0.0

                tax1_code = str_line[53:55].strip()
                flt_total_tax  = flt_total_tax  + flt_tax1
                try:

                    flt_tax2 = float(str_line[55:63].strip())

                except ValueError:
                    #print "tax2 ",str_line[55:63].strip()
                    str_tax = str_line[55:63].strip().replace(' ', '0')     
                    try:
                        p = re.compile("(\D+)")
                        str_part = p.search(str_tax).groups()[0]
                        str_part = str_part.strip()
                        tax2_id = str_part
                        intex = str_tax.find(str_part)
                        flt_tax2 = str_tax[intex + len(str_part):]
                        flt_tax2 = float(flt_tax2)
                    except:
                        flt_tax2 = 0.0

                except:
                    flt_tax2 = 0.0

                tax2_code = str_line[63:65].strip()
                flt_total_tax = flt_total_tax + flt_tax2
                try:
                    flt_tax3 = float(str_line[65:73].strip())
                except ValueError:

                    #print "tax3 ",str_line[65:73].strip()
                    str_tax = str_line[65:73].strip().replace(' ', '0')
                    try:

                        p = re.compile("(\D+)")
                        str_part = p.search(str_tax).groups()[0]
                        str_part = str_part.strip()
                        tax3_id = str_part
                        intex = str_tax.find(str_part)
                        flt_tax3 = str_tax[intex + len(str_part):]
                        flt_tax3 = float(flt_tax3)
                    except:
                        flt_tax3 = 0.0

                except:

                    flt_tax3 = 0.0
                tax3_code = str_line[73:75].strip()
                flt_total_tax = flt_total_tax + flt_tax3
                chr_total_fare_sign = str_line[75:76]
                str_currency_total_fare = str_line[76:79]

                try:
                    flt_total_fare_amt = float(str_line[79:87].strip())
                except ValueError:
                    try:
                        flt_total_fare_amt = float(str_line[79:86].strip())
                    except ValueError:
                        flt_total_fare_amt = 0
                except:
                    flt_total_fare_amt = 0

                if chr_total_fare_sign == '-':
                    flt_total_fare_amt = -1 *  flt_total_fare_amt



                try:
                    flt_cancellation_penalty_amt = float(str_line[87:98].strip())
                except :
                    flt_cancellation_penalty_amt = 0.0
                ins_capture_ticket_data.flt_cancellation_penalty_amt = flt_cancellation_penalty_amt

                try:
                    flt_equalent_fare_amount = float(str_line[119 :127].strip())
                except:
                    flt_equalent_fare_amount = 0.0


                str_equalent_curr_code = str_line[116:119].replace('*','').strip() or ins_general_methods.str_base_currency
                chr_equalent_paid_sign = str_line[115:116].strip()
                if chr_equalent_paid_sign == '-':
                    flt_equalent_fare_amount  = -1 * flt_equalent_fare_amount

                #42997
                if str_fare_currency_code.strip() != self.str_defult_currency_code and str_equalent_curr_code:  #44316
                    flt_base_fare = float(flt_equalent_fare_amount)

                str_fare_currency_code = str_equalent_curr_code
                ins_capture_ticket_data.str_equalent_curr_code = str_equalent_curr_code
                ins_capture_ticket_data.flt_equalent_fare_amount = flt_equalent_fare_amount

                try:
                    flt_commission_percentage = float(str_line[127:135].strip())
                except:
                    flt_commission_percentage = 0.0
                ins_capture_ticket_data.flt_commission_percentage = flt_commission_percentage
                try:
                    flt_commission_amt = float(str_line[136:144].strip())
                except:
                    flt_commission_amt = 0.0

                chr_commission_amt_sign = str_line[135:136].strip()
                if chr_commission_amt_sign == '-':
                    flt_commission_amt *= -1
                ins_capture_ticket_data.flt_commission_amt = flt_commission_amt
                try:
                    flt_net_amt = float(str_line[144:153].strip())
                except:
                    flt_net_amt = 0.0

                ins_capture_ticket_data.flt_net_amt = flt_net_amt

                try:
                    flt_travel_agency_tax = float(str_line[156:164].strip())
                except:
                    flt_travel_agency_tax = 0.0
                ins_capture_ticket_data.flt_travel_agency_tax = flt_travel_agency_tax
                ins_capture_ticket_data.str_inclusive_tour_number = str_line[164:179].strip().strip("*")
                ins_capture_ticket_data.str_fare_agent =  str_line[186:196].strip()
                ins_capture_ticket_data.str_fare_agent_code = str_line[191:193].strip()
                ins_capture_ticket_data.str_fare_agent_sine = str_line[193:196].strip()
                str_temp_code = str_line[191:193].strip()
                if str_temp_code not in ('*','') and ins_capture_ticket_data.str_booking_agent_code.strip() in ( '*',''):
                    ins_capture_ticket_data.str_booking_agent_code  = str_temp_code
                elif ins_capture_ticket_data.str_booking_agent_code.strip() in ( '*',''):
                    ins_capture_ticket_data.str_booking_agent_code = ''

                ins_capture_ticket_data.str_print_agent =  str_line[196:206].strip()
                ins_capture_ticket_data.str_print_agent_code = str_line[201:203].strip()
                ins_capture_ticket_data.str_print_agent_sine = str_line[203:206].strip()
                str_airline_code = str_line[231:233]
                str_ticket_number =str_line[233:243].strip()
                str_no_of_conj = str_line[243:244].strip() or '0'

                str_no_of_segments = str_line[227:229].strip()

                if str_no_of_conj == '0':
                    str_last_conj = ""
                else:
                    str_last_conj = str( int(str_ticket_number) + int(str_no_of_conj))
                lst_tmp_tax = []
                lst_tax = []
                str_tax = ''
                #Refer #38871   #refer #39886 
                if float(flt_tax1) > 0 and tax1_id not in ('PD', 'P'):
                    lst_tax.append(tax1_code + '=' + str(flt_tax1))
                    lst_tmp_tax.append((str(flt_tax1),tax1_code,tax1_id))
                if float(flt_tax2) >0 and tax2_id not in ('PD', 'P'):
                    lst_tax.append(tax2_code + '=' + str(flt_tax2))
                    lst_tmp_tax.append((str(flt_tax2),tax2_code,tax2_id))
                if float(flt_tax3) > 0 and tax3_id not in ('PD', 'P'):
                    lst_tax.append(tax3_code + '=' + str(flt_tax3))
                    lst_tmp_tax.append((str(flt_tax3),tax3_code,tax3_id))
                str_tax = ','.join(lst_tax)
                str_pax_name = ins_capture_ticket_data.dct_pax_name[int(int_interface_no)]

                tmp_lst = str_line.split('\r')


                str_m4_sqeuence_number = tmp_lst[1].strip()
                str_original_issue = tmp_lst[4].strip() #page 27
                str_original_issue = str_original_issue[3:13].strip()
                ## refer #13347
                str_issued_in_exchange = tmp_lst[5]

                str_issued_in_exchange = str_issued_in_exchange[3:13].strip()
                #Card approval code setting only for a particular ticket.(Refer #10384)

                str_card_approval_code = str_line[207:216].strip()

                ins_capture_ticket_data.dct_ticket_details[str_ticket_number] = [str_ticket_number,
                                       str_last_conj,
                                       str_airline_code,
                                       str_pax_name,
                                       str_pax_type,
                                       flt_base_fare,
                                       flt_total_fare_amt,
                                       flt_total_tax,
                                       lst_tmp_tax,
                                       str_tax,
                                       str_fare_currency_code,
                                       str_int_dom,
                                       str_currency_total_fare,
                                       str_issued_in_exchange,
                                       str_card_approval_code,
                                       flt_net_amt,
                                       flt_travel_agency_tax,
                                       flt_commission_amt,
                                       flt_commission_percentage,
                                       str_no_of_segments,
                                       str_m4_sqeuence_number
                                       ]

                ins_capture_ticket_data.lst_ticket_detls.append([str_ticket_number ,
                                                    str_last_conj ,
                                                    'ET' ,
                                                    str_airline_code ,
                                                    '' ,#airline numeric code
                                                    str_pax_name ,
                                                    str_pax_type ,
                                                    0.0 ,#service fee
                                                    '' ,#amadeus i section
                                                    '' ,#amadeus i section
                                                    0 #due to no pax item number
                                                    ])
                if tmp_lst[3][:4] == 'CASH' :
                    ins_capture_ticket_data.str_customer_code = 'CASH'

                elif tmp_lst[3][:3] == 'INV':
                    ins_capture_ticket_data.str_customer_code = tmp_lst[3].split('INV')[1].strip().upper()

            elif str_line[:2] == 'M3' and str_line[4:5] == '1':
                ins_capture_ticket_data.int_sectors_total_count+=1
                if not ins_capture_ticket_data.bln_sector_start:
                    ins_capture_ticket_data.lst_temp_sector_details = []
                
                if str_line[232:233].strip() != '1' and ( (not ins_capture_ticket_data.bln_emd or ins_capture_ticket_data.bln_emd_and_ticket ) and not ins_capture_ticket_data.bln_advance_deposit_file):
                    if ins_capture_ticket_data.int_number_of_segments > 0:
                        ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments -1
                    else:
                        ins_capture_ticket_data.int_number_of_segments = 0
                    return ins_capture_ticket_data

                str_dep_day = str_line[9:11].strip() #eg. 21MAR

                if not str_dep_day:
                    if ins_capture_ticket_data.int_number_of_segments > 0:
                        ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments -1
                    else:
                        ins_capture_ticket_data.int_number_of_segments = 0
                    return ins_capture_ticket_data # if M3 section contain no date
                
                ins_capture_ticket_data.str_dep_month =  str_dep_month = str_line[11:14].strip()
                if not ins_capture_ticket_data.str_first_dep_month:
                    ins_capture_ticket_data.str_first_dep_month = ins_capture_ticket_data.str_dep_month

                str_itinerary_item_number = str_line[2:4]
                str_dep_city = str_line[18:21].strip()
                str_dep_city_name = str_line[21:38].strip()
                str_arrival_city = str_line[38:41].strip()
                str_arrival_city_name = str_line[41:58]
                str_carrier_code = str_line[58:60].strip()
                str_flight_no = str_line[60:65].strip()
                str_service_class = str_line[65:67].strip() #40169
                if ins_capture_ticket_data.str_class_of_booking:
                    ins_capture_ticket_data.str_class_of_booking += '/'
                ins_capture_ticket_data.str_class_of_booking += str_service_class 
                diparture_time = str_line[67:72].strip()
                arrival_time = str_line[72:77].strip()
                str_no_of_segments = str_line[227:229].strip()
                if not ins_capture_ticket_data.str_first_departure_date and str_dep_day:
                    ins_capture_ticket_data.str_first_departure_date = str_dep_day
                if not ins_capture_ticket_data.str_departure_time and diparture_time:
                    ins_capture_ticket_data.str_departure_time = diparture_time

                if not ins_capture_ticket_data.str_arrival_time and arrival_time:
                    ins_capture_ticket_data.str_arrival_time = arrival_time

                str_flight_year = str_line[235:239].strip()

                if not str_flight_year :
                    str_flight_year = datetime.datetime.now().strftime("%d/%m/%Y").split('/')[2]

                ins_capture_ticket_data.str_flight_year = ins_capture_ticket_data.str_dep_year = str_flight_year

                #37216
                if not ins_capture_ticket_data.str_first_travel_year :
                    ins_capture_ticket_data.str_first_travel_year = str_flight_year

                if not str_line[90:91].strip():
                    str_arrival_date_count = 0
                else:
                    str_arrival_date_count = int(str_line[90:91])
                int_no_of_stops = 0
                str_no_of_stops = str_line[91:92].strip()

                if str_no_of_stops:
                    int_no_of_stops = int(str_no_of_stops)

                str_stop_city = str_line[92:110].strip()
                if int_no_of_stops > 1:
                    i = 0
                    j = len(str_stop_city) - 3
                    str_doc = ""
                    while i< j:
                        str_doc = str_stop_city[i:i+3] + "/"
                        i = i +3
                        str_stop_city = str_doc + str_stop_city[i:]


                str_dep_date = None
                str_arr_date = None
                if str_line[232:233].strip() != '1':
                    return ins_capture_ticket_data
                if ins_capture_ticket_data.lst_sector_details and not ins_capture_ticket_data.lst_sector_details[ins_capture_ticket_data.int_number_of_segments-1][0] == str_dep_city:
    #                bln_open_segment = True
                    ins_capture_ticket_data.lst_sector.append('')
                    ins_capture_ticket_data.lst_sector.append(str_dep_city)

                ins_capture_ticket_data.lst_sector.append(str_arrival_city)
                if not ins_capture_ticket_data.int_number_of_segments and not ins_capture_ticket_data.str_start_port_code:
                    ins_capture_ticket_data.str_start_port_code = str_dep_city 
                    
                ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments + 1
                if str_dep_day and str_dep_month:
                    str_dep_date = str_dep_day +"/"+ str_dep_month + "/" +ins_capture_ticket_data.str_flight_year
                    dat_departure = datetime.datetime.strptime(str_dep_date, "%d/%b/%Y")
                    str_dep_date = dat_departure.strftime("%d/%m/%Y")
                    if str_arrival_date_count == 0:
                        dat_arrival = dat_departure
                        str_arr_date = dat_arrival.strftime("%d/%m/%Y")
                    elif str_arrival_date_count == 1:
    #                    dat_arrival = dat_departure  + mx.DateTime.RelativeDateTime(days=+1)
                        dat_arrival = dat_departure  + datetime.timedelta(days=1)
                        str_arr_date = dat_arrival.strftime("%d/%m/%Y")
                    elif str_arrival_date_count == 2:
                        dat_arrival = dat_departure  + datetime.timedelta(days=+2)
                        str_arr_date = dat_arrival.strftime("%d/%m/%Y")
                    elif str_arrival_date_count == 3:
                        dat_arrival = dat_departure  + datetime.timedelta(days=+3)
                        str_arr_date = dat_arrival.strftime("%d/%m/%Y")
                    elif str_arrival_date_count == 4:
                        dat_arrival = dat_departure  + datetime.timedelta(days=-1)
                        str_arr_date = dat_arrival.strftime("%d/%m/%Y")
                    elif str_arrival_date_count == 5:
                        dat_arrival = dat_departure  + datetime.timedelta(days=-2)
                        str_arr_date = dat_arrival.strftime("%d/%m/%Y")

                if str_no_of_stops == '0':
                    bln_stop_over = False

                else:
                    bln_stop_over = True
                bln_open_segment = False


                if ins_capture_ticket_data.lst_temp_sector_details :
                    if ins_capture_ticket_data.lst_temp_sector_details[-1][4] != str_dep_city :
                        ins_capture_ticket_data.lst_temp_sector_details.append( [ins_capture_ticket_data.lst_temp_sector_details[-1][0],
                                    None,
                                    None,
                                    ins_capture_ticket_data.lst_temp_sector_details[-1][4],
                                    str_dep_city,
                                    ins_capture_ticket_data.lst_temp_sector_details[-1][6],
                                    str_dep_city_name,
                                    False,
                                    ins_capture_ticket_data.lst_temp_sector_details[-1][8],
                                    ins_capture_ticket_data.lst_temp_sector_details[-1][9],
                                    0.00,
                                    '' ,
                                    '' ,
                                    '',
                                    str_stop_city,
                                    'E',
                                    ''
                                    ] )
                        #common format of lst_sectors used in all GDS
                        ins_capture_ticket_data.lst_sector_details.append([ins_capture_ticket_data.lst_sector_details[-1][0],
                                    str_dep_city,
                                    ins_capture_ticket_data.lst_sector_details[-1][2],
                                    '',
                                    ins_capture_ticket_data.lst_sector_details[-1][4],
                                    ins_capture_ticket_data.lst_sector_details[-1][5],
                                    ins_capture_ticket_data.lst_sector_details[-1][6],
                                    None,
                                    None,
                                    False,
                                    0,#int_mileage
                                    0,#dbl_sector_wise_fare
                                    '',
                                    '',
                                    bln_open_segment
                                    ])

                ins_capture_ticket_data.lst_temp_sector_details.append( [str_carrier_code,
                                    str_arr_date,
                                    str_dep_date,
                                    str_dep_city,
                                    str_arrival_city,
                                    str_dep_city_name,
                                    str_arrival_city_name,
                                    bln_stop_over,
                                    str_service_class,
                                    str_flight_no,
                                    0.00,
                                    ins_capture_ticket_data.str_departure_time ,
                                    ins_capture_ticket_data.str_arrival_time,
                                    ins_capture_ticket_data.int_sectors_total_count,
                                    str_stop_city,
                                    '',
                                    str_itinerary_item_number] )

                #common format of lst_sectors used in all GDS
                ins_capture_ticket_data.lst_sector_details.append([str_dep_city,
                                    str_arrival_city,
                                    str_carrier_code,
                                    '',
                                    str_flight_no,
                                    str_service_class,
                                    str_service_class,
                                    str_arr_date,
                                    str_dep_date,
                                    bln_stop_over,
                                    0,#int_mileage
                                    0,#dbl_sector_wise_fare
                                    ins_capture_ticket_data.str_arrival_time,
                                    ins_capture_ticket_data.str_departure_time,
                                    bln_open_segment
                                    ])

                # // 9211 To consider the case of open Jow to save sector
                if ins_capture_ticket_data.str_previous_dest_code and not ins_capture_ticket_data.str_previous_dest_code == str_dep_city:
                    ins_capture_ticket_data.str_sector = ins_capture_ticket_data.str_sector +  "//" + str_dep_city
                    pass
                if ins_capture_ticket_data.bln_sector_start:
                    if str_stop_city:
                        ins_capture_ticket_data.str_stop_over_cities = ins_capture_ticket_data.str_stop_over_cities + "," + str_stop_city
    #                    ins_capture_ticket_data.str_sector = ins_capture_ticket_data.str_sector +  "/" + str_stop_city # refer 28169
                    ins_capture_ticket_data.str_sector = ins_capture_ticket_data.str_sector +  "/" + str_arrival_city
                    ins_capture_ticket_data.str_previous_dest_code = str_arrival_city
                else:
                    if str_stop_city:
                        ins_capture_ticket_data.str_stop_over_cities = str_stop_city
    #                    ins_capture_ticket_data.str_sector =  str_dep_city +"/"+str_arrival_city  # refer 28169
    #                    ins_capture_ticket_data.str_previous_dest_code = str_arrival_city
    #                else:
                    ins_capture_ticket_data.str_sector =  str_dep_city +"/" +str_arrival_city
                    ins_capture_ticket_data.str_previous_dest_code = str_arrival_city

                    ins_capture_ticket_data.bln_sector_start = True


                """# Refer #43565
                The below are the possible product codes (As per specification file)
                1 -> Air Ticket
                2 -> Manual Tour
                3 -> Hotel
                4 -> Sea
                5 -> Bus
                6 -> Rail
                7 -> Insurance
                8 -> Air Taxi and other
                9 -> Miscellaneous Charge Order
                A -> Car
                B -> Prepaid Ticket Advice
                C -> Land
                D -> Cruise Director
                E -> SNCB Rail, Elgar Rail, Eurostar Rail segments
                F -> Elva Sea Segment
                G -> Elva Tour Segment Auto
                H -> CruiseMatch
                I -> LeisureNet
                J -> Tour Guide Auto
                K -> Swedish Ground Transportation
                L -> Add Segment
                M -> Limo
                """
            elif str_line[:2] == 'M3' and str_line[4:5] != '1':
                str_product_code = str_line[4:5]
                if str_product_code == '3' and str_line[14:17] == 'HHT' and 1==0 : #To capture Hotel segment from HHT, remove "and 1==0" from IF condition.
                    ins_voucher_base = instanceBase.VoucherBase()            #as per Quality Aviation customer they are using this as passive segment
                    ins_voucher_base.str_hotel_check_in_date = str_line[9:14]

                    try :
                        ins_voucher_base.int_no_of_rooms = int(str_line[18:20])
                    except :
                        ins_voucher_base.int_no_of_rooms = 1

                    ins_voucher_base.str_hotel_confirm_number = str_line[20:35].replace('-','').strip()
                    lst_hotel_split_line = str_line[35:].split('\r')[0].split('/')
                    ins_voucher_base.str_city_code = lst_hotel_split_line[0].strip()
                    try:
                        ins_voucher_base.str_hotel_check_out_date = lst_hotel_split_line[1].replace('OUT','').strip()
                    except:
                        ins_voucher_base.str_hotel_check_out_date = ins_voucher_base.str_hotel_check_in_date

                    str_hotel_chain_code = lst_hotel_split_line[2][:lst_hotel_split_line[2].index(' ')].strip()
                    ins_voucher_base.str_hotel_name = lst_hotel_split_line[2][lst_hotel_split_line[2].index(' '):].strip()
                    ins_voucher_base.str_room_type = lst_hotel_split_line[3].strip()

                    ins_voucher_base.str_voucher_currency_code = lst_hotel_split_line[4][-3:]
                    try :
                        ins_voucher_base.flt_fare_inv = float(lst_hotel_split_line[4].replace(ins_voucher_base.str_voucher_currency_code,'').strip())
                    except :
                        ins_voucher_base.flt_fare_inv = 0.0


                    for str_hotel_data in lst_hotel_split_line:
                        if str_hotel_data[0]== 'G' and len(str_hotel_data[3:19].strip())>= 15  \
                                and str_hotel_data[3:19].strip().isdigit():
                            ins_voucher_base.str_credit_card_num = str_hotel_data[3:19].strip()
                            ins_voucher_base.str_cc_type = str_hotel_data[1:3].strip()

                        elif str_hotel_data[:3] == 'SI-':
                            lst_temp_data = str_hotel_data[3:].split('@')
                            if lst_temp_data and lst_temp_data[0][:3].strip()== 'CF-' and not ins_voucher_base.str_hotel_confirm_number:
                                ins_voucher_base.str_hotel_confirm_number = lst_temp_data[0][3:].replace('-','').strip()
                            if lst_temp_data[1].find('#FONE') != -1:
                                ins_voucher_base.str_hotel_address = lst_temp_data[1][:lst_temp_data[1].index('#FONE')].replace('#',',').strip()
                                if lst_temp_data[1].find('#FAX') != -1:
                                    ins_voucher_base.str_hotel_phone = lst_temp_data[1][lst_temp_data[1].index('#FONE')+5:lst_temp_data[1].index('#FAX')].strip()
                                else:
                                    ins_voucher_base.str_hotel_phone = lst_temp_data[1][lst_temp_data[1].index('#FONE')+5:].strip()
                            if lst_temp_data[1].find('#FAX') != -1:
                                ins_voucher_base.str_hotel_fax = lst_temp_data[1][lst_temp_data[1].index('#FAX')+4:].strip()

                    ins_voucher_base.str_voucher_number = ins_voucher_base.str_hotel_confirm_number
                    ins_voucher_base.str_voucher_type = 'H'
                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)            

                if str_product_code == '3' and str_line[14:17] == 'HHL' : #Hotel segment
                    ins_voucher_base = instanceBase.VoucherBase()
                    ins_voucher_base.str_hotel_check_in_date = str_line[9:14]
                    try:
                        ins_voucher_base.int_no_of_guest_inv = int(str_line[18:20])
                    except:
                        ins_voucher_base.int_no_of_guest_inv = 1

                    ins_voucher_base.str_hotel_confirm_number = str_line[20:34].replace('-','').strip()
                    ins_voucher_base.str_city_code = str_line[35:38]
                    str_hotel_chain_code = str_line[38:40]
                    str_date_and_night = str_line[41:54]
                    try:
                        str_out_date = str_date_and_night.split(' ')[0]
                        ins_voucher_base.str_hotel_check_out_date = str_out_date.replace('OUT','').strip()
                    except:
                        ins_voucher_base.str_hotel_check_out_date = ins_voucher_base.str_hotel_check_in_date

                    try :
                        ins_voucher_base.int_no_of_nights = int(str_date_and_night.split(' ')[-1][:-2].strip())
                    except :
                        ins_voucher_base.int_no_of_nights = 1

                    ins_voucher_base.str_property_code = str_line[54:60].strip()
                    ins_voucher_base.str_hotel_name = str_line[60:92].strip()

                    try :
                        ins_voucher_base.int_no_of_rooms = int(str_line[92:93])
                    except :
                        ins_voucher_base.int_no_of_rooms = 1

                    lst_hotel_split_line = str_line.split('\r')[0].split('/')
                    ins_voucher_base.str_voucher_currency_code = lst_hotel_split_line[0][-3:]
                    try :
                        ins_voucher_base.flt_fare_inv = float(lst_hotel_split_line[0][103:].replace(ins_voucher_base.str_voucher_currency_code,'').strip())
                    except :
                        ins_voucher_base.flt_fare_inv = 0.0

                    #for amount in base currency
    #                ins_voucher_base.str_voucher_currency_code = ins_general_methods.str_base_currency
    #                try :
    #                    ins_voucher_base.flt_fare_inv = float(lst_hotel_split_line[1].replace(ins_voucher_base.str_voucher_currency_code,'').strip())
    #                except :
    #                    ins_voucher_base.flt_fare_inv = 0.0

                    ins_voucher_base.flt_fare_inv = ins_voucher_base.flt_fare_inv * ins_voucher_base.int_no_of_rooms

                    ins_voucher_base.str_room_type =  str_line[93:101].strip() #As per customer
                    #####
                    #As per Das sir the meals plan will be hard coded as below.
                    ins_voucher_base.str_meals_plan = 'BB' #BB- Bed and breakfast
                    #####
                    for str_hotel_data in lst_hotel_split_line:
                        if str_hotel_data[:4] == 'TTX-':
                            try:
                                ins_voucher_base.flt_total_tax_inv = float(str_hotel_data[4:].replace(ins_voucher_base.str_voucher_currency_code,'').strip())
                            except:
                                ins_voucher_base.flt_total_tax_inv = 0.0

                        elif str_hotel_data[:4] == 'TSC-': #As per customer requirement,Total Surcharge/Fee is added to total tax 
                            try:
                                ins_voucher_base.flt_total_tax_inv += float(str_hotel_data[4:].replace(ins_voucher_base.str_voucher_currency_code,'').strip())
                            except:
                                pass

                        elif str_hotel_data[:3] == 'SI-':
                            lst_temp_data = str_hotel_data[3:].split('@')
                            if lst_temp_data and lst_temp_data[0][:3].strip()== 'CF-' and not ins_voucher_base.str_hotel_confirm_number:
                                ins_voucher_base.str_hotel_confirm_number = lst_temp_data[0][3:].replace('-','').strip()
                            if lst_temp_data[1].find('#FONE') != -1:
                                ins_voucher_base.str_hotel_address = lst_temp_data[1][:lst_temp_data[1].index('#FONE')].replace('#',',').strip()
                                if lst_temp_data[1].find('#FAX') != -1:
                                    ins_voucher_base.str_hotel_phone = lst_temp_data[1][lst_temp_data[1].index('#FONE')+5:lst_temp_data[1].index('#FAX')].strip()
                                else:
                                    ins_voucher_base.str_hotel_phone = lst_temp_data[1][lst_temp_data[1].index('#FONE')+5:].strip()
                            if lst_temp_data[1].find('#FAX') != -1:
                                ins_voucher_base.str_hotel_fax = lst_temp_data[1][lst_temp_data[1].index('#FAX')+4:].strip()

                        elif str_hotel_data[:3] == '7P-' and not ins_voucher_base.str_property_code:
                            ins_voucher_base.str_property_code = str_hotel_data[3:].strip()

                        elif str_hotel_data[0]== 'G' and len(str_hotel_data[3:19].strip())>= 15 \
                                and str_hotel_data[3:19].strip().isdigit():
                            ins_voucher_base.str_credit_card_num = str_hotel_data[3:19].strip()
                            ins_voucher_base.str_cc_type = str_hotel_data[1:3].strip()
                    ins_voucher_base.str_voucher_number = ins_voucher_base.str_hotel_confirm_number
                    ins_voucher_base.str_supplier_confirm_number = ins_voucher_base.str_hotel_confirm_number
                    ins_voucher_base.str_voucher_type = 'H'
                    if ins_voucher_base.str_voucher_number and ins_voucher_base.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base.str_voucher_number)
                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base)



                if str_product_code == 'A' and str_line[14:17] == 'CAR' : #Car segment
                    ins_voucher_base1 = instanceBase.VoucherBase()
                    ins_voucher_base1.str_voucher_type = 'C'
                    ins_voucher_base1.str_voucher_category = 'CAR'
                    ins_voucher_base1.str_voucher_service_name = 'CAR'

                    ins_voucher_base1.str_pick_up_date = str_line[9:14]
                    try :
                        ins_voucher_base1.int_no_of_car = int(str_line[17:18])
                    except :
                        ins_voucher_base1.int_no_of_car = 1

                    ins_voucher_base1.str_vendor_code = str_line[18:20].strip()
                    ins_voucher_base1.str_supplier_confirm_number = str_line[20:35].replace('-','').strip()

                    lst_car_split_line = str_line[35:].split('\r')[0].split('/')
                    ins_voucher_base1.str_city_code = lst_car_split_line[0].strip()
                    ins_voucher_base1.str_pick_up_location = ins_voucher_base1.str_city_code
                    ins_voucher_base1.bln_with_pickup = True
                    ins_voucher_base1.str_drop_off_date = lst_car_split_line[1].strip()
                    ins_voucher_base1.str_booking_details = "CAR CODE:%s ,CAR TYPE:%s"%(ins_voucher_base1.str_vendor_code,lst_car_split_line[2].strip())#car details

                    for str_car_data in lst_car_split_line:
                        if str_car_data[:4] == 'ARR-':
                            ins_voucher_base1.str_drop_off_time = str_car_data[4:].strip()

                        elif str_car_data[:4] == 'RET-':
                            ins_voucher_base1.str_pick_up_time = str_car_data[4:].strip()

                        elif str_car_data[:3] == 'PH-':
                            ins_voucher_base1.str_driver_ph_no = str_car_data[3:].strip()

                        elif str_car_data[:3] == 'DO-':
                            ins_voucher_base1.str_drop_off_location = str_car_data[3:].strip()
                            if ins_voucher_base1.str_drop_off_location :
                                ins_voucher_base1.bln_with_dropup = True #tbl_car_voucher_sales

                        elif str_car_data[0]== 'G' and len(str_car_data[3:19].strip())>= 15 \
                                and str_car_data[3:19].strip().isdigit():
                            ins_voucher_base1.str_credit_card_num = str_car_data[3:19].strip()
                            ins_voucher_base1.str_cc_type = str_car_data[1:3].strip()

                        elif str_car_data[:3] == 'RC-': #rate type
                            ins_voucher_base1.str_rate_type = str_car_data[3:].strip()

                        elif str_car_data[:3] == 'RG-': #rate guaranteed
                            ins_voucher_base1.str_voucher_currency_code = str_car_data[3:6].strip()
                            try :
                                ins_voucher_base1.flt_fare_inv = float(str_car_data[3:].split()[0].replace(ins_voucher_base1.str_voucher_currency_code,'').strip())
                            except :
                                ins_voucher_base1.flt_fare_inv = 0.0

                        elif str_car_data[:3] == 'CF-' and not ins_voucher_base1.str_supplier_confirm_number:
                                ins_voucher_base1.str_supplier_confirm_number = str_car_data[3:].replace('-','').strip()


                    ins_voucher_base1.str_voucher_number = ins_voucher_base1.str_supplier_confirm_number
                    if ins_voucher_base1.str_voucher_number and ins_voucher_base1.str_voucher_number not in ins_capture_ticket_data.lst_voucher_numbers :
                        ins_capture_ticket_data.lst_voucher_numbers.append(ins_voucher_base1.str_voucher_number)
                        ins_capture_ticket_data.lst_ins_voucher.append(ins_voucher_base1)
                
            elif str_line[:2] == "M4":
                try:
                    ins_capture_ticket_data.int_m4_sequence += 1
                    ins_capture_ticket_data.dct_m4_sequence[ins_capture_ticket_data.int_m4_sequence] = int(str_line[2:4])
                    ins_capture_ticket_data.dct_stop_over[str_line[2:4]] = str_line[7]  #44255
                
                    int_order = int(str_line[2:4])-1 
                    ins_capture_ticket_data.lst_order_details.append(int_order)
                except:
                    pass

                if not ins_capture_ticket_data.bln_got_fare_basis:
                    ins_capture_ticket_data.str_fare_basis = str_line[24:37].strip()
                    ## ins_capture_ticket_data.str_dep_month = str_dep_month
    ## 				ins_capture_ticket_data.str_dep_year = str_dep_year

                    ins_capture_ticket_data.lst_agent_details.append([ins_capture_ticket_data.str_fare_basis,
                                          ins_capture_ticket_data.str_sector,
                                          ins_capture_ticket_data.str_stop_over_cities,
                                          ins_capture_ticket_data.int_number_of_segments,
                                          ins_capture_ticket_data.str_dep_month,
                                          ins_capture_ticket_data.str_dep_year
                                          ])

                    ins_capture_ticket_data.bln_got_fare_basis = True

                else:
                    pass

            elif str_line[:2] == "M5":
                try:
                    if not str_line[4:6].isdigit():
                        return ins_capture_ticket_data
                    lst_split_line = str_line.split('\r')[0].split('/')  #37944
                    # Code changed to solve the issue for sabre files having one M2 section ie having a Ticket number T1 
                    #but has two M5 section having tickets T1 & T2 and the form of payment is just taking from M5 section 
                    #without checking the ticket corresponding to the ticket in M2 section.So now Ticket number in M5 and M2 
                    #section will be compared and form of payment will be set only if the ticket number matches.(Refer #10351)

                    str_m5_ticket_number = lst_split_line[0][-10:].strip()
                    if str_m5_ticket_number in ins_capture_ticket_data.dct_ticket_details:  #41912 - #In the case of reissue, net amount is taken from M5 if it is not obtained from M2.
                        lst_ticket_detls = ins_capture_ticket_data.dct_ticket_details[str_m5_ticket_number]
                        if lst_ticket_detls[0] == str_m5_ticket_number and lst_ticket_detls[13] \
                                and not lst_ticket_detls[15]:
                            try:
                                if lst_split_line[2][0].strip() and not lst_split_line[2][0].strip().isdigit():
                                    lst_ticket_detls[15] = float(lst_split_line[2][3:].replace(self.str_defult_currency_code,'').strip() or 0)
                                else :
                                    lst_ticket_detls[15] = float(lst_split_line[2].strip().replace(self.str_defult_currency_code,'').strip() or 0)
                            except:
                                lst_ticket_detls[15] = 0.0
                         
                    str_card_data=''
                    for str_item in lst_split_line[4:] :
                        if str_item[:2]== 'CC' or str_item[:2]== 'CX': 
                            str_card_data = str_item
                            break

                    if str_card_data[:2]== 'CC' or str_card_data[:2]== 'CX':
                        ins_capture_ticket_data.str_cc_card_no = str_card_data[4:19]
                        ins_capture_ticket_data.str_cc_type = str_card_data[2:4]
                        # refer 30892 Miss match between TRAACS and BSP

                        try:
                            if lst_split_line[2][0].strip() and not lst_split_line[2][0].strip().isdigit():
                                        flt_base_fare = float(lst_split_line[2][3:].replace(self.str_defult_currency_code,'').strip() or 0)
                            else :
                                flt_base_fare = float(lst_split_line[2].strip().replace(self.str_defult_currency_code,'').strip() or 0)
                        except:
                            flt_base_fare = 0.0
                        try:
                            flt_total_tax = float(lst_split_line[3].replace(self.str_defult_currency_code,'').strip() or 0)
                            if str_m5_ticket_number in ins_capture_ticket_data.dct_ticket_details: #38502
                                if ins_general_methods.ins_auto_inv.str_input_vat_code \
                                        and ins_capture_ticket_data.dct_ticket_details[str_m5_ticket_number][9].find(ins_general_methods.ins_auto_inv.str_input_vat_code) != -1:
                                    flt_total_tax += float(lst_split_line[4].replace(self.str_defult_currency_code,'').replace('D','').strip() or 0)
                                elif lst_split_line[4].replace(self.str_defult_currency_code,'').replace('D','').strip() :
                                    ins_capture_ticket_data.dct_m5_card_vat[str_m5_ticket_number] = float(lst_split_line[4].replace(self.str_defult_currency_code,'').replace('D','').strip() or 0)
                        except:
                            flt_total_tax = 0.0
#                        try:
#                            flt_total_tax = float(lst_split_line[3].replace(self.str_defult_currency_code,'').strip() or 0)
#                            for lst_ticket_details1 in ins_capture_ticket_data.lst_ticket_details: #38502
#                                if lst_ticket_details1[0] == str_m5_ticket_number and ins_general_methods.ins_auto_inv.str_input_vat_code :
#                                    if lst_ticket_details1[9].find(ins_general_methods.ins_auto_inv.str_input_vat_code) != -1:
#                                        flt_total_tax += float(lst_split_line[4].replace(self.str_defult_currency_code,'').replace('D','').strip() or 0)
#                                    elif lst_split_line[4].replace(self.str_defult_currency_code,'').replace('D','').strip() :
#                                        ins_capture_ticket_data.dct_m5_card_vat[str_m5_ticket_number] = float(lst_split_line[4].replace(self.str_defult_currency_code,'').replace('D','').strip() or 0)
#                        except:
#                            flt_total_tax = 0.0

                        ins_capture_ticket_data.dct_cc_card[str_m5_ticket_number] = [ins_capture_ticket_data.str_cc_card_no,ins_capture_ticket_data.str_cc_type,flt_base_fare,flt_total_tax]

                    #pnr pax number
                    try:
                        if not str_m5_ticket_number in ins_capture_ticket_data.dct_m5_pax_no:
                            str_pax_association = ''
                            str_pax_association_string = lst_split_line[5].split(' ')[1].strip()
                            for chr in str_pax_association_string:
                                if chr and (chr.isdigit() or chr == '.') :
                                    str_pax_association += chr
                                else:
                                    break
                            ins_capture_ticket_data.dct_m5_pax_no[str_m5_ticket_number] = str_pax_association
                    except:
                       pass


                    bln_m5_refund = False

                    if ins_capture_ticket_data.bln_advance_deposit_file and str_line[7] != 'R': ### Refer # 18249
                        lst_split_line = str_line.split('/')

                        if str_line[10] == '#' :
                            str_ticket_number = lst_split_line[0][-10:].strip()

                            str_accounting_item_number = str_line[2:4]
                            str_airline_code = str_line[8:10]
                            str_last_conj = ''
                            str_pax_name = ins_capture_ticket_data.dct_pax_name.get(int(str_accounting_item_number),'')
                            if not str_pax_name :
                                str_pax_name = ''.join(lst_split_line[5].split('.')[1:])[1:].strip()
                            str_pax_type = ''
                            if lst_split_line[2][0].strip() and not lst_split_line[2][0].strip().isdigit():
                                flt_base_fare = float(lst_split_line[2][3:].replace(self.str_defult_currency_code,'').strip() or 0)
                            else :
                                flt_base_fare = float(lst_split_line[2].strip().replace(self.str_defult_currency_code,'').strip() or 0)

                            flt_total_fare_amt = flt_base_fare
                            flt_total_tax = float(lst_split_line[3].replace(self.str_defult_currency_code,'').strip() or 0)
                            lst_tmp_tax = []
                            str_tax = ''
                            str_fare_currency_code = self.str_defult_currency_code
                            str_int_dom = ''
                            str_currency_total_fare = ''
                            str_issued_in_exchange = ''
                        else :
                            str_ticket_number = lst_split_line[1][-10:].strip()
                            str_accounting_item_number = str_line[2:4]
                            str_airline_code = str_line[8:10]
                            str_last_conj = ''
                            str_pax_name = ins_capture_ticket_data.dct_pax_name.get(int(str_accounting_item_number),'') 
                            if not str_pax_name :
                                str_pax_name = ''.join(lst_split_line[6].split('.')[1:])[1:].strip()
                            str_pax_type = ''
                            if lst_split_line[3][0].strip() and not lst_split_line[3][0].strip().isdigit():
                                flt_base_fare = float(lst_split_line[3][3:].replace(self.str_defult_currency_code,'').strip() or 0)
                            else :
                                flt_base_fare = float(lst_split_line[3].strip().replace(self.str_defult_currency_code,'').strip() or 0)

                            flt_total_fare_amt = flt_base_fare
                            flt_total_tax = float(lst_split_line[4].replace(self.str_defult_currency_code,'').strip() or 0)
                            lst_tmp_tax = []
                            str_tax = ''
                            str_fare_currency_code = self.str_defult_currency_code
                            str_int_dom = ''
                            str_currency_total_fare  = ''
                            str_issued_in_exchange = ''



                        if len(str_ticket_number.lstrip('0')) < 10 or not str_ticket_number.lstrip('0').isdigit():
                            return ins_capture_ticket_data

                        ins_capture_ticket_data.str_ticket_rm_remarks = ''
                        ins_capture_ticket_data.dct_ticket_details[str_ticket_number] = [str_ticket_number,
                                               str_last_conj,
                                               str_airline_code,
                                               str_pax_name,
                                               str_pax_type,
                                               flt_base_fare,
                                               flt_total_fare_amt,
                                               flt_total_tax,
                                               lst_tmp_tax,
                                               str_tax,
                                               str_fare_currency_code,
                                               str_int_dom,
                                               str_currency_total_fare,
                                               str_issued_in_exchange,
                                               '',
                                               0,
                                               0,
                                               0,
                                               0,
                                               '',
                                               '']  

                        ins_capture_ticket_data.lst_ticket_detls.append([str_ticket_number ,
                                                str_last_conj ,
                                                'EMD' ,
                                                str_airline_code ,
                                                '' ,#airline numeric code
                                                str_pax_name ,
                                                str_pax_type ,
                                                0.0 ,#service fee
                                                '' ,#amadeus i section
                                                '' ,#amadeus i section
                                                0 #due to no pax item number
                                                ])
                    elif str_line[7] == 'R' :

                        if str_line[10] != '#' :
                            str_line = str_line[:10]+'#'+str_line[11:]
                            lst_split_line = str_line.split('/')

                        str_ref_ticket_number = lst_split_line[0][-10:].strip()
                        str_ref_airline_code = lst_split_line[0][8:10].strip()
                        str_accounting_item_number = str_line[2:4]
                        bln_m5_refund = True

                        try :
                            flt_ref_fare = float(lst_split_line[2].strip() or 0.00)
                        except :
                            flt_ref_fare = 0.00

                        try :
                            flt_ref_tax = float(lst_split_line[3].strip() or 0.00)
                        except :
                            flt_ref_tax = 0.00

                        ins_capture_ticket_data.str_ref_price_indicator = lst_split_line[4].strip() or 'ONE'
                        str_ref_pax_name = lst_split_line[5].strip().lstrip('CA 1.1') or ''
                        str_ref_pax_name_rfd = ins_capture_ticket_data.dct_pax_name.get(str_accounting_item_number,str_ref_pax_name)
                        flt_refund_amount = flt_ref_fare + flt_ref_tax
                        flst_sale_amount = 0

                        flt_fare = 0
                        flt_tax = 0

                        rst = ins_general_methods.get_ticket_details(str_ref_ticket_number)

                        if rst not in [[],None,[None]]:
                            if not rst[0]['dat_refund'] :
                                flst_sale_amount += rst[0]['dbl_tran_currency_market_fare_inv'] + rst[0]['dbl_tran_currency_tax_inv']
                                flt_fare = rst[0]['dbl_tran_currency_market_fare_inv']
                                flt_tax = rst[0]['dbl_tran_currency_tax_inv']
                                str_tkt_no = str_ref_ticket_number
                                str_original_issue = rst[0]['vchr_original_issue']
                                lst_original_issue = []
                                while str_original_issue and str_tkt_no != str_original_issue :
                                    rst_orig = ins_general_methods.get_ticket_details(str_original_issue)
                                    str_tkt_no = ''
                                    str_original_issue = ''
                                    if rst_orig not in [[],None,[None]]:
                                        if not rst_orig[0]['dat_refund'] :
                                            str_tkt_no = rst_orig[0]['vchr_ticket_number']
                                            str_original_issue = rst_orig[0]['vchr_original_issue']
                                            if str_original_issue in lst_original_issue :
                                                break
                                            else :
                                                lst_original_issue.append(str_original_issue)

                                            flst_sale_amount += rst_orig[0]['dbl_tran_currency_market_fare_inv'] + rst_orig[0]['dbl_tran_currency_tax_inv']

                                            flt_fare += rst_orig[0]['dbl_tran_currency_market_fare_inv']
                                            flt_tax += rst_orig[0]['dbl_tran_currency_tax_inv']

                                ## Refund charges = (old_fare+tax) - (new_fare+tax)
                                flt_ref_charge = flst_sale_amount - flt_refund_amount
                                ins_capture_ticket_data.bln_refund = True
#                                ins_capture_ticket_data.dct_refund_tickets[str_ref_ticket_number] = [flt_fare,flt_tax,str_ref_price_indicator,str_ref_pax_name,flt_ref_charge]
                                ins_capture_ticket_data.dct_refund_data[str_ref_ticket_number] = [str_ref_ticket_number,
                                                '',
                                                str_ref_pax_name,
                                                flt_ref_charge,
                                                0.0,
                                                float(flt_fare or 0.0),
                                                float(flt_tax or 0.0),
                                                '', #taxString
                                                0.0,#vat amount
                                                '', #str_cc_type
                                                '', #str_cc_card_no
                                                ''] #str_credit_card_expiration_mm_yy_rfd
                                                
                                ins_capture_ticket_data.lst_ticket_detls.append([str_ref_ticket_number ,
                                                '' ,
                                                'ET' ,
                                                '' ,
                                                '' ,#airline numeric code
                                                str_ref_pax_name ,
                                                '' ,
                                                0.0 ,#service fee
                                                '' ,#amadeus i section
                                                '' ,#amadeus i section
                                                0 #due to no pax item number
                                                ])
                            else :
                                ins_capture_ticket_data.dct_refund_tickets[str_ref_ticket_number] = ['D']
                        else :#direct refund
                            ins_capture_ticket_data.dct_refund_tickets['REFUND'] = ['Z'] #to do 
#                            ins_capture_ticket_data.dct_refund_tickets[str_ref_ticket_number] = ['Z',flt_ref_fare,flt_ref_tax,str_ref_pax_name_rfd,str_ref_airline_code,0]
                    if not str_m5_ticket_number in ins_capture_ticket_data.lst_m5_ticket_type :
                        ins_capture_ticket_data.lst_m5_ticket_type.append(str_m5_ticket_number)
                    else:
                        # Delete extra M5 line data if there is a refund in same file
                        if bln_m5_refund and ins_capture_ticket_data.lst_ticket_details:
                            lst_temp_ticket_details = []
                            for lst_tkt_detls in ins_capture_ticket_data.lst_ticket_details :
                                if lst_tkt_detls[0] != str_m5_ticket_number :
                                    lst_temp_ticket_details.append(lst_tkt_detls)
                            ins_capture_ticket_data.lst_ticket_details = lst_temp_ticket_details
                except:
                    pass

                pass

            elif str_line[:2] == "M6":
                flt_roe = 0.0

                if "ROE" in str_line:
                    index = str_line.index("ROE")

                    max_index =  index + 15

                    str_roe = str_line[index : max_index].strip().split()[0]
                    try:
                        flt_roe = float(str_roe[3:])
                    except:
                        flt_roe = 0
                ins_capture_ticket_data.flt_roe = flt_roe

                #38243
                str_m6_xt_tax_split = ''
                flt_vat_xt = 0.0
                str_temp_line = str_line.split('\r')[0]
                if "XT" in str_temp_line:
                    str_xt_tax_split_line = str_temp_line[str_temp_line.index("XT")+2: ].strip()
                    int_start_index = 0
                    bln_flag = False
                    for int_count in range(len(str_xt_tax_split_line)):
                        if bln_flag :
                            bln_flag =False
                            continue
                        if str_xt_tax_split_line[int_count].isalpha():
                            flt_xt_tax = str_xt_tax_split_line[int_start_index:int_count]
                            str_xt_component = str_xt_tax_split_line[int_count:int_count+2]
                            str_m6_xt_tax_split += ',' +str_xt_component +'='+ flt_xt_tax 
                            int_start_index = int_count+2
                            bln_flag = True
                            #43241
                            if ins_general_methods.ins_auto_inv.str_input_vat_code and ins_general_methods.ins_auto_inv.str_input_vat_code == str_xt_component :
                                try:
                                    flt_vat_xt += float(flt_xt_tax)
                                except:
                                    pass
                if str_m6_xt_tax_split:#43241
                    for str_key,lst_ticket_details1 in ins_capture_ticket_data.dct_ticket_details.items():
                        if lst_ticket_details1[4] == str_temp_line[2:5] and flt_vat_xt \
                              and lst_ticket_details1[0] in ins_capture_ticket_data.dct_cc_card and lst_ticket_details1[0] in ins_capture_ticket_data.dct_m5_card_vat \
                              and ins_capture_ticket_data.dct_m5_card_vat[lst_ticket_details1[0]] == flt_vat_xt:    
                            ins_capture_ticket_data.dct_cc_card[lst_ticket_details1[0]][3] += flt_vat_xt


                if str_temp_line[2:5] == 'ADT'and not ins_capture_ticket_data.str_m6_xt_tax_split_adt :
                    ins_capture_ticket_data.str_m6_xt_tax_split_adt = str_m6_xt_tax_split

                elif (str_temp_line[2:5] == 'CHD' or 'CNN')and not ins_capture_ticket_data.str_m6_xt_tax_split_chd :
                    ins_capture_ticket_data.str_m6_xt_tax_split_chd = str_m6_xt_tax_split

                elif str_temp_line[2:5] == 'INF'and not ins_capture_ticket_data.str_m6_xt_tax_split_inf :
                    ins_capture_ticket_data.str_m6_xt_tax_split_inf = str_m6_xt_tax_split


            elif str_line[:2] == 'MX':
                lst_tax = []
                lst_tax_all = []
                try :
                    if str_line[12:22]:
                        str_mx_line = str_line[26:]
                        int_no_of_tax_components = int(str_line[23:26]) or 0
                        str_ticket_number_mx = str_line[12:22]
                        #36789
                        lst_tax_all = [str_tax.strip() for str_tax in str_mx_line.split('\r')][:int_no_of_tax_components]

                        for str_tax in lst_tax_all:  
                            if str_tax[0] == 'E'or str_tax[0] == 'P':
                                continue
                            lst_tax.append(str_tax)

                        str_mx_tax_details = ' ,'.join([str_tax[-2:].strip() + '=' + str(str_tax[:-2])     for str_tax in lst_tax])
                        lst_tax = [float(str_tax[:-2]) for str_tax in lst_tax]
                        if lst_tax and str_ticket_number_mx not in ins_capture_ticket_data.dct_mx_tax_detail:
                            ins_capture_ticket_data.dct_mx_tax_detail[str_ticket_number_mx] =  [sum(lst_tax),str_mx_tax_details]
                except :
                    pass


            elif str_line[:2] == 'MA':
                try :
                    if str_line[65:76].strip() and float(str_line[65:76].strip()):
                        flt_tax = float(str_line[65:76].strip())
                        str_tkt_no = str_line[10:20].strip()
                        if str_tkt_no in ins_capture_ticket_data.dct_ma_tax :
                            ins_capture_ticket_data.dct_ma_tax[str_tkt_no] += flt_tax
                        else :
                            ins_capture_ticket_data.dct_ma_tax[str_tkt_no] = flt_tax
#
                        if str_line[37:62].find('CREDIT CARD CHARGE') != -1:  #42585
                            ins_capture_ticket_data.bln_ma_tax_card_amt = True 
                            
                        if str_tkt_no in ins_capture_ticket_data.dct_ticket_details:
                            ins_capture_ticket_data.dct_ticket_details[str_tkt_no][7] += flt_tax
                            ins_capture_ticket_data.dct_ticket_details[str_tkt_no][8] = ins_capture_ticket_data.dct_ticket_details[str_tkt_no][8].append ((str(flt_tax),'MA','T4'))
                            ins_capture_ticket_data.dct_ticket_details[str_tkt_no][9] += ',MA='+str(flt_tax)
                except :
                    pass

            # // M9 Section containing remarks field
            try:
                for str_key in ins_general_methods.dct_capturing_settings:
                    code = ins_general_methods.dct_capturing_settings[str_key][2] #43507
                    if code and code[:3] in ('M8*','M9*') and str_line[:2] in ('M8','M9') and str_line[2:4].isdigit() and str_line[4:len(code)+1] == code[3:]:
                        str_format = str_line[:len(code)+1]


                        if str_key == "PARTY_CODE":
                            ins_capture_ticket_data.str_rm_customer_code = str_line.replace(str_format,'').split("\r")[0].strip()[:50].split('/')[0].strip().split(';')[0]
                        elif str_key == 'CUST_PAX_EMAIL':
                            ins_capture_ticket_data.str_rm_email = str_line.replace(str_format,'').split("\r")[0].strip()[:100].strip().split(';')[0]    
                            ins_capture_ticket_data.str_cust_pax_email = str_line.replace(str_format,'').split("\r")[0].strip()[:100].strip().split(';')[0]  

                        elif str_key == 'AGENCY_INTERNAL_REMARKS':
                            ins_capture_ticket_data.str_ticket_rm_remarks = str_line.replace(str_format,'').split("\r")[0].strip()[:200].strip().split(';')[0]    
                            ins_capture_ticket_data.str_agency_internal_remarks = str_line.replace(str_format,'').split("\r")[0].strip()[:200].strip().split(';')[0]   

                        elif str_key == 'AGENCY_TICKETING_STAFF':
                            try:   #  Refer #40193
                                ins_capture_ticket_data.str_agency_ticketing_staff = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]
                                if re.match('[^@]+@[^@]+\.[^@]+',ins_capture_ticket_data.str_agency_ticketing_staff.replace('\\\\','@')):  #45305
                                    ins_capture_ticket_data.str_ticketing_agent_code, ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_general_methods.get_staff_code_from_email(ins_capture_ticket_data.str_agency_ticketing_staff.replace('\\\\','@'), 'Sabre')
                                elif ins_capture_ticket_data.str_agency_ticketing_staff[:4].isdigit(): 
                                    ins_capture_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_agency_ticketing_staff[4:6]
                                    ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_agency_ticketing_staff[:4]
                                else:
                                    ins_capture_ticket_data.str_ticketing_agent_code = ins_capture_ticket_data.str_agency_ticketing_staff[:2]
                                    ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_agency_ticketing_staff[2:6]
                            except:
                                ins_capture_ticket_data.str_ticketing_agent_code = ''
                                ins_capture_ticket_data.str_ticketing_agent_numeric_code = ''

                        elif str_key == 'CUST_PAX_MOBILE':
                            ins_capture_ticket_data.str_phone = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]
                            ins_capture_ticket_data.str_cust_pax_mobile = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]

                        elif str_key == 'CUST_PURPOSE_OF_TRAVEL':
                            ins_capture_ticket_data.str_purpose = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]    
                            ins_capture_ticket_data.str_cust_purpose_of_travel = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]    

                        elif str_key == "AGENCY_COST CENTRE_CODE":
                            ins_capture_ticket_data.str_rm_cost_centre = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_auto_invoice_location = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]

                        elif str_key == "CUST_JOB_CODE":
                            ins_capture_ticket_data.str_rm_job_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]
                            ins_capture_ticket_data.str_cust_job_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]

                        elif str_key == 'PARTY_LPO_NO':
                            ins_capture_ticket_data.str_rm_lpo_number = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]    
                            ins_capture_ticket_data.str_party_lpo_no = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]    

                        elif str_key == "CUST_EMPLOYEE_NO":
                            ins_capture_ticket_data.str_rm_employee_number = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    
                            ins_capture_ticket_data.str_cust_employee_no = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0] 

                        elif str_key == "CUST_SUB_CUSTOMER_CODE":
                            ins_capture_ticket_data.str_rm_sub_customer = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]

                        elif str_key == "FARE_SERVICE_FEE":
                            flt_svf = 0.0
                            try:
                                ins_capture_ticket_data.bln_adt_svf = True
                                if str_line.find("\r") != -1:
                                    flt_svf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_svf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])

                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:  # refer 30961

                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_service_fee_sector_wise[starting_segment] =  flt_svf
                                else:
                                    ins_capture_ticket_data.flt_rm_service_charge = flt_svf

                            except:
                                ins_capture_ticket_data.flt_rm_service_charge = 0.0   


                        elif str_key == "FARE_SERVICE_FEE_INFANT":
                            flt_svf_infant = 0.0
                            try:
                                ins_capture_ticket_data.bln_inf_svf = True
                                if str_line.find("\r") != -1:
                                    flt_svf_infant = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_svf_infant = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 30961
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_infant_service_fee_sector_wise[starting_segment] = flt_svf_infant
                                else:
                                    ins_capture_ticket_data.flt_service_fee_infant = flt_svf_infant  

                            except:
                                ins_capture_ticket_data.flt_service_fee_infant = 0.0   



                        elif str_key == "FARE_SERVICE_FEE_CHILD":
                            flt_svf_child = 0.0
                            try:
                                ins_capture_ticket_data.bln_chd_svf = True
                                if str_line.find("\r") != -1:
                                    flt_svf_child= float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_svf_child = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3: # refer 30961

                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_child_service_fee_sector_wise[starting_segment] = flt_svf_child
                                else:
                                    ins_capture_ticket_data.flt_service_fee_child = flt_svf_child

                            except:
                                ins_capture_ticket_data.flt_service_fee_child = 0.0   


                        elif str_key == "FARE_CHD_SELLING_PRICE":
                            try:
                                ins_capture_ticket_data.bln_chd_selling_price = True
                                if str_line.find("\r") != -1:
                                    ins_capture_ticket_data.flt_selling_price_child = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                else:
                                    ins_capture_ticket_data.flt_selling_price_child = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_selling_price_child = 0.0   

                        elif str_key == "FARE_INF_SELLING_PRICE":
                            try:
                                ins_capture_ticket_data.bln_inf_selling_price = True
                                if str_line.find("\r") != -1:
                                    ins_capture_ticket_data.flt_selling_price_infant = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                else:
                                    ins_capture_ticket_data.flt_selling_price_infant = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_selling_price_infant = 0.0   



                        elif str_key == "FARE_DISCOUNT_GIVEN":
                            ins_capture_ticket_data.bln_discount_adt = True
                            try:
                                if str_line.find("\r") != -1:
                                    ins_capture_ticket_data.flt_rm_discount = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                else:
                                    ins_capture_ticket_data.flt_rm_discount = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_discount = 0.0
                                
                            ins_capture_ticket_data.flt_discount_given_ext = ins_capture_ticket_data.flt_rm_discount

                        elif str_key == "FARE_DISCOUNT_GIVEN_CHILD":
                            ins_capture_ticket_data.bln_discount_chd = True
                            try:
                                if str_line.find("\r") != -1:
                                    ins_capture_ticket_data.flt_rm_discount_chd = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                else:
                                    ins_capture_ticket_data.flt_rm_discount_chd = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_discount_chd = 0.0   

                        elif str_key == "FARE_DISCOUNT_GIVEN_INFANT":
                            ins_capture_ticket_data.bln_discount_inf = True
                            try:
                                if str_line.find("\r") != -1:
                                    ins_capture_ticket_data.flt_rm_discount_inf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                else:
                                    ins_capture_ticket_data.flt_rm_discount_inf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_discount_inf = 0.0   
                                
                        elif str_key == "FARE_PLB_DISCOUNT": #45196
                            try:
                                if str_line.find("\r") != -1:
                                    ins_capture_ticket_data.flt_rm_plb_discount = ins_general_methods.rm_str_to_flt(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                else:
                                    ins_capture_ticket_data.flt_rm_plb_discount = ins_general_methods.rm_str_to_flt(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_plb_discount = 0.0
                                
                        elif str_key == "FARE_DEAL_DISCOUNT": #45196
                            try:
                                if str_line.find("\r") != -1:
                                    ins_capture_ticket_data.flt_rm_deal_discount = ins_general_methods.rm_str_to_flt(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                else:
                                    ins_capture_ticket_data.flt_rm_deal_discount = ins_general_methods.rm_str_to_flt(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_rm_deal_discount = 0.0

                        elif str_key == 'FARE_SELLING_PRICE':
                            try:
                                ins_capture_ticket_data.bln_adt_selling_price = True
                                ins_capture_ticket_data.flt_rm_collection_amount = float(float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0]))
                                ins_capture_ticket_data.flt_selling_price_ext = float(float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0]))
                            except:
                                ins_capture_ticket_data.flt_rm_collection_amount = 0.0    
                                ins_capture_ticket_data.flt_selling_price_ext = 0.0   

                        elif str_key == "AGENCY_DEPARTMENT_CODE":
                            ins_capture_ticket_data.str_auto_invoice_branch_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    
                            ins_capture_ticket_data.str_branch_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_ENGAGEMENT_CODE":
                            ins_capture_ticket_data.str_cust_engagement_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_RESOURCE_CODE":
                            ins_capture_ticket_data.str_cust_resource_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_COMMITMENT_NO":
                            ins_capture_ticket_data.str_cust_commitment_no = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_ACCOUNTING_UNIT":
                            ins_capture_ticket_data.str_cust_accounting_unit = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_INTERNAL_AC_NO":
                            ins_capture_ticket_data.str_cust_internal_ac_no = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_PROJECT_CODE":
                            ins_capture_ticket_data.str_cust_project_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_ACTION_NO":
                            ins_capture_ticket_data.str_cust_action_no = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_APPROVER_NAME":
                            ins_capture_ticket_data.str_cust_approver_name = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_APPROVER_EMAIL":
                            ins_capture_ticket_data.str_cust_approver_email = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_EMPLOYEE_GRADE":
                            ins_capture_ticket_data.str_cust_employee_grade = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_COST_CENTRE":
                            ins_capture_ticket_data.str_cust_cost_centre = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CUST_DEPARTMENT":
                            ins_capture_ticket_data.str_cust_department = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "PARTY_MULTIPLE_FOP_YES_NO":
                            ins_capture_ticket_data.str_party_multiple_fop_yes_no = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]   

                        elif str_key == "PARTY_ADDITIONAL_AR":
                            ins_capture_ticket_data.str_party_additional_ar = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == 'PARTY_MAXIMUM_TICKETS':
                            try:
                                ins_capture_ticket_data.int_tickets_count_in_inv = int(str_line.replace(str_format,'').split("\r")[0].strip()[:10].strip().split(';')[0])
                                ins_capture_ticket_data.int_party_maximum_tickets = int(str_line.replace(str_format,'').split("\r")[0].strip()[:10].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.int_tickets_count_in_inv = 1 
                                ins_capture_ticket_data.int_party_maximum_tickets = 1 

                        elif str_key == "PARTY_FILE_JOB_CARD_NO":
                            ins_capture_ticket_data.str_party_file_job_card_no = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]       

                        elif str_key == "AGENCY_SALES_MAN":
                            ins_capture_ticket_data.str_agency_sales_man = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]        

                        elif str_key == "AGENCY_TRAACS_USER":
                            ins_capture_ticket_data.str_agency_traacs_user = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]        

                        elif str_key == "AGENCY_ADV_RECEIPT_NO":
                            ins_capture_ticket_data.str_agency_adv_receipt_no = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]        

                        elif str_key == "AGENCY_PRODUCT_CODE":
                            ins_capture_ticket_data.str_agency_product_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]        

                        elif str_key == "AGENCY_SUB_PRODUCT_CODE":
                            ins_capture_ticket_data.str_agency_sub_product_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]        

                        elif str_key == "AGENCY_AUTO_INVOICE_YES_NO":
                            ins_capture_ticket_data.str_agency_auto_invoice_yes_no = str_line.replace(str_format,'').split("\r")[0].strip()[:4].strip().split(';')[0]        

                        elif str_key == 'FARE_PUBLISHED':
                            flt_published_fare = 0.0

                            try:
                                ins_capture_ticket_data.bln_published_fare_adt = True
                                if str_line.find("\r") != -1:
                                    flt_published_fare = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_published_fare = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_published_fare_sector_wise[starting_segment] = flt_published_fare
                                else:
                                    ins_capture_ticket_data.flt_published_fare_ext = flt_published_fare
                            except:
                                ins_capture_ticket_data.flt_published_fare_ext = 0.0    

                        elif str_key == 'FARE_PUBLISHED_CHILD':
                            flt_published_fare_chd = 0.0
                            try:
                                ins_capture_ticket_data.bln_published_fare_chd = True
                                if str_line.find("\r") != -1:
                                    flt_published_fare_chd = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_published_fare_chd = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_child_published_fare_sector_wise[starting_segment] = flt_published_fare_chd
                                else:
                                    ins_capture_ticket_data.flt_published_fare_chd = flt_published_fare_chd

                            except:
                                ins_capture_ticket_data.flt_published_fare_chd = 0.0    

                        elif str_key == 'FARE_PUBLISHED_INFANT':
                            flt_published_fare_inf = 0.0
                            try:
                                ins_capture_ticket_data.bln_published_fare_inf = True
                                if str_line.find("\r") != -1:
                                    flt_published_fare_inf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_published_fare_inf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_infant_published_fare_sector_wise[starting_segment] = flt_published_fare_inf
                                else:
                                    ins_capture_ticket_data.flt_published_fare_inf = flt_published_fare_inf
                            except:
                                ins_capture_ticket_data.flt_published_fare_inf = 0.0    

                        elif str_key == 'FARE_ORIGINAL':
                            try:
                                ins_capture_ticket_data.flt_original_fare_ext = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                                ins_capture_ticket_data.flt_original_fare = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_original_fare_ext = 0.0    
                                ins_capture_ticket_data.flt_original_fare = 0.0    

                        elif str_key == 'FARE_PRINTING':
                            try:
                                ins_capture_ticket_data.flt_printing_fare_ext = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_printing_fare_ext = 0.0    

                        elif str_key == 'FARE_EXTRA_EARNING':

                            flt_ext_earning = 0.0
                            try:
                                ins_capture_ticket_data.bln_extra_earning_adt = True
                                if str_line.find("\r") != -1: # refer 34029
                                    flt_ext_earning = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_ext_earning = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])

                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:  # refer 30961
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_extra_earning_sector_wise[starting_segment] =  flt_ext_earning
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_inv = flt_ext_earning
                            except:
                                ins_capture_ticket_data.flt_extra_earning_inv = 0.0    

                        elif str_key == 'FARE_EXTRA_EARNING_CHILD':
                            flt_ext_earning_child = 0.0 # refer 34029
                            try:
                                ins_capture_ticket_data.bln_extra_earning_chd = True
                                if str_line.find("\r") != -1: # refer 34029
                                    flt_ext_earning_child = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_ext_earning_child = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])
                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_child_extra_earning_sector_wise[starting_segment] = flt_ext_earning_child
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_chd = flt_ext_earning_child
                            except:
                                ins_capture_ticket_data.flt_extra_earning_chd = 0.0    

                        elif str_key == 'FARE_EXTRA_EARNING_INFANT':
                            flt_ext_earning_inf = 0.0 # refer 34029

                            try:
                                ins_capture_ticket_data.bln_extra_earning_inf = True
                                if str_line.find("\r") != -1: # refer 34029
                                    flt_ext_earning_inf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0].split('/')[0])
                                else:
                                    flt_ext_earning_inf = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split(';')[0].split('/')[0])

                                if len(str_line.split('$'))>1 and len(str_line.split('$')[1]) >= 3:
                                    starting_segment = str_line.split('$')[1][:3]
                                    ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise[starting_segment] = flt_ext_earning_inf
                                else:
                                    ins_capture_ticket_data.flt_extra_earning_inf = flt_ext_earning_inf
                            except:
                                ins_capture_ticket_data.flt_extra_earning_inf = 0.0    

                        elif str_key == 'FARE_PAYBACK_COMMISSION':
                            try:
                                ins_capture_ticket_data.flt_payback_commission_ext = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_payback_commission_ext = 0.0    

                        elif str_key == 'FARE_CC_CHARGE_COLLECTED':
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_ext = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_ext = 0.0    
                                
                        elif str_key == 'FARE_CC_CHARGE_COLLECTED_CHILD':  #45305
                            ins_capture_ticket_data.bln_cc_charge_collected_chd = True
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_chd = ins_general_methods.rm_str_to_flt(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_chd = 0.0    
                            
                        elif str_key == 'FARE_CC_CHARGE_COLLECTED_INFANT':
                            ins_capture_ticket_data.bln_cc_charge_collected_inf = True
                            try:
                                ins_capture_ticket_data.flt_cc_charge_collected_inf = ins_general_methods.rm_str_to_flt(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_cc_charge_collected_inf = 0.0

                        elif str_key == 'FARE_LOWEST_OFFERED':
                            try:
                                ins_capture_ticket_data.flt_lowest_offered_ext = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_lowest_offered_ext = 0.0    

                        elif str_key == 'FARE_ACCEPTED_OR_PAID':
                            try:
                                ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_fare_accepted_or_paid_ext = 0.0    

                        elif str_key == 'FARE_LOST_AMOUNT':
                            try:
                                ins_capture_ticket_data.flt_fare_lost_amount_ext = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_fare_lost_amount_ext = 0.0    


                        elif str_key == "FARE_REASON_FOR_CHOOSE_HIGHER":
                            ins_capture_ticket_data.str_reason_for_choose_higher_ext = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "COMPLIANCE":
                            ins_capture_ticket_data.str_compliance_ext = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "CORPORATE_CARD_CODE":
                            ins_capture_ticket_data.str_corp_card_code_ext = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "AGAINST_DOCUMENT_NO":
                            ins_capture_ticket_data.str_against_doc_ext = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "PNR_TYPE":
                            ins_capture_ticket_data.str_pnr_type_ext = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]    

                        elif str_key == "AGENCY_PAYBACK_ACCOUNT":
                            ins_capture_ticket_data.str_pay_back_account_code = str_line.replace(str_format,'').split("\r")[0].strip()[:20].strip().split(';')[0]


                        elif str_key == 'SUBTYPE':
                                ins_capture_ticket_data.str_rm_sub_type = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]

                        elif str_key == 'REASON':
                            ins_capture_ticket_data.str_reason = str_line.replace(str_format,'').split("\r")[0].strip()[:200].strip().split(';')[0]
                        elif str_key == 'LOST':
                            ins_capture_ticket_data.str_lost = str_line.replace(str_format,'').split("\r")[0].strip()[:200].strip().split(';')[0]
                        elif str_key == 'EMPLOYEE EMAIL':
                            ins_capture_ticket_data.str_rm_emp_email = str_line.replace(str_format,'').split("\r")[0].strip()[:50].strip().split(';')[0]

                        elif str_key == 'ADDITIONAL SERVICE FEE':
                            try:
                                 ins_capture_ticket_data.flt_additional_service_charge = float(float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').strip().split("\r")[0].strip().split(';')[0]))
                            except:
                                 ins_capture_ticket_data.flt_additional_service_charge = 0.0

                        elif str_key == "EMD SERVICE FEE": #refer 12811
                            try:
                                ins_capture_ticket_data.flt_emd_service_charge = float(str_line.replace(str_format,'').replace(self.str_defult_currency_code,'').split("\r")[0].strip()[:20].strip().split(';')[0])
                            except:
                                ins_capture_ticket_data.flt_emd_service_charge = 0.0

                        elif str_key == "OPTION_1":
                            ins_capture_ticket_data.str_quot_option_1 = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]

                        elif str_key == "OPTION_2":
                            ins_capture_ticket_data.str_quot_option_2 = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]

                        elif str_key == "CUST_POS_ID": # refer 20879
                            ins_capture_ticket_data.int_credit_card_pos_id = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]

                        elif str_key == "CUST_CC_NUMBER": # refer 20879
                            ins_capture_ticket_data.str_cc_number = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]

                        elif str_key == "CRM_REFERENCE": # Refer 23236
                            ins_capture_ticket_data.str_crm_reference = str_line.replace(str_format,'').split("\r")[0].strip()[:499].strip().split(';')[0]

                        elif str_key == "MASTER_REFERENCE": # Refer 23236
                            ins_capture_ticket_data.str_master_refrence = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]

                        elif str_key == "MASTER_NARRATION": # Refer 23236
                            ins_capture_ticket_data.str_master_narration = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]

                        elif str_key == "LPO_DATE":  # Refer 34857
                            ins_capture_ticket_data.dat_rm_lpo_date = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]
                        
                        elif str_key == 'PASSIVE_SEGMENTS': #Refer #43745
                            str_lcc_number = str_line.replace(str_format,'').split("\r")[0].strip()[:200].split('/')[0].strip()
                            ins_capture_ticket_data.lst_lcc_ticket_voucher_number.append(str_lcc_number)
                        
                        elif str_key == "CUST_TRAVELLER_ID_LOYALTY_ID":  # Refer 45745
                            ins_capture_ticket_data.str_cust_traveller_id = str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0]

            except Exception as msg:
                raise Exception(msg)

            else:
                pass

            #39282
            try:
                if str_line[:2] in ('M8','M9') and str_line[2:4].isdigit():
                    for code in ins_general_methods.dct_sabre_optional_fields:
                        if code and code[:3] in ('M8*','M9*') and str_line[4:len(code)+1] == code[3:]: #43507
                            str_format = str_line[:len(code)+1]
                            if ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS' :
                                if isinstance(ins_general_methods.dct_sabre_optional_fields[code], list) and \
                                                    ins_general_methods.dct_sabre_optional_fields[code][0] == 'JSON': #43533
                                    ins_capture_ticket_data.json_user_defined_remark[ins_general_methods.dct_sabre_optional_fields[code][1]] = str_line.replace(str_format,'').split("\r")[0].split(';')[0].strip().upper()
                                    break
                                else:
                                    setattr(ins_capture_ticket_data,ins_general_methods.dct_sabre_optional_fields[code],str_line.replace(str_format,'').split("\r")[0].strip()[:49].strip().split(';')[0])
                            else:
                                ins_capture_ticket_data.dct_extra_capturing_data.update({ins_general_methods.dct_sabre_optional_fields[code] : str_line.replace(str_format,'').split("\r")[0].strip().split(';')[0]})
                        break
            except Exception as msg:
                raise Exception(msg)

            if str_line[:2] in ('M8','M9') :
                if str_line.strip() not in ins_capture_ticket_data.lst_rm_field_data :
                    if str_line.strip().find('\r') != -1 :
                        ins_capture_ticket_data.lst_rm_field_data.append(str_line.strip()[:str_line.strip().find('\r')])
                    else :
                        ins_capture_ticket_data.lst_rm_field_data.append(str_line.strip()[:str_line.strip().find('\r')])
            
            return ins_capture_ticket_data
            pass
        except:
            raise


    def generate_valid_date(self, str_date_string, str_issue_date = None):
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
            if str_date_string[2:] in dct_month:
                int_day = int(str_date_string[:2])
                int_month = dct_month[str_date_string[2:]]
                if (int_month < int_current_month) or (int_month == int_current_month and int_current_day > int_day):
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
    
    def get_details_of_file(self, str_file):
        lst_option = ['','','']
        lst_ticket = []
        try:
            fd = open(str_file, 'r')
            lst_file_data = fd.readlines()
            fd.close()
        except:
            return 'Error'
        try:
            str_line = lst_file_data[0]
            int_count = 1
            mess_id = None
            while 1:
                mess_id = 'M' + str(int_count)
                m1_pos = str_line.find(mess_id)
                if m1_pos != -1 and str_line[m1_pos -1 :m1_pos] == '\r':
                    lst_option[1] = "F"
                    str_line = str_line[m1_pos:]
                    
                    if str_line[:2] == "AA" and str_line[11:13] == "M0" and str_line[13:14].strip() == '5':
                        str_ticket_number = str_line[29:40].strip()
                        if str_ticket_number:
                            lst_option[0] = "T"
                            lst_ticket.append(str_ticket_number)
                    if str_line[:2] == 'M2':
                        str_ticket_number =str_line[233:243]
                        if str_ticket_number:
                            lst_option[0] = "T"
                            lst_ticket.append(str_ticket_number)
                        try:
                            int_no_of_conj = int(str_line[243:244])
                            while int_no_of_conj > 0:
                                int_no_of_conj -= 1
                                lst_ticket.append((str(int(str_ticket_number) + 1)))
                        except:
                            pass
                        pass
                int_count = int_count + 1
                if int_count == 10:
                    
                    str_line.find('\rMG')
                    
                    break
                pass
            
            lst_option[2] = lst_ticket
        except:
            pass
        return lst_option

# //  CREATE INS
if __name__ == "__main__":
    ins_capture = Capture()
    # // CREATE DIRECTORY
    

    # // Move not parsed folder files to parent folder
    #print '@@@ Move Not parsed files to parent folder'
    ins_capture.move_not_parsed_folder_files_to_parent_folder()

    #// if auto invoice is active then changing Path and Woking Directory to parent
    # //Directory to avoid problems of relative import in the case of auto invoice
    str_current_path = os.getcwd()
    #str_module_path = str_current_path.replace( 'ticketCapture','')
    #sys.path.append( str_module_path )
    #os.chdir(str_module_path)

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
