"""
Purpose: To create ticket base instances
Owner  : KISHOR PS
Date   : 
Re. F/M:
Last Update: 
"""
import os
import os.path
import time
import datetime
import re
import json


try:
    from ticketCapture.lib import generalMethods
    from ticketCapture.lib import generalMethodsWave
    import ticketCapture.instanceBase as instanceBase
    import ticketCapture.saveOrUpdateData as saveOrUpdateData
    import ticketCapture.saveOrUpdateDataWave as saveOrUpdateDataWave
except:
    from lib import generalMethods
    from lib import generalMethodsWave
    import saveOrUpdateData
    import saveOrUpdateDataWave
    import instanceBase

try:
    ins_general_methods = generalMethods.GeneralMethods()
    if ins_general_methods.dct_conf_data['TRAACS_VERSION'] != 'SAAS':
        ins_general_methods = generalMethodsWave.GeneralMethods()
        ins_save_or_update_data = saveOrUpdateDataWave.captureDB()
    else:
        ins_save_or_update_data = saveOrUpdateData.captureDB()
        
except Exception as msg:
    print ('Connection Error..',msg)
    raise
 
class createInstance():
    
    def __init__(self, *args):
        pass
    
    def create_ticket_data_to_save(self ,ins_capture_ticket_data , str_file, lst_emd_tickets = [] ):
        lst_tickets = []
        lst_ticket_capture_details = []
        str_dom_sectors = ""
        int_airline_id = None
        int_airline_account_id = None

        # // For making emd ticket Auto capturing
        if lst_emd_tickets:
            lst_tickets = lst_emd_tickets
        #// Refer 9167
        if ins_capture_ticket_data.flt_rm_net_fare :
               if ins_capture_ticket_data.flt_published_fare :
                    ins_capture_ticket_data.flt_rm_service_charge = ins_capture_ticket_data.flt_rm_net_fare - (ins_capture_ticket_data.flt_published_fare + ins_capture_ticket_data.flt_tax)
               elif ins_capture_ticket_data.flt_market_fare:
                    ins_capture_ticket_data.flt_rm_service_charge = ins_capture_ticket_data.flt_rm_net_fare - (ins_capture_ticket_data.flt_market_fare + ins_capture_ticket_data.flt_tax)
               else :
                   ins_capture_ticket_data.flt_rm_service_charge = ins_capture_ticket_data.flt_rm_net_fare - ins_capture_ticket_data.flt_tax

        int_ticket_count = 1
        int_count = 1
        lst_temp = []
#        for i in ins_capture_ticket_data.lst_ticket_detls:
#            for jd in range(0,99):
#                new = i.copy()
#                new[0] = i[0]+str(jd).zfill(2)
#                lst_temp.append(new)
#        ins_capture_ticket_data.lst_ticket_detls = lst_temp
        for lst_ticket_detls in ins_capture_ticket_data.lst_ticket_detls:
            # // Ticket Details
            [str_ticket_number,
                str_conjection_ticket_number,
                chr_ticket_type,
                str_ticketing_airline_character_code,
                str_ticketing_airline_numeric_code,
                str_pax_name,
                str_pax_type,
                flt_service_charge,
                str_isection_mobile,
                str_isection_email,
                int_pax_item_number] = lst_ticket_detls
            lst_tickets.append(str_ticket_number)
            
            str_tax = ''
            flt_vat = 0.0
            lst_tax = []
            str_basic_fare = 0.0
            flt_net_amt = 0.0
            flt_travel_agency_tax = 0.0
            flt_commision_amount = 0.0
            flt_commision_percetage = 0.0
            str_total_tax = ''
            str_int_dom = ''
            str_original_issue = ''
            str_fare_currency_code = ''
            str_card_approval_code = ''
            str_emd_remarks = ''
            #common EMD dictionary for sabre,galileo and worldspan
            if ins_capture_ticket_data.dct_emd_ticket_details and str_ticket_number in ins_capture_ticket_data.dct_emd_ticket_details:
                [str_emd_ticket_number,
                str_pax_name,
                str_pax_type,
                str_basic_fare,
                str_total_tax,
                str_emd_total_fare_currency,
                lst_emd_tax_details,
                str_emd_remarks,
                str_emd_connection_ticket_number,
                str_emd_cc_type ,
                str_emd_cc_no ,
                str_emd_cc_approval_code ,
                str_emd_issue_date] = ins_capture_ticket_data.dct_emd_ticket_details[str_ticket_number]
                ins_capture_ticket_data.bln_emd = True
                ins_capture_ticket_data.lst_sector = []
                ins_capture_ticket_data.str_first_departure_date = None
                if not ins_capture_ticket_data.lst_tax and lst_emd_tax_details:
                    ins_capture_ticket_data.lst_tax = lst_emd_tax_details
                ins_capture_ticket_data.str_connection_ticket = str_emd_connection_ticket_number   
            
            if not ins_capture_ticket_data.str_ticketing_airline_character_code and ins_capture_ticket_data.str_airline_code: # refer 25579
                str_ticketing_airline_character_code = ins_capture_ticket_data.str_airline_code
            elif ins_capture_ticket_data.str_ticketing_airline_character_code : #41722
                str_ticketing_airline_character_code = ins_capture_ticket_data.str_ticketing_airline_character_code

            # // Airline Details
            str_airline_numeric_code = str_ticketing_airline_numeric_code
            rst = ins_general_methods.get_airline_data(str_ticketing_airline_numeric_code,str_ticketing_airline_character_code)
#                print "str_ticketing_airline_numeric_code, str_ticketing_airline_character_code"
#                print str_ticketing_airline_numeric_code,'<>', str_ticketing_airline_character_code,rst
            if rst:
                int_airline_id = rst['pk_bint_airline_id']
                int_airline_account_id = rst['fk_bint_airline_account_id']
            else:
#                    
                int_airline_id = None
                int_airline_account_id = None

                if not str_ticketing_airline_character_code:
                    str_ticketing_airline_character_code = str_airline_numeric_code

                if ins_capture_ticket_data.str_ticketing_airline_name:
                    str_ticketing_airline_name = ins_capture_ticket_data.str_ticketing_airline_name
                else:
                    str_ticketing_airline_name = str_ticketing_airline_character_code+str_airline_numeric_code
                if str_ticketing_airline_name:
                    str_ticketing_airline_name = str_ticketing_airline_name.replace("\r",'').replace("\n",'')
                if str_ticketing_airline_character_code:
                    str_ticketing_airline_character_code = str_ticketing_airline_character_code.replace("\r",'').replace("\n",'')
                if str_airline_numeric_code:
                    str_airline_numeric_code = str_airline_numeric_code.replace("\r",'').replace("\n",'')

                if str_ticketing_airline_name and \
                str_ticketing_airline_character_code and \
                str_airline_numeric_code :
                    int_airline_id ,int_airline_account_id = ins_save_or_update_data.add_new_airline_into_db(str_ticketing_airline_character_code,
                                                                        str_airline_numeric_code ,
                                                                        str_ticketing_airline_name)

                    str_dom_sectors = ''
                else:
                    if ins_capture_ticket_data.bln_xo_capturing and ins_general_methods.ins_auto_inv.bln_xo_capture: # refer 25579
                        print('Airline %s is missing in master. Please Add it.')
                        pass
                    
                if not int_airline_id and ins_capture_ticket_data.lst_airline_data:
                    for tpl_airline_data in ins_capture_ticket_data.lst_airline_data: #J-389
                        if tpl_airline_data[1] or tpl_airline_data[2]:
                            rst_airline = ins_general_methods.get_airline_data(tpl_airline_data[2], tpl_airline_data[1])
                            if rst_airline and rst_airline['pk_bint_airline_id']:
                                int_airline_id = rst_airline['pk_bint_airline_id']
                                int_airline_account_id = rst_airline['fk_bint_airline_account_id']
                            else:
                                int_airline_id,int_airline_account_id = ins_save_or_update_data.ins_capture_db.add_new_airline_into_db(tpl_airline_data[2],
                                                                                                        tpl_airline_data[1],
                                                                                                        tpl_airline_data[3])         
            
            #J-340
            if not int_airline_id and not ins_capture_ticket_data.bln_refund and not ins_capture_ticket_data.str_void_date:
                raise Exception('NOTICKET Airline data missing.')
                
            #for sabre tickets
            if not ins_capture_ticket_data.bln_emd and ins_capture_ticket_data.dct_ticket_details and str_ticket_number in ins_capture_ticket_data.dct_ticket_details:
                [str_ticket_number,
                    str_last_conj,
                    str_airline_code,
                    str_pax_name_1,
                    str_pax_type_1,
                    str_basic_fare,
                    flt_total_fare_amt,
                    str_total_tax,
                    lst_tax_details,
                    str_tax_1,
                    str_fare_currency_code,
                    str_int_dom,
                    str_currency_total_fare,
                    str_original_issue,
                    str_card_approval_code,
                    flt_net_amt,
                    flt_travel_agency_tax,
                    flt_commision_amount,
                    flt_commision_percetage,
                    str_no_of_segments,
                    str_m4_sqeuence_number] = ins_capture_ticket_data.dct_ticket_details[str_ticket_number]

                if not ins_capture_ticket_data.lst_tax and lst_tax_details:
                    ins_capture_ticket_data.lst_tax = lst_tax_details
                ins_capture_ticket_data.lst_sector = ins_capture_ticket_data.str_sector.split('/')
                ins_capture_ticket_data.lst_sector.pop(0) # removing first segment for adjust into same method
                #44255
                for lst_tmp in ins_capture_ticket_data.lst_temp_sector_details:
                    if str_no_of_segments == '01' or (lst_tmp[16] and ins_capture_ticket_data.dct_stop_over[lst_tmp[16]] == 'O'):
                        for lst_tmp_sector in ins_capture_ticket_data.lst_sector_details:
                            if lst_tmp[3] == lst_tmp_sector[0] and lst_tmp[4] == lst_tmp_sector[1] :
                                lst_tmp_sector[14] = True
                    
                if (len(ins_capture_ticket_data.dct_ticket_details.keys()) > len(ins_capture_ticket_data.dct_pax_name.keys()) and not ins_capture_ticket_data.bln_emd) or\
                        (len(ins_capture_ticket_data.dct_ticket_details.keys()) > len(ins_capture_ticket_data.dct_pax_name.keys()) and ins_capture_ticket_data.bln_emd and ins_capture_ticket_data.bln_advance_deposit_file):                        
                    ins_capture_ticket_data.str_previous_dest_code = ''
                    ins_capture_ticket_data.str_sector = ''
                    ins_capture_ticket_data.lst_sector = []
                    ins_capture_ticket_data.lst_sector_details = []
                    for int_index in range(0, len(str_m4_sqeuence_number), 2): #42277
                        for lst_temp_sector in ins_capture_ticket_data.lst_temp_sector_details:
                            bln_open_segment = False
                            if lst_temp_sector[15] != 'E' and int(str_m4_sqeuence_number[int_index:int_index+2]) in ins_capture_ticket_data.dct_m4_sequence \
                                    and ins_capture_ticket_data.dct_m4_sequence[int(str_m4_sqeuence_number[int_index:int_index+2])] == int(lst_temp_sector[16]):
                                str_dep_city = lst_temp_sector[3]
                                str_arrival_city = lst_temp_sector[4]

                                if ins_capture_ticket_data.str_previous_dest_code and ins_capture_ticket_data.str_previous_dest_code != str_dep_city:
                                    ins_capture_ticket_data.str_sector = ins_capture_ticket_data.str_sector +  "//" + str_dep_city
                                    ins_capture_ticket_data.lst_sector.extend(['',str_dep_city])
                                if not ins_capture_ticket_data.str_sector:
                                    ins_capture_ticket_data.str_sector =  str_dep_city +"/"+str_arrival_city
                                else:
                                    ins_capture_ticket_data.str_sector = ins_capture_ticket_data.str_sector +"/"+str_arrival_city
                                ins_capture_ticket_data.lst_sector.append(str_arrival_city)
                                ins_capture_ticket_data.str_previous_dest_code = str_arrival_city
                                if str_no_of_segments == '01' or ins_capture_ticket_data.dct_stop_over[lst_temp_sector[16]] == 'O':
                                    bln_open_segment = True
                                    
                                ins_capture_ticket_data.lst_sector_details.append([ lst_temp_sector[3],
                                                                        lst_temp_sector[4],
                                                                        lst_temp_sector[0],
                                                                        '',
                                                                        lst_temp_sector[9],
                                                                        lst_temp_sector[8],
                                                                        lst_temp_sector[8],
                                                                        lst_temp_sector[1],
                                                                        lst_temp_sector[2],
                                                                        False,#bln_stopover_permitted,
                                                                        0, # Mileage
                                                                        0.0,# Sector wise fare
                                                                        lst_temp_sector[12],
                                                                        lst_temp_sector[11],
                                                                        bln_open_segment])
                    
            # // Tax String
            for tup_tax in ins_capture_ticket_data.lst_tax:
                (str_tax_amount,
                    str_tax_code,
                    str_nature_code) = tup_tax
                if str_tax_code:
                    lst_tax.append(str_tax_code + '=' + str_tax_amount)

                # refer 30830
                if ins_general_methods.ins_auto_inv.str_input_vat_code and str_tax_code.upper() == ins_general_methods.ins_auto_inv.str_input_vat_code:
                    flt_vat += float(str_tax_amount)
            str_tax = ','.join(lst_tax)


            # // Fare Details => 'A07' -> FARE VALUE DATA Galileo
            if not ins_capture_ticket_data.bln_emd and int_pax_item_number in ins_capture_ticket_data.dct_fare_details :
                (str_basic_fare_currency_code,
                    str_basic_fare,
                    str_total_amount_currency_code,
                    str_total_amount,
                    str_equivalent_amount_currency_code,
                    str_equivalent_amount,
                    str_tax_currency_code,
                    str_tax,
                    str_total_tax,
                    flt_vat ) = ins_capture_ticket_data.dct_fare_details[int_pax_item_number]
            # // Original Ticket => 'A10' -> EXCHANGE TICKET INFORMATION
            if int_pax_item_number in ins_capture_ticket_data.dct_original_ticket_data:#41208
                (str_pax_exchange_item_numder,
                    str_original_issue) = ins_capture_ticket_data.dct_original_ticket_data[int_pax_item_number]
            
            #galileo
            if not ins_capture_ticket_data.bln_emd and ins_capture_ticket_data.lst_airline_data:
                lst_sector = []
                str_previous_destination_city_code = ''
                str_airline_numeric_code = ''
                str_airline_character_code = ''
                ins_capture_ticket_data.lst_sector_details = []
                for tpl_airline_data in ins_capture_ticket_data.lst_airline_data:
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
                        str_departure_dd_mmm_yy) = tpl_airline_data     #40776


                    # // 9211 To consider the case of open Jow to save sector
                    bln_open_segment = False
                    if str_previous_destination_city_code and not str_previous_destination_city_code == str_origin_code:
                        ins_capture_ticket_data.int_number_of_segments = ins_capture_ticket_data.int_number_of_segments -1
                        bln_open_segment = True
                        lst_sector.append('')
                        lst_sector.append(str_origin_code)
                        pass
                    lst_sector.append(str_destination_city_code)
                    str_previous_destination_city_code = str_destination_city_code  # refer 31014
                    # // Refer #10657 preparing list to Inserting data to sector details --------------
                    #Refer #40441
                    try :
                        str_departure_date = time.strptime((str_departure_dd_mmm + ins_capture_ticket_data.str_first_travel_date[-4:]),"%d%b%Y")
                    except :
                        str_departure_date = time.strptime((str_departure_dd_mmm + str(int(ins_capture_ticket_data.str_first_travel_date[-4:])+1)),"%d%b%Y")

                    str_departure_date = time.strftime("%d/%m/%Y", str_departure_date)
                    #38056
                    if ins_capture_ticket_data.str_ntd_issue_date:
                        ins_capture_ticket_data.str_mir_creation_date = time.strftime("%d/%m/%Y", time.strptime(ins_capture_ticket_data.str_ntd_issue_date, "%d%b%y"))
                    #40776    
                    if str_departure_dd_mmm_yy:
                        str_departure_date = time.strftime("%d/%m/%Y", time.strptime(str_departure_dd_mmm_yy, "%d%b%y"))

                    elif ins_capture_ticket_data.str_mir_creation_date and self.ins_capture_db.strp_date_time(str_departure_date) < self.ins_capture_db.strp_date_time(ins_capture_ticket_data.str_mir_creation_date) \
                                            and not ins_capture_ticket_data.bln_is_refund: #38330
                        str_departure_date = time.strptime((str_departure_dd_mmm + str(int(ins_capture_ticket_data.str_first_travel_date[-4:])+1)),"%d%b%Y")
                        str_departure_date = time.strftime("%d/%m/%Y", str_departure_date)

#                        if not ins_capture_ticket_data.str_first_departure_date:
#                            ins_capture_ticket_data.str_first_departure_date = str_departure_date

                    str_arrival_date = str_departure_date

                    bln_stopover_permitted = False
                    if str_stopover_indicators =='O' :
                        bln_stopover_permitted =True

                    ins_capture_ticket_data.lst_sector_details.append([ str_origin_code,
                                                                        str_destination_city_code,
                                                                        str_airline_character_code,
                                                                        str_airline_numeric_code,
                                                                        str_flight_number,
                                                                        str_booking_class_of_service,
                                                                        str_booking_class_of_service,
                                                                        str_arrival_date,
                                                                        str_departure_date,
                                                                        bln_stopover_permitted,
                                                                        0, # Mileage
                                                                        0.0,# Sector wise fare
                                                                        str_arrival_time,
                                                                        str_departure_time,
                                                                        bln_open_segment])
                ins_capture_ticket_data.lst_sector = lst_sector
                pass
            
            
            #get region code
            str_region_code = ''
            lst_sector = [ins_capture_ticket_data.str_start_port_code] #J-360
            lst_sector.extend(ins_capture_ticket_data.lst_sector)
                
            ## Check whether the sector is in region table.
            dct_region_details = ins_general_methods.get_region_details()
            for str_region in list(dct_region_details.keys()):
                lst_region = dct_region_details[str_region].split(',')
                lst_region_segments = [str_code.strip() for str_code in lst_region]
                if lst_sector and not (set(list(filter(None,lst_sector))) - set(lst_region_segments)):
                    str_region_code = str_region
                    break
            
            ## Check whether the sector is a DOM
            if str_region_code == '' and str_dom_sectors != '':
                lst_dom_sector = str_dom_sectors.split(',')
                lst_dom_segments = [str_code.strip() for str_code in lst_dom_sector]
                if lst_sector and not (set(list(filter(None,lst_sector))) - set(lst_dom_segments)):
                    str_region_code = 'DOM'
                else:
                    str_region_code = 'INT'
            elif str_region_code == '' and str_dom_sectors == '':
                str_region_code = 'INT'

            if str_int_dom:
                str_region_code = str_region_code
            # // set data
            ins_ticket_base = instanceBase.TicketBase()
            ins_ticket_base.str_crs_company = ins_capture_ticket_data.str_crs_company
            ins_ticket_base.int_ticket_id = None
            ins_ticket_base.str_pnr_no = ins_capture_ticket_data.str_pnr_no.strip()
            ins_ticket_base.str_our_lpo = ''
            ins_ticket_base.str_ticket_number = str_ticket_number
            ins_ticket_base.str_last_conjection_ticket_number = str_conjection_ticket_number
            # If hand ticket no exist it will be taken as original issue ticket no
            if ins_capture_ticket_data.str_base_ticket_number :
                ins_ticket_base.str_base_ticket_number = ins_capture_ticket_data.str_base_ticket_number
            else :
                ins_ticket_base.str_base_ticket_number = ''
            ins_ticket_base.str_original_issue = ins_capture_ticket_data.str_original_issue or str_original_issue
            ins_ticket_base.str_hand_ticket_number = ins_capture_ticket_data.str_hand_ticket_number
            ins_ticket_base.str_pricing_code = ins_capture_ticket_data.str_pricing_code
            ins_ticket_base.bln_reissue_fop = ins_capture_ticket_data.bln_reissue_fop
            ins_ticket_base.str_email = ins_capture_ticket_data.str_rm_email
            ins_ticket_base.int_tickets_count_in_inv = ins_capture_ticket_data.int_tickets_count_in_inv
            if ins_capture_ticket_data.bln_refund:
                ins_ticket_base.str_remarks_rfd = ins_capture_ticket_data.str_ticket_rm_remarks
            else:
                ins_ticket_base.str_remarks = ins_capture_ticket_data.str_ticket_rm_remarks

            if ins_capture_ticket_data.bln_refund:
                ins_ticket_base.chr_ticket_status = 'R'
            else:
                ins_ticket_base.chr_ticket_status = 'I'

            ins_ticket_base.int_dc_card = None
            ins_ticket_base.bln_uccf = False
            ins_ticket_base.int_supplier_id  = None

            if ins_ticket_base.str_crs_company == 'Galileo':
                if ins_capture_ticket_data.str_ntd_issue_date:
                    ins_ticket_base.str_ticket_issue_date = time.strftime("%d/%m/%Y", time.strptime(ins_capture_ticket_data.str_ntd_issue_date, "%d%b%y"))
                else:    
                    ins_ticket_base.str_ticket_issue_date = ins_capture_ticket_data.str_file_creation_date
            elif ins_ticket_base.str_crs_company == 'Worldspan':
                ins_ticket_base.str_ticket_issue_date = ins_capture_ticket_data.str_ticket_issue_date
            else:
                ins_ticket_base.str_ticket_issue_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_file_creation_date)

            # // Set additional fields
            #ins_ticket_base.str_account_name_inv = ins_capture_ticket_data.str_account_name_inv
            ins_ticket_base.str_departure_date = ins_capture_ticket_data.str_departure_date
            #ins_ticket_base.str_airline_name = ins_capture_ticket_data.str_airline_name
#                ins_ticket_base.str_orgin_name = ins_capture_ticket_data.str_orgin_name
#                ins_ticket_base.str_dest_name = ins_capture_ticket_data.str_dest_name
            ins_ticket_base.str_departure_time = ins_capture_ticket_data.str_departure_time
            ins_ticket_base.str_arrival_time = ins_capture_ticket_data.str_arrival_time

#                        ins_ticket_base.dat_card_authorization = ins_ticket_base.str_ticket_issue_date
#                if ins_ticket_base.str_ticket_issue_date :
#                    ins_ticket_base.tim_card_authorization = ins_ticket_base.str_ticket_issue_date+' '+time.strftime("%H:%M:%S",time.localtime(os.path.getctime(str_file)))
            if ins_capture_ticket_data.str_pnr_creation_date :
                ins_ticket_base.str_ticket_booking_date = ins_general_methods.generate_valid_booking_date(ins_capture_ticket_data.str_pnr_creation_date)

            # // Added new Ticket type EMD in case of an EMD ticket Refer #9833
            if ins_capture_ticket_data.bln_vmco and chr_ticket_type == 'VMCO' :
                ins_ticket_base.str_ticket_type = 'VMPD'
                ins_ticket_base.str_original_issue = ''
            elif ins_capture_ticket_data.bln_emd:
                ins_ticket_base.str_ticket_type = 'EMD'
                ins_ticket_base.str_original_issue = ''
            else:
                ins_ticket_base.str_ticket_type = 'ET'
            ins_ticket_base.flt_overriding_commn_percentage  = 0.00
            ins_ticket_base.str_booking_agent_code = ins_capture_ticket_data.str_booking_agent_code
            ins_ticket_base.str_booking_agent_numeric_code = ins_capture_ticket_data.str_booking_agent_numeric_code
            ins_ticket_base.str_ticketing_agent_code = ins_capture_ticket_data.str_ticketing_agent_code
            ins_ticket_base.str_ticketing_agent_numeric_code = ins_capture_ticket_data.str_ticketing_agent_numeric_code

            (ins_ticket_base.int_counter_staff_id_inv,#refs 21409
                str_mapping_client_account,
                int_mapping_client_department_id,
                int_mapping_client_location_id,
                    int_counter_staff_cost_center ,
                        int_counter_staff_department,
                        str_counter_staff_numeric_code,
                        str_cust_cash_acc_id_tick) = ins_general_methods.get_counter_staff_id(ins_ticket_base.str_crs_company,
                                                                                ins_ticket_base.str_ticketing_agent_code,
                                                                                ins_ticket_base.str_ticketing_agent_numeric_code,
                                                                                ins_general_methods.ins_auto_inv.bln_staff_num_code_match
                                                                                )
            if str_counter_staff_numeric_code and  not ins_general_methods.ins_auto_inv.bln_staff_num_code_match :
                ins_ticket_base.str_ticketing_agent_numeric_code = str_counter_staff_numeric_code

            #//refer 23491
            (ins_ticket_base.int_booking_agent_counter_staff_id,
                    str_book_mapping_client_account,
                    int_book_mapping_client_department_id,
                    int_book_mapping_client_location_id,
                    int_book_counter_staff_cost_center ,
                        int_book_counter_staff_department,
                        str_booking_agent_numeric_code,
                        str_cust_cash_acc_id_book) = ins_general_methods.get_counter_staff_id(ins_ticket_base.str_crs_company,
                                                                        ins_ticket_base.str_booking_agent_code,
                                                                        ins_ticket_base.str_booking_agent_numeric_code,
                                                                        ins_general_methods.ins_auto_inv.bln_staff_num_code_match) #Ref No-17134 
            if str_booking_agent_numeric_code and  not ins_general_methods.ins_auto_inv.bln_staff_num_code_match :
                ins_ticket_base.str_booking_agent_numeric_code = str_booking_agent_numeric_code

            if ins_general_methods.ins_auto_inv.bln_capture_client_set_in_book_agent:
                str_mapping_client_account = str_book_mapping_client_account
                int_mapping_client_department_id = int_book_mapping_client_department_id
                int_mapping_client_location_id = int_book_mapping_client_location_id
                int_counter_staff_cost_center = int_book_counter_staff_cost_center
                int_counter_staff_department = int_book_counter_staff_department


            ins_ticket_base.int_airline_id = int_airline_id
            ins_ticket_base.int_airline_account_id = int_airline_account_id

            ins_ticket_base.str_ticketing_airline_character_code = str_ticketing_airline_character_code
            ins_ticket_base.str_ticketing_airline_numeric_code = str_ticketing_airline_numeric_code or ins_capture_ticket_data.str_ticketing_airline_numeric_code
            ins_ticket_base.str_ticketing_airline_name = ins_capture_ticket_data.str_ticketing_airline_name


#                ins_ticket_base.str_airline_character_code  = str_airline_code
#                ins_ticket_base.str_airline_numeric_code = str_airline_numeric_code
#                ins_ticket_base.str_airline_name = str_airline_name
            ins_ticket_base.str_region_code = str_region_code
            ins_ticket_base.str_sector = '/'.join(ins_capture_ticket_data.lst_sector)
            ins_ticket_base.str_sector = ins_capture_ticket_data.str_start_port_code + '/' + ins_ticket_base.str_sector

            #38116
            if not ins_capture_ticket_data.bln_emd :
                ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector[ins_ticket_base.str_ticket_number] = [ins_ticket_base.str_sector,ins_ticket_base.str_region_code] #38326

            ins_save_or_update_data.save_airport(ins_ticket_base.str_sector.split('/'))
            ins_ticket_base.str_destination, ins_ticket_base.bln_return = ins_general_methods.get_destination_airport_code(ins_ticket_base.str_sector) #40613

            if ins_capture_ticket_data.bln_emd : #refer 19527
                ins_ticket_base.str_sector = ins_capture_ticket_data.str_issuance_subcode_descr.upper().strip() or str_emd_remarks.upper().strip()
                ins_ticket_base.bln_auto_invoice_emd_tickets = ins_save_or_update_data.save_emd_sector(ins_ticket_base.str_sector)


            ins_ticket_base.int_no_of_segments = ins_capture_ticket_data.int_number_of_segments
            ins_ticket_base.int_number_of_segments_rfd = ins_capture_ticket_data.int_number_of_segments_rfd

            ins_ticket_base.str_itinerary_details = ''
            try:
                ins_ticket_base.str_class = ins_capture_ticket_data.lst_sector_details[0][5]
            except:
                pass

            if ins_capture_ticket_data.str_class_of_booking:
                ins_ticket_base.str_class = ins_capture_ticket_data.str_class_of_booking
            ins_ticket_base.str_tour_code = ins_capture_ticket_data.str_tour_code
            try:
                if ins_capture_ticket_data.lst_fare_basis:
                    ins_ticket_base.str_fare_basis = ins_capture_ticket_data.lst_fare_basis[0]
                elif int_pax_item_number in ins_capture_ticket_data.dct_fare_basis_details :
                    (int_pax_item_number,
                        str_segment_number,
                        ins_ticket_base.str_fare_basis) = ins_capture_ticket_data.dct_fare_basis_details[int_pax_item_number]

            except:
                pass
            ins_ticket_base.str_pax_type = str_pax_type
            ins_ticket_base.str_pax_name = str_pax_name
            ins_ticket_base.str_currency_type_code = ins_capture_ticket_data.str_currency_type_code or str_fare_currency_code
            ins_ticket_base.flt_it_total = ins_capture_ticket_data.flt_it_total
            ins_ticket_base.str_it_total = ins_capture_ticket_data.str_it_total

            ins_ticket_base.flt_published_fare_inv = ins_capture_ticket_data.flt_published_fare or float(str_basic_fare or 0)

            if ins_capture_ticket_data.flt_published_fare_ext :
                ins_ticket_base.flt_published_fare_inv = ins_capture_ticket_data.flt_published_fare_ext

            if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_published_fare_sector_wise:
                ins_ticket_base.flt_published_fare_inv = ins_general_methods.ins_global.dct_published_fare_sector_wise[ins_ticket_base.str_sector[:3]]

            if ins_ticket_base.str_pax_type == 'CHD' and ins_capture_ticket_data.bln_published_fare_chd :
                if ins_capture_ticket_data.flt_published_fare_chd:
                    ins_ticket_base.flt_published_fare_inv = abs(ins_capture_ticket_data.flt_published_fare_chd)

                if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_child_published_fare_sector_wise:
                    ins_ticket_base.flt_published_fare_inv = ins_general_methods.ins_global.dct_child_published_fare_sector_wise[ins_ticket_base.str_sector[:3]]

            elif ins_ticket_base.str_pax_type == 'INF' and ins_capture_ticket_data.bln_published_fare_inf:
                if ins_capture_ticket_data.flt_published_fare_inf:
                    ins_ticket_base.flt_published_fare_inv = abs(ins_capture_ticket_data.flt_published_fare_inf)

                if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_infant_published_fare_sector_wise:
                    ins_ticket_base.flt_published_fare_inv = ins_general_methods.ins_global.dct_infant_published_fare_sector_wise[ins_ticket_base.str_sector[:3]]

            if not ins_capture_ticket_data.flt_market_fare:
                ins_ticket_base.flt_market_fare_inv = ins_capture_ticket_data.flt_published_fare or float(str_basic_fare or 0)
            else:
                ins_ticket_base.flt_market_fare_inv = ins_capture_ticket_data.flt_market_fare

            if not ins_ticket_base.flt_published_fare_inv :
                ins_ticket_base.flt_published_fare_inv = ins_ticket_base.flt_market_fare_inv
            # refer 28892  
            if not ins_ticket_base.flt_published_fare_inv and ins_ticket_base.str_currency_type_code not in (ins_capture_ticket_data.str_defult_currency_code,)\
                    and ins_capture_ticket_data.flt_total_amount_collected and (not ins_general_methods.ins_capture_base.bln_multi_currency):
                ins_ticket_base.flt_published_fare_inv = float(ins_capture_ticket_data.flt_total_amount_collected) -ins_capture_ticket_data.flt_tax
                ins_ticket_base.flt_market_fare_inv = float(ins_capture_ticket_data.flt_total_amount_collected) -ins_capture_ticket_data.flt_tax
#                if ins_ticket_base.flt_special_fare_inv == 0.00 :
            ins_ticket_base.flt_total_tax_inv = ins_capture_ticket_data.flt_tax or float(str_total_tax or 0)
            
            ins_ticket_base.flt_it_market_fare = float(flt_net_amt or 0.0)   # refer 25702
            ins_ticket_base.flt_it_tax = float(flt_travel_agency_tax or 0.0)
            ins_ticket_base.str_suppression_indicator = ins_capture_ticket_data.str_suppression_indicator
            if ins_ticket_base.str_original_issue and ins_ticket_base.str_crs_company == 'Sabre' :
                int_orig_tkt_seg_count = ins_general_methods.get_original_issue_ticket_segment_count(ins_ticket_base.str_original_issue)
                ins_ticket_base.int_no_of_segments = ins_ticket_base.int_no_of_segments - int_orig_tkt_seg_count

                ins_ticket_base.flt_published_fare_inv = float(ins_ticket_base.flt_it_market_fare)
                ins_ticket_base.flt_market_fare_inv = float(ins_ticket_base.flt_it_market_fare)
                ins_ticket_base.flt_total_tax_inv = float(ins_ticket_base.flt_it_tax)
                ins_ticket_base.flt_special_fare_inv = float(ins_ticket_base.flt_it_market_fare)

            if ins_ticket_base.str_suppression_indicator  in ('N','D','R'):  # refer 21167
                # suppresion indicator is a flag which decides which fare should be taken as the market fare whether the net fare or normal fare
                ins_ticket_base.flt_market_fare_inv = float(ins_ticket_base.flt_it_market_fare)
                ins_ticket_base.flt_published_fare_inv = float(ins_ticket_base.flt_it_market_fare)
                ins_ticket_base.flt_special_fare_inv = float(ins_ticket_base.flt_it_market_fare)

            
            ins_ticket_base.flt_special_fare_inv = ins_ticket_base.flt_market_fare_inv

            ins_ticket_base.bln_k_section_fare = ins_capture_ticket_data.bln_k_section_fare
            flt_vat_rfd = 0.0
            str_issued_date_of_refund_document = ''
            if ins_capture_ticket_data.dct_refund_data and ins_ticket_base.str_ticket_number in ins_capture_ticket_data.dct_refund_data:
                if ins_capture_ticket_data.str_ref_price_indicator:
                    if ins_capture_ticket_data.str_ref_price_indicator =='D' :
                        lst_tickets.append(str_ref_ticket_number)
                        continue

                    if ins_capture_ticket_data.str_ref_price_indicator =='Z' :
                        print ("Issue side not found for refund ticket : ",str_ref_ticket_number)
                        raise

                [str_ticket_number_rfd,
                str_issued_date_of_refund_document,
                str_pax_name_rfd,
                ins_capture_ticket_data.flt_cancellation_fee,
                ins_capture_ticket_data.flt_std_commn_percentage_rfd,
                str_refund_amount,
                ins_capture_ticket_data.flt_tax_refund,
                str_tax,
                flt_vat_rfd,
                ins_capture_ticket_data.str_cc_type,
                ins_capture_ticket_data.str_cc_card_no,
                str_credit_card_expiration_mm_yy_rfd] = ins_capture_ticket_data.dct_refund_data[ins_ticket_base.str_ticket_number]
                
                try:
                    #// included str_cancellation_fee in fare calculation AS said by Niyas sir
                    #// refs #7374
                    ins_capture_ticket_data.flt_fare_refund = float(str_refund_amount) - float(ins_capture_ticket_data.flt_tax_refund) + float(ins_capture_ticket_data.flt_cancellation_fee)
                except:
                    ins_capture_ticket_data.flt_fare_refund = 0.00
                if not ins_capture_ticket_data.dat_ticket_refund: #J-340
                    ins_capture_ticket_data.dat_ticket_refund = str_issued_date_of_refund_document
#                else:
#                    try:
#                        #// In case of A23 section is not present
#                        #// refs #9542
#                        ins_ticket_base.flt_market_fare_rfd = float(str_basic_fare.strip() or 0)
#                    except:
#                        ins_ticket_base.flt_market_fare_rfd = 0.00
#                        pass
#                    pass

            
#            if ins_capture_ticket_data.dct_refund_tickets :
#
#                for str_ref_ticket_number,lst_ref_values in  ins_capture_ticket_data.dct_refund_tickets.items() :
#
#                    if lst_ref_values[0]=='D' :
#                        lst_tickets.append(str_ref_ticket_number)
#                        continue
#
#                    if lst_ref_values[0]=='Z' :
#                        print ("Issue side not found for refund ticket : ",str_ref_ticket_number)
#                        raise
#                    else:
#                        ins_ticket_base.str_pax_name_rfd = lst_ref_values[3]
#                        ins_capture_ticket_data.flt_fare_refund = lst_ref_values[0]
#                        ins_capture_ticket_data.flt_tax_refund = lst_ref_values[1]
#                        ins_capture_ticket_data.flt_cancellation_fee = lst_ref_values[-1]
            
            
            ins_ticket_base.str_refund_date = ins_capture_ticket_data.dat_ticket_refund
            ins_ticket_base.flt_total_tax_rfd = ins_capture_ticket_data.flt_tax_refund

            ins_ticket_base.flt_published_fare_rfd = ins_capture_ticket_data.flt_fare_refund
            ins_ticket_base.flt_market_fare_rfd = ins_capture_ticket_data.flt_fare_refund
            ins_ticket_base.flt_special_fare_rfd = ins_capture_ticket_data.flt_fare_refund or 0.00

            ins_ticket_base.flt_supplier_refund_charge = ins_capture_ticket_data.flt_cancellation_fee
            
            #Code to set the the cc type and  cc number for the corresponding ticket.(Refer #10351)
            flt_mf_card_amount = 0
            flt_tax_card_amount = 0
            if ins_ticket_base.str_ticket_number in ins_capture_ticket_data.dct_cc_card:
                [ins_capture_ticket_data.str_cc_card_no,
                  ins_capture_ticket_data.str_cc_type,
                    flt_mf_card_amount,
                      flt_tax_card_amount] = ins_capture_ticket_data.dct_cc_card[ins_ticket_base.str_ticket_number]

            #42585
            if ins_capture_ticket_data.bln_ma_tax_card_amt and ins_ticket_base.str_ticket_number in ins_capture_ticket_data.dct_ma_tax :
                flt_tax_card_amount += ins_capture_ticket_data.dct_ma_tax[ins_ticket_base.str_ticket_number]
            
            #36789
            if (
                 (ins_ticket_base.flt_total_tax_inv == 0.0 or str_tax == '') or \
                 (ins_ticket_base.str_ticket_number in ins_capture_ticket_data.dct_ma_tax  \
                 and ins_ticket_base.flt_total_tax_inv == ins_capture_ticket_data.dct_ma_tax[ins_ticket_base.str_ticket_number])
                ) and \
                ins_ticket_base.str_ticket_number in ins_capture_ticket_data.dct_mx_tax_detail \
                and ins_capture_ticket_data.dct_mx_tax_detail[ins_ticket_base.str_ticket_number] :
                
                ins_ticket_base.flt_total_tax_inv = ins_capture_ticket_data.dct_mx_tax_detail[ins_ticket_base.str_ticket_number][0]
                str_tax = ins_capture_ticket_data.dct_mx_tax_detail[ins_ticket_base.str_ticket_number][1]
                
                if str_tax and ins_general_methods.ins_auto_inv.str_input_vat_code and ins_general_methods.ins_auto_inv.str_input_vat_code in str_tax:
                    flt_vat = 0.0
                    lst_tx_details = str_tax.split(',')
                    for str_tax_comp in lst_tx_details:
                        if str_tax_comp[:2] == ins_general_methods.ins_auto_inv.str_input_vat_code:
                            flt_vat += float(str_tax_comp.split('=')[1])
                    ins_ticket_base.flt_vat_in_inv = flt_vat
                
            
            #38243, #42196
            if (ins_capture_ticket_data.str_m6_xt_tax_split_adt or ins_capture_ticket_data.str_m6_xt_tax_split_chd or ins_capture_ticket_data.str_m6_xt_tax_split_inf ) \
                        and not ins_ticket_base.str_original_issue \
                        and (str_tax.find('XT') != -1): 

                lst_tax_split = str_tax.split(',')
                lst_tax_split = [ str_tax for str_tax in lst_tax_split if str_tax.find('XT') == -1 ]
                str_tax_split = ','.join(lst_tax_split)
                if ins_ticket_base.str_pax_type == 'ADT':
                    str_tax = str_tax_split  + ins_capture_ticket_data.str_m6_xt_tax_split_adt

                elif ins_ticket_base.str_pax_type == 'CHD':
                    str_tax = str_tax_split + ins_capture_ticket_data.str_m6_xt_tax_split_chd

                elif ins_ticket_base.str_pax_type == 'INF':
                    str_tax = str_tax_split + ins_capture_ticket_data.str_m6_xt_tax_split_inf
                    
                for str_tax_splits in str_tax.split(','): #J-349
                    lst_tax_splits = str_tax_splits.split('=')
                    if lst_tax_splits[0] and lst_tax_splits[1] and ins_general_methods.ins_auto_inv.str_input_vat_code and ins_general_methods.ins_auto_inv.str_input_vat_code == lst_tax_splits[0].strip():
                        flt_vat = float(lst_tax_splits[1])
            try:
                flt_ait_tax_inv = ins_general_methods.get_ait_tax_component_using_admin_settings(ins_ticket_base.flt_market_fare_inv,ins_ticket_base.flt_total_tax_inv,str_tax,ins_capture_ticket_data.str_defult_currency_code)
                flt_ait_tax_rfd = ins_general_methods.get_ait_tax_component_using_admin_settings(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_total_tax_rfd,str_tax,ins_capture_ticket_data.str_defult_currency_code)
                if flt_ait_tax_inv and not ins_capture_ticket_data.bln_refund:
                    ins_ticket_base.flt_total_tax_inv += flt_ait_tax_inv
                    str_tax += ',AIT='+str(flt_ait_tax_inv)
                    #45626
                    if flt_ait_tax_inv and ins_general_methods.ins_auto_inv.str_input_vat_code and ins_general_methods.ins_auto_inv.str_input_vat_code == 'AIT':
                        flt_vat = flt_ait_tax_inv
                if flt_ait_tax_rfd and ins_capture_ticket_data.bln_refund:
                    ins_ticket_base.flt_total_tax_rfd += flt_ait_tax_rfd
                    str_tax += ',AIT='+str(flt_ait_tax_rfd)
                    #45626
                    if flt_ait_tax_rfd and ins_general_methods.ins_auto_inv.str_input_vat_code and ins_general_methods.ins_auto_inv.str_input_vat_code == 'AIT':
                        flt_vat_rfd = flt_ait_tax_rfd
            except:
                pass

            flt_credit_rem_amount = 0.00

            if ins_ticket_base.flt_cc_collected_amount:  ## refer 34791
                int_currency_precision = ins_general_methods.get_rounding_value(ins_ticket_base.str_defult_currency_code)
                if ins_ticket_base.flt_market_fare_inv > ins_ticket_base.flt_cc_collected_amount:
                    ins_ticket_base.flt_market_fare_credit_inv = ins_ticket_base.flt_cc_collected_amount
                    ins_ticket_base.flt_market_fare_cash_inv = round(ins_ticket_base.flt_market_fare_inv - ins_ticket_base.flt_cc_collected_amount,int_currency_precision)
                else:
                    ins_ticket_base.flt_market_fare_cash_inv = 0.0
                    ins_ticket_base.flt_market_fare_credit_inv = ins_ticket_base.flt_market_fare_inv
                    flt_credit_rem_amount = round(ins_ticket_base.flt_cc_collected_amount - ins_ticket_base.flt_market_fare_inv,int_currency_precision)        

                if flt_credit_rem_amount:
                    if ins_ticket_base.flt_total_tax_inv > flt_credit_rem_amount:
                        ins_ticket_base.flt_total_tax_credit_inv = flt_credit_rem_amount
                        ins_ticket_base.flt_total_tax_cash_inv = round(ins_ticket_base.flt_total_tax_inv - flt_credit_rem_amount,int_currency_precision)
                    else:
                        ins_ticket_base.flt_total_tax_cash_inv = 0.0 
                        ins_ticket_base.flt_total_tax_credit_inv = ins_ticket_base.flt_total_tax_inv
                else:    
                    ins_ticket_base.flt_total_tax_credit_inv =  0.0 
                    ins_ticket_base.flt_total_tax_cash_inv = ins_ticket_base.flt_total_tax_inv

            if ins_capture_ticket_data.str_card_approval_code or (ins_capture_ticket_data.str_cc_card_no.strip() and len(ins_capture_ticket_data.str_cc_card_no) > 10 ):
                ins_ticket_base.flt_market_fare_credit_inv = ins_ticket_base.flt_market_fare_inv
                ins_ticket_base.flt_market_fare_cash_inv = 0.0
                ins_ticket_base.flt_total_tax_credit_inv = ins_ticket_base.flt_total_tax_inv
                ins_ticket_base.flt_total_tax_cash_inv = 0.0

                ins_ticket_base.flt_market_fare_cash_rfd = 0.00
                ins_ticket_base.flt_market_fare_credit_rfd = ins_ticket_base.flt_market_fare_rfd
                ins_ticket_base.flt_total_tax_cash_rfd = 0.00
                ins_ticket_base.flt_total_tax_credit_rfd = ins_ticket_base.flt_total_tax_rfd
                ins_ticket_base.flt_supplier_refund_charge_credit = ins_ticket_base.flt_supplier_refund_charge
                ins_ticket_base.flt_supplier_refund_charge_cash = 0.00
                
                if flt_mf_card_amount and flt_mf_card_amount != ins_ticket_base.flt_market_fare_inv : #for sabre
                    ins_ticket_base.flt_market_fare_credit_inv = flt_mf_card_amount
                    ins_ticket_base.flt_market_fare_cash_inv = ins_ticket_base.flt_market_fare_inv-flt_mf_card_amount
                else :
                    ins_ticket_base.flt_market_fare_cash_inv = 0.00
                    ins_ticket_base.flt_market_fare_credit_inv = ins_ticket_base.flt_market_fare_inv

                if flt_tax_card_amount != ins_ticket_base.flt_total_tax_inv : #44825 #  if flt_tax_card_amount and
                    ins_ticket_base.flt_total_tax_credit_inv = flt_tax_card_amount
                    ins_ticket_base.flt_total_tax_cash_inv = ins_ticket_base.flt_total_tax_inv - flt_tax_card_amount

                else :
                    ins_ticket_base.flt_total_tax_cash_inv = 0.00
                    ins_ticket_base.flt_total_tax_credit_inv = ins_ticket_base.flt_total_tax_inv
                    
                pass
            else:
                ins_ticket_base.flt_market_fare_cash_inv = ins_ticket_base.flt_market_fare_inv
                ins_ticket_base.flt_market_fare_credit_inv = 0.0
                ins_ticket_base.flt_total_tax_credit_inv = 0.0
                ins_ticket_base.flt_total_tax_cash_inv = ins_ticket_base.flt_total_tax_inv

                ins_ticket_base.flt_market_fare_cash_rfd = ins_ticket_base.flt_market_fare_rfd
                ins_ticket_base.flt_market_fare_credit_rfd = 0.00
                ins_ticket_base.flt_total_tax_cash_rfd = ins_ticket_base.flt_total_tax_rfd
                ins_ticket_base.flt_total_tax_credit_rfd = 0.00
                ins_ticket_base.flt_supplier_refund_charge_credit = 0.00
                ins_ticket_base.flt_supplier_refund_charge_cash = ins_ticket_base.flt_supplier_refund_charge

            flt_uccf_rem_amount = 0.0
            flt_credit_rem_amount = 0.0
            int_currency_precision = ins_general_methods.get_rounding_value(ins_capture_ticket_data.str_defult_currency_code)
            # if partial uccf is true , that means tyhey have paid the amount by both card and cash.
            if ins_capture_ticket_data.bln_partial_uccf and not ins_capture_ticket_data.bln_refund:
                if ins_capture_ticket_data.flt_uccf_amount :
                    if ins_ticket_base.flt_market_fare_inv > ins_capture_ticket_data.flt_uccf_amount:
                        ins_ticket_base.flt_market_fare_credit_inv = ins_capture_ticket_data.flt_uccf_amount
                        ins_ticket_base.flt_market_fare_cash_inv = round(ins_ticket_base.flt_market_fare_inv - ins_capture_ticket_data.flt_uccf_amount,int_currency_precision)

                    else:
                        ins_ticket_base.flt_market_fare_credit_inv = ins_ticket_base.flt_market_fare_inv
                        ins_ticket_base.flt_market_fare_cash_inv = 0.0
                        flt_uccf_rem_amount = round(ins_capture_ticket_data.flt_uccf_amount - ins_ticket_base.flt_market_fare_inv,int_currency_precision)

                    if flt_uccf_rem_amount:
                        if ins_ticket_base.flt_total_tax_inv > flt_uccf_rem_amount:
                            ins_ticket_base.flt_total_tax_credit_inv = flt_uccf_rem_amount
                            ins_ticket_base.flt_total_tax_cash_inv = round(ins_ticket_base.flt_total_tax_inv - flt_uccf_rem_amount,int_currency_precision)
                        else:
                            ins_ticket_base.flt_total_tax_credit_inv = ins_ticket_base.flt_total_tax_inv
                            ins_ticket_base.flt_total_tax_cash_inv = 0.0
                    else:
                        ins_ticket_base.flt_total_tax_cash_inv = ins_ticket_base.flt_total_tax_inv
                        ins_ticket_base.flt_total_tax_credit_inv = 0.0

                elif ins_capture_ticket_data.flt_credit_amount:
                    if ins_ticket_base.flt_market_fare_inv > ins_capture_ticket_data.flt_credit_amount:
                        ins_ticket_base.flt_market_fare_credit_inv = round(ins_ticket_base.flt_market_fare_inv - ins_capture_ticket_data.flt_credit_amount,int_currency_precision)
                        ins_ticket_base.flt_market_fare_cash_inv = ins_capture_ticket_data.flt_credit_amount
                    else:
                        ins_ticket_base.flt_market_fare_cash_inv = ins_ticket_base.flt_market_fare_inv
                        ins_ticket_base.flt_market_fare_credit_inv = 0
                        flt_credit_rem_amount = round(ins_capture_ticket_data.flt_credit_amount - ins_ticket_base.flt_market_fare_inv,int_currency_precision)
                    if flt_credit_rem_amount:
                        if ins_ticket_base.flt_total_tax_inv > flt_credit_rem_amount:
                            ins_ticket_base.flt_total_tax_cash_inv = flt_credit_rem_amount
                            ins_ticket_base.flt_total_tax_credit_inv = round(ins_ticket_base.flt_total_tax_inv - flt_credit_rem_amount,int_currency_precision)
                        else:
                            ins_ticket_base.flt_total_tax_cash_inv = ins_ticket_base.flt_total_tax_inv
                            ins_ticket_base.flt_total_tax_credit_inv = 0.0
                    else:    
                        ins_ticket_base. flt_total_tax_credit_inv = ins_ticket_base.flt_total_tax_inv
                        ins_ticket_base.flt_total_tax_cash_inv = 0.0

            flt_uccf_rem_amount = 0.0
            flt_credit_rem_amount = 0.0
            # if partial uccf is true , that means tyhey have paid the amount by both card and cash.
            if ins_capture_ticket_data.bln_partial_uccf and ins_capture_ticket_data.bln_refund :
                if ins_capture_ticket_data.flt_uccf_amount:
                    if ins_ticket_base.flt_market_fare_rfd > ins_capture_ticket_data.flt_uccf_amount:
                        ins_ticket_base.flt_market_fare_credit_rfd = ins_capture_ticket_data.flt_uccf_amount
                        ins_ticket_base.flt_market_fare_cash_rfd = round(ins_ticket_base.flt_market_fare_rfd - ins_capture_ticket_data.flt_uccf_amount,int_currency_precision)

                    else:
                        ins_ticket_base.flt_market_fare_credit_rfd = ins_ticket_base.flt_market_fare_rfd
                        ins_ticket_base.flt_market_fare_cash_rfd = 0.0
                        flt_uccf_rem_amount = round(ins_capture_ticket_data.flt_uccf_amount - ins_ticket_base.flt_market_fare_rfd,int_currency_precision)

                    if flt_uccf_rem_amount:
                        if ins_ticket_base.flt_total_tax_rfd > flt_uccf_rem_amount:
                            ins_ticket_base.flt_total_tax_credit_rfd = flt_uccf_rem_amount
                            ins_ticket_base.flt_total_tax_cash_rfd = round(ins_ticket_base.flt_total_tax_rfd - flt_uccf_rem_amount,int_currency_precision)
                        else:
                            ins_ticket_base.flt_total_tax_credit_rfd = ins_ticket_base.flt_total_tax_rfd
                            ins_ticket_base.flt_total_tax_cash_rfd = 0.0
                            flt_uccf_rem_amount = round(flt_uccf_rem_amount - ins_ticket_base.flt_total_tax_rfd,int_currency_precision)

                    else :
                        ins_ticket_base.flt_total_tax_cash_rfd = ins_ticket_base.flt_total_tax_rfd
                        ins_ticket_base.flt_total_tax_credit_rfd = 0.0

                    if flt_uccf_rem_amount:
                        if ins_ticket_base.flt_supplier_refund_charge > flt_uccf_rem_amount:
                            ins_ticket_base.flt_supplier_refund_charge_credit = flt_uccf_rem_amount
                            ins_ticket_base.flt_supplier_refund_charge_cash = round(ins_ticket_base.flt_supplier_refund_charge - flt_uccf_rem_amount,2)
                        else:
                            ins_ticket_base.flt_supplier_refund_charge_credit = ins_ticket_base.flt_supplier_refund_charge
                            ins_ticket_base.flt_supplier_refund_charge_cash = 0.0
                            flt_uccf_rem_amount = round(flt_uccf_rem_amount - ins_ticket_base.flt_supplier_refund_charge,2)

                    else:
                        ins_ticket_base.flt_supplier_refund_charge_cash = ins_ticket_base.flt_supplier_refund_charge
                        ins_ticket_base.flt_supplier_refund_charge_credit = 0.0

                elif ins_capture_ticket_data.flt_credit_amount:        
                    if ins_ticket_base.flt_market_fare_rfd > ins_capture_ticket_data.flt_credit_amount:
                        ins_ticket_base.flt_market_fare_credit_rfd = round(ins_ticket_base.flt_market_fare_rfd - ins_capture_ticket_data.flt_credit_amount,int_currency_precision)
                        ins_ticket_base.flt_market_fare_cash_rfd = ins_capture_ticket_data.flt_credit_amount

                    else:
                        ins_ticket_base.flt_market_fare_credit_rfd = 0
                        ins_ticket_base.flt_market_fare_cash_rfd = ins_ticket_base.flt_market_fare_rfd
                        flt_credit_rem_amount = round(ins_capture_ticket_data.flt_credit_amount - ins_ticket_base.flt_market_fare_rfd,int_currency_precision)

                    if flt_credit_rem_amount:
                        if ins_ticket_base.flt_total_tax_rfd > flt_credit_rem_amount:
                            ins_ticket_base.flt_total_tax_credit_rfd = round(ins_ticket_base.flt_total_tax_rfd - flt_credit_rem_amount,int_currency_precision)
                            ins_ticket_base.flt_total_tax_cash_rfd = flt_credit_rem_amount
                        else:
                            ins_ticket_base.flt_total_tax_cash_rfd = ins_ticket_base.flt_total_tax_rfd
                            ins_ticket_base.flt_total_tax_credit_rfd = 0.0
                            flt_credit_rem_amount = round(flt_credit_rem_amount - ins_ticket_base.flt_total_tax_rfd,int_currency_precision)    
                    else:
                        ins_ticket_base.flt_total_tax_credit_rfd = ins_ticket_base.flt_total_tax_rfd
                        ins_ticket_base.flt_total_tax_cash_rfd  = 0.0

                    if flt_credit_rem_amount:
                        if ins_ticket_base.flt_supplier_refund_charge > flt_credit_rem_amount:
                            ins_ticket_base.flt_supplier_refund_charge_cash = flt_credit_rem_amount
                            ins_ticket_base.flt_supplier_refund_charge_credit = round(ins_ticket_base.flt_supplier_refund_charge - flt_credit_rem_amount,int_currency_precision)
                        else:
                            ins_ticket_base.flt_supplier_refund_charge_cash= ins_ticket_base.flt_supplier_refund_charge
                            ins_ticket_base.flt_supplier_refund_charge_credit = 0.0
                            flt_credit_rem_amount = round(flt_credit_rem_amount - ins_ticket_base.flt_supplier_refund_charge,int_currency_precision)

                    else:
                        ins_ticket_base.flt_supplier_refund_charge_cash = 0.0
                        ins_ticket_base.flt_supplier_refund_charge_credit = ins_ticket_base.flt_supplier_refund_charge     



            ins_ticket_base.bln_partial_uccf = ins_capture_ticket_data.bln_partial_uccf
            ins_ticket_base.flt_uccf_amount = ins_capture_ticket_data.flt_uccf_amount
            
            ins_ticket_base.str_tax_details = str_tax
            if ins_capture_ticket_data.bln_refund or ins_capture_ticket_data.str_void_date:
                ins_ticket_base.flt_vat_in_rfd = flt_vat_rfd or flt_vat
                ins_ticket_base.str_tax_details_rfd = str_tax #42728
#                ins_ticket_base.str_tax_details = ''
                ins_ticket_base.int_distribution_type_rfd = ins_capture_ticket_data.int_distribution_type #46542
            else:
                ins_ticket_base.flt_vat_in_inv = flt_vat
                ins_ticket_base.str_tax_details_rfd = ''
                ins_ticket_base.int_distribution_type_inv = ins_capture_ticket_data.int_distribution_type #46542
                
            ins_ticket_base.flt_selling_price = 0.00
            ins_ticket_base.flt_service_charge = flt_service_charge
            ins_ticket_base.flt_supplier_amount = 0.00
            ins_ticket_base.flt_std_commn_percentage_inv = ins_capture_ticket_data.flt_std_commn_percentage_inv or flt_commision_percetage
            ins_ticket_base.flt_standard_commission_captured = ins_capture_ticket_data.flt_standard_commission_captured or flt_commision_amount #ref 19763 

            ins_ticket_base.flt_discount_given_inv = ins_capture_ticket_data.flt_rm_discount
            ins_ticket_base.flt_fare_differece = 0.00
            ins_ticket_base.flt_adm_expect = 0.00



            ins_ticket_base.flt_std_commn_percentage_rfd = ins_capture_ticket_data.flt_std_commn_percentage_rfd
            ins_ticket_base.flt_discount_given_rfd = 0.00

            ins_ticket_base.flt_client_refund_charge = ins_capture_ticket_data.flt_cancellation_fee
            ins_ticket_base.flt_supplier_refund_net = 0.00
            ins_ticket_base.flt_client_refund_net = 0.00
            ins_ticket_base.str_inv_document_number = ''
            ins_ticket_base.str_rfd_document_number = ''
            ins_ticket_base.bln_bsp_reconciled_issue = False
            ins_ticket_base.bln_dc_reconciled_issue = False
            ins_ticket_base.flt_bsp_difference_amt_issue = 0.00
            ins_ticket_base.flt_dc_difference_amt_issue = 0.00
            ins_ticket_base.flt_supplier_amt_as_per_bsp_issue = 0.00
            ins_ticket_base.flt_supplier_amt_as_per_dc_issue = 0.00
            ins_ticket_base.str_fare_construction = ins_capture_ticket_data.str_fare_construction
            ins_ticket_base.str_ticket_designator = ins_capture_ticket_data.str_ticket_designator
            ins_ticket_base.bln_bsp_reconciled_refund = False
            ins_ticket_base.bln_dc_reconciled_refund = False
            ins_ticket_base.flt_bsp_difference_amt_refund = 0.00
            ins_ticket_base.flt_dc_difference_amt_refund  = 0.00
            ins_ticket_base.flt_supplier_amt_as_per_bsp_refund  = 0.00
            ins_ticket_base.flt_supplier_amt_as_per_dc_refund  = 0.00
            ins_ticket_base.str_defult_currency_code = ins_capture_ticket_data.str_defult_currency_code
            ins_ticket_base.flt_supplier_currency_roe = 1
            ins_ticket_base.flt_supplier_currency_roe_rfd = 1

            #41852
            if ins_capture_ticket_data.flt_co2_emission:
                ins_ticket_base.str_co2_emission = str(ins_capture_ticket_data.flt_co2_emission) + 'KG'

            lst_sector_details = ins_capture_ticket_data.lst_sector_details
            if not ins_capture_ticket_data.lst_sector_details: # refer 25579
                lst_sector_details = ins_capture_ticket_data.lst_new_u_section_sector

            flt_amount = float(ins_ticket_base.flt_market_fare_inv) + float(ins_ticket_base.flt_total_tax_inv)
            try :
                flt_sector_amount = round(flt_amount/ins_capture_ticket_data.int_number_of_segments,2)
            except :
                flt_sector_amount = 0.0
            try :
                flt_rounding_amount = flt_amount - (flt_sector_amount *ins_capture_ticket_data.int_number_of_segments)
            except :
                flt_rounding_amount = 0.0
                
            lst_sector = []
            str_class_name = ''
            for lst_sector in lst_sector_details:
                # refer 34008
                if ins_general_methods.str_consider_higher_travel_class and ins_general_methods.str_consider_higher_travel_class.upper()  == 'TRUE' and str_class_name != 'First Class':
                    int_class_id,str_class_name = ins_general_methods.get_class_id('%'+lst_sector[6]+'%',ins_ticket_base.int_airline_account_id)
                    if str_class_name == 'First Class':
                        ins_ticket_base.str_class = lst_sector[6].strip()
                    elif str_class_name == 'Business Class':
                        ins_ticket_base.str_class = lst_sector[6].strip()
                    else:
                        pass
                lst_sector[11]= flt_sector_amount
            if lst_sector :
                lst_sector[11] = flt_sector_amount + flt_rounding_amount

            ins_ticket_base.lst_sector_details = lst_sector_details
            ins_ticket_base.dct_airport = ins_capture_ticket_data.dct_airport

            ins_ticket_base.str_booking_agency_iata_no = ins_capture_ticket_data.str_booking_agency_iata_no
            ins_ticket_base.str_ticketing_agency_iata_no = ins_capture_ticket_data.str_ticketing_agency_iata_no
            ins_ticket_base.str_pnr_current_owner_iata_no = ins_capture_ticket_data.str_pnr_current_owner_iata_no
            ins_ticket_base.str_booking_agency_office_id = ins_capture_ticket_data.str_booking_agency_office_id
            ins_ticket_base.str_pnr_first_owner_office_id = ins_capture_ticket_data.str_pnr_first_owner_office_id
            ins_ticket_base.str_pnr_current_owner_office_id = ins_capture_ticket_data.str_pnr_current_owner_office_id
            ins_ticket_base.str_ticketing_agency_office_id = ins_capture_ticket_data.str_ticketing_agency_office_id
            ins_ticket_base.str_airline_pnr_number = ins_capture_ticket_data.str_airline_pnr_number    
            #// Refer #9517
            ins_ticket_base.str_branch_code = ins_capture_ticket_data.str_branch_code
            ins_ticket_base.int_branch_id = ins_general_methods.verify_department_details(ins_capture_ticket_data.str_branch_code)
#		"""record = []
#		record = self.ins_capture_db.getCompanyDetails()
#		ins_ticket_base.str_company_name = record[0]
#		ins_ticket_base.str_group = record[1]
#		ins_ticket_base.str_address = record[2]
#		ins_ticket_base.str_email = record[3]
#		ins_ticket_base.str_phone = record[4]
#		ins_ticket_base.str_fax = record[5]"""

            ins_ticket_base.flt_total_amount_collected =  ins_capture_ticket_data.flt_total_amount_collected
            try:
                if ins_capture_ticket_data.lst_stop_over_airports:
                    ins_ticket_base.str_stop_over_airports = ins_capture_ticket_data.lst_stop_over_airports[0]
            except:
                pass

            ins_ticket_base.str_void_date = ins_capture_ticket_data.str_void_date
            ins_ticket_base.bln_refund = ins_capture_ticket_data.bln_refund

            ins_ticket_base.str_file_name = str_file
            str_file_name = os.path.split(str_file)[-1]
            if ins_capture_ticket_data.bln_refund or ins_capture_ticket_data.str_void_date:
                ins_ticket_base.str_file_name_rfd = str_file_name
            else:
                ins_ticket_base.str_file_name_inv = str_file_name
            try:
                if ins_capture_ticket_data.str_first_departure_date:
                    ins_ticket_base.str_travel_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_first_departure_date,ins_ticket_base.str_ticket_issue_date)
                elif ins_capture_ticket_data.lst_sector_details and ins_capture_ticket_data.str_first_travel_date :
                    if datetime.datetime.strptime(ins_capture_ticket_data.lst_sector_details[0][8], "%d/%m/%Y") > datetime.datetime.strptime(ins_capture_ticket_data.str_first_travel_date, "%d/%m/%Y"):
                        ins_ticket_base.str_travel_date = ins_capture_ticket_data.lst_sector_details[0][8] 
                    else :
                        ins_ticket_base.str_travel_date = ins_capture_ticket_data.str_first_travel_date or None
            except:
                ins_ticket_base.str_travel_date = None
            # // Customer code, LPO No and Service Charge from Remark section
            ins_ticket_base.str_customer_code = ins_capture_ticket_data.str_rm_customer_code.split(';')[0].strip()
            ins_ticket_base.str_lpo_number = ins_capture_ticket_data.str_rm_lpo_number

            ins_ticket_base.flt_coll_amount = ins_capture_ticket_data.flt_rm_collection_amount

#                ins_ticket_base.int_agent_code  = ins_capture_ticket_data.int_rm_agent_code
            ins_ticket_base.str_cost_centre = ins_capture_ticket_data.str_rm_cost_centre
            if ins_ticket_base.str_cost_centre :
                ins_ticket_base.int_location_id,ins_ticket_base.int_company_id = ins_general_methods.verify_location_details(ins_ticket_base.str_cost_centre)


            ins_ticket_base.str_job_code = ins_capture_ticket_data.str_rm_job_code
            ins_ticket_base.str_employee_number = ins_capture_ticket_data.str_rm_employee_number
            ins_ticket_base.str_travel_reg_no = ins_capture_ticket_data.str_rm_travel_reg_no
            ins_ticket_base.str_card_approval_code = ins_capture_ticket_data.str_card_approval_code or str_card_approval_code
            ins_ticket_base.str_cc_card_no = ins_capture_ticket_data.str_cc_card_no or ''
            ins_ticket_base.str_rm_sub_type = ins_capture_ticket_data.str_rm_sub_type
            ins_ticket_base.str_rm_staff = ins_capture_ticket_data.str_rm_staff
            ins_ticket_base.str_phone = ins_capture_ticket_data.str_phone
            ins_ticket_base.str_purpose = ins_capture_ticket_data.str_purpose
            ins_ticket_base.str_reason = ins_capture_ticket_data.str_reason
            ins_ticket_base.str_lost = ins_capture_ticket_data.str_lost
            ins_ticket_base.str_employee_email = ins_capture_ticket_data.str_rm_emp_email
            ins_ticket_base.int_corporate_card_id_rfd = None
            #UCCF set false by default .If the dc card number in master match with captured number then UCCF is set as false else true
            ins_ticket_base.bln_uccf = False
            if ins_ticket_base.str_cc_card_no:
                (int_account_master_id,int_account_type) = ins_general_methods.get_corporate_card_id(ins_ticket_base.str_cc_card_no,ins_ticket_base.str_crs_company)
                if int_account_master_id:
                    if ins_ticket_base.bln_refund:
                        ins_ticket_base.int_corporate_card_id_rfd = int_account_master_id
                    else:
                        ins_ticket_base.int_dc_card = int_account_master_id
                    if int_account_type == 1 : #// bln customer card true
                        ins_ticket_base.int_card_payment_type = 3

                        ins_ticket_base.flt_vat_in_rfd = 0.00
                        ins_ticket_base.flt_vat_in_inv = 0.00
                    else :
                        ins_ticket_base.int_card_payment_type = 2
                        pass

#                elif ins_capture_ticket_data.str_cc_type.upper() == 'TP' : ##Refer #16207
#                    ins_ticket_base.int_dc_card = ins_general_methods.int_airplus_card_id
#                    if ins_ticket_base.int_dc_card  :
#                        ins_ticket_base.int_card_payment_type = 3
#                        ins_ticket_base.flt_vat_in_rfd = 0.00
#                        ins_ticket_base.flt_vat_in_inv = 0.00



                if not int_account_master_id: #39332
                    ins_ticket_base.bln_uccf = True
                    ins_ticket_base.int_card_payment_type = 1
                    ins_ticket_base.flt_vat_in_rfd = 0.00
                    ins_ticket_base.flt_vat_in_inv = 0.00

            if ins_capture_ticket_data.str_cc_type in ('AX','$$'):
                    ins_ticket_base.bln_uccf = False

            if ins_capture_ticket_data.bln_refund:
                ins_ticket_base.str_cc_type_rfd = ins_capture_ticket_data.str_cc_type
            else:
                ins_ticket_base.str_cc_type_inv = ins_capture_ticket_data.str_cc_type
#                    if ins_capture_ticket_data.str_cc_type =='AX':
#                        int_account_master_id = self.ins_capture_db.get_amex_account_id()
#                        if int_account_master_id:
#                            ins_ticket_base.int_dc_card = int_account_master_id

            ins_ticket_base.lst_card_data = ins_capture_ticket_data.lst_card_data

            # // Customer code, LPO No and Discount Given in FP section
            if str_ticket_number in ins_capture_ticket_data.dct_fp_data:
                (str_customer_code,
                    str_lpo_number,
                    flt_discount_given,
                    flt_discount_given_per,
                    flt_service_fee,
                    flt_service_fee_per) = ins_capture_ticket_data.dct_fp_data[str_ticket_number]

                if not ins_ticket_base.str_customer_code:
                    ins_ticket_base.str_customer_code = str_customer_code.split(';')[0].strip()
                if not ins_ticket_base.str_lpo_number:
                    ins_ticket_base.str_lpo_number = str_lpo_number
                if not ins_ticket_base.flt_service_charge:
                    if flt_discount_given or ins_ticket_base.flt_service_charge < 0:
                        ins_ticket_base.flt_service_charge = flt_discount_given * -1
                    elif flt_discount_given_per or ins_ticket_base.flt_service_charge < 0:
                        ins_ticket_base.flt_service_charge = (float(ins_ticket_base.flt_market_fare_inv) * flt_discount_given_per / 100) * -1
                    elif flt_service_fee:
                        ins_ticket_base.flt_service_charge = flt_service_fee
                    elif flt_service_fee_per:
                        ins_ticket_base.flt_service_charge = float(ins_ticket_base.flt_market_fare_inv) * flt_service_fee_per / 100
                        pass
                pass

            #customer code from counter staff  #39635
            if ins_general_methods.ins_auto_inv.bln_capture_use_client_set_in_tick_agent and not ins_ticket_base.str_customer_code :
                ins_ticket_base.str_customer_code = str_mapping_client_account
            elif ins_general_methods.ins_auto_inv.bln_capture_use_client_set_in_book_agent and not ins_ticket_base.str_customer_code :
                ins_ticket_base.str_customer_code = str_book_mapping_client_account

            #refer 14819

            str_book_off_temp = ins_ticket_base.str_crs_company+'BOOKING'+ins_ticket_base.str_booking_agency_office_id
#                str_book_off_temp = 'AMADEUS'+'BOOKING'+ins_ticket_base.str_booking_agency_office_id
            str_tick_off_temp = ins_ticket_base.str_crs_company+'ISSUING'+ins_ticket_base.str_ticketing_agency_office_id
#                str_tick_off_temp = 'AMADEUS'+'ISSUING'+ins_ticket_base.str_ticketing_agency_office_id

            ins_auto = None
            if ins_ticket_base.str_booking_agency_office_id and str_book_off_temp in ins_general_methods.ins_auto_inv.dct_office_id_data:
                ins_auto = ins_general_methods.ins_auto_inv.dct_office_id_data[str_book_off_temp]
            elif ins_ticket_base.str_ticketing_agency_office_id and str_tick_off_temp in ins_general_methods.ins_auto_inv.dct_office_id_data:
                ins_auto = ins_general_methods.ins_auto_inv.dct_office_id_data[str_tick_off_temp]

            if ins_auto :  
                if not ins_ticket_base.str_original_issue and not ins_auto.bln_capture_service_fee:
                    ins_capture_ticket_data.flt_rm_service_charge = 0.00
                    ins_ticket_base.flt_service_charge = 0.00
                    ins_general_methods.ins_global.dct_pax_no_service_fee['ALL'] = 0.0
                if ins_auto.bln_auto_invoice and ins_capture_ticket_data.str_agency_auto_invoice_yes_no.upper() not in ('NO','FALSE'):
                    ins_capture_ticket_data.str_agency_auto_invoice_yes_no = 'YES'
                ins_ticket_base.str_customer_code = ins_auto.str_customer_code

                str_mapping_client_account = ''

                if ins_auto.int_cost_center_id :    
                    ins_ticket_base.int_location_id = ins_auto.int_cost_center_id
                    ins_ticket_base.int_company_id = ins_auto.int_company_id

                if ins_auto.int_department_id :    
                    ins_ticket_base.int_branch_id = ins_auto.int_department_id

                if ins_auto.int_counter_staff_id :    
                    ins_ticket_base.int_counter_staff_id_inv = ins_auto.int_counter_staff_id
                    ins_ticket_base.int_booking_agent_counter_staff_id = ins_ticket_base.int_counter_staff_id_inv

                    ins_ticket_base.str_booking_agent_code = ins_auto.str_amad_code
                    ins_ticket_base.str_ticketing_agent_code = ins_auto.str_amad_code

                    ins_ticket_base.str_booking_agent_numeric_code = ins_auto.str_amad_num_code
                    ins_ticket_base.str_ticketing_agent_numeric_code = ins_auto.str_amad_num_code


            ins_ticket_base.str_sub_customer_code = ins_capture_ticket_data.str_sub_customer_code.split(';')[0].strip()

            int_account_profile_id = None
            #refer 21409
            if ins_general_methods.ins_auto_inv.bln_capture_client_set_in_tick_agent \
                or ins_general_methods.ins_auto_inv.bln_capture_client_set_in_book_agent:#//refer 23491

                if str_mapping_client_account :
                    if ins_capture_ticket_data.str_agency_auto_invoice_yes_no.upper() not in ('NO','FALSE'):
                        ins_capture_ticket_data.str_agency_auto_invoice_yes_no = 'YES'
                    ins_ticket_base.str_customer_code = str_mapping_client_account
                    if int_mapping_client_department_id:
                        ins_ticket_base.int_branch_id = int_mapping_client_department_id
                    if int_mapping_client_location_id:
                        ins_ticket_base.int_location_id = int_mapping_client_location_id

            if int_counter_staff_cost_center and not ins_ticket_base.int_location_id:
                ins_ticket_base.int_location_id = int_counter_staff_cost_center

            if int_counter_staff_department and not ins_ticket_base.int_branch_id :
                ins_ticket_base.int_branch_id = int_counter_staff_department

            if str_cust_cash_acc_id_tick and ins_capture_ticket_data.str_fop_data == 'CASH' :
                ins_ticket_base.str_customer_code = str_cust_cash_acc_id_tick

            # // Get Customer data
            (int_account_master_id,
                chr_account_type,
                str_account_code_inv,
                str_account_name_inv,
                int_location_id,
                int_branch_id,
                int_auto_invoice,
                str_auto_inv_grouping
                ) = ins_general_methods.get_account_data(ins_ticket_base.str_customer_code,'C')


#            if ins_ticket_base.str_sub_customer_code :
#                int_account_master_id_using_sub_cust_code,int_account_profile_id = ins_general_methods.get_customer_id_using_profile_code(ins_ticket_base.str_sub_customer_code)
#
#                if int_account_master_id_using_sub_cust_code and not int_account_master_id :
#                    int_account_master_id = int_account_master_id_using_sub_cust_code
#
#            elif not ins_ticket_base.str_sub_customer_code and ins_ticket_base.str_customer_code and not int_account_master_id:
#                int_account_master_id_using_sub_cust_code,int_account_profile_id = ins_general_methods.get_customer_id_using_profile_code(ins_ticket_base.str_customer_code)
#
#                if int_account_master_id_using_sub_cust_code :
#                    int_account_master_id = int_account_master_id_using_sub_cust_code
#                if not int_account_master_id_using_sub_cust_code:
#                    int_account_profile_id = None

#                refer #13997
            if not int_account_master_id and ins_ticket_base.str_cc_card_no:
                int_account_master_id = ins_general_methods.get_customer_id_using_card_no(ins_ticket_base.str_cc_card_no,ins_ticket_base.str_crs_company) #39593
            if not int_account_master_id and ins_capture_ticket_data.int_credit_card_pos_id and ins_capture_ticket_data.str_cc_number: # refer 20879


                (int_account_master_id,
                    chr_account_type,
                    str_account_code_inv,
                    str_account_name_inv,
                    int_location_id,
                    int_branch_id,
                    int_auto_invoice,
                    str_auto_inv_grouping) = ins_general_methods.get_account_data(ins_ticket_base.str_customer_code,'CC')

            if not  int_account_master_id :
                (int_account_master_id,
                    chr_account_type,
                    str_account_code_inv,
                    str_account_name_inv,
                    int_location_id,
                    int_branch_id,
                    int_auto_invoice,
                    str_auto_inv_grouping) = ins_general_methods.get_account_data(ins_ticket_base.str_customer_code,'CASH')

                #refer #42110 - To get details of multi-currency-customer using customer code and currency code
                if not int_account_master_id:
                    (int_account_master_id,
                        chr_account_type,
                        str_account_code_inv,
                        str_account_name_inv,
                        int_location_id,
                        int_branch_id,
                        int_auto_invoice,
                        str_auto_inv_grouping) = ins_general_methods.get_account_data(ins_ticket_base.str_customer_code+ins_ticket_base.str_defult_currency_code)
                
            ins_ticket_base.int_account_master_id = int_account_master_id
            ins_ticket_base.int_profile_id = int_account_profile_id
            ins_ticket_base.chr_account_type = chr_account_type
            ins_ticket_base.str_account_code_inv = str_account_code_inv
            ins_ticket_base.str_account_name_inv = str_account_name_inv
            ins_ticket_base.str_auto_inv_grouping = str_auto_inv_grouping
            ins_capture_ticket_data.str_auto_inv_grouping = str_auto_inv_grouping

            if int_branch_id and not ins_ticket_base.int_branch_id :
                ins_ticket_base.int_branch_id = int_branch_id
            if int_location_id and not ins_ticket_base.int_location_id:
                ins_ticket_base.int_location_id = int_location_id
            if not ins_ticket_base.int_location_id :
                ins_ticket_base.int_location_id = ins_general_methods.int_min_location_id
            
            if not ins_ticket_base.int_branch_id :
                ins_ticket_base.int_branch_id = ins_general_methods.int_min_department_id
                
            if int_auto_invoice and ins_capture_ticket_data.str_agency_auto_invoice_yes_no.upper() not in ('NO','FALSE') :
                ins_capture_ticket_data.str_agency_auto_invoice_yes_no = 'YES'
                ins_ticket_base.str_agency_auto_invoice_yes_no = 'YES'

            if ins_ticket_base.flt_service_charge < 0:
                ins_ticket_base.flt_service_charge = 0

            if 'ALL' in ins_general_methods.ins_global.dct_pax_no_service_fee and ins_capture_ticket_data.flt_rm_service_charge :
                ins_ticket_base.flt_service_charge =  ins_capture_ticket_data.flt_rm_service_charge

            elif ins_general_methods.ins_global.dct_pax_no_service_fee and 'ALL' not in ins_general_methods.ins_global.dct_pax_no_service_fee :
                ins_ticket_base.flt_service_charge = ins_general_methods.ins_global.dct_pax_no_service_fee.get(ins_capture_ticket_data.int_tst_count,0)

            elif ins_capture_ticket_data.flt_rm_service_charge:
                ins_ticket_base.flt_service_charge =  ins_capture_ticket_data.flt_rm_service_charge
            # refer 30961
            if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_service_fee_sector_wise:
                ins_ticket_base.flt_service_charge = ins_general_methods.ins_global.dct_service_fee_sector_wise[ins_ticket_base.str_sector[:3]]

            if ins_ticket_base.str_pax_type == 'CHD' and ins_capture_ticket_data.bln_chd_svf :
                ins_ticket_base.flt_service_charge = ins_capture_ticket_data.flt_service_fee_child
                if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_child_service_fee_sector_wise:
                    ins_ticket_base.flt_service_charge = ins_general_methods.ins_global.dct_child_service_fee_sector_wise[ins_ticket_base.str_sector[:3]]

            elif ins_ticket_base.str_pax_type == 'INF' and ins_capture_ticket_data.bln_inf_svf:
                ins_ticket_base.flt_service_charge = ins_capture_ticket_data.flt_service_fee_infant
                if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_infant_service_fee_sector_wise:
                    ins_ticket_base.flt_service_charge = ins_general_methods.ins_global.dct_infant_service_fee_sector_wise[ins_ticket_base.str_sector[:3]]

            ins_ticket_base.flt_service_charge = ins_ticket_base.flt_service_charge + ins_capture_ticket_data.flt_additional_service_charge

            # Refer 14759
            ins_ticket_base.flt_deal_fare_amt = (float(ins_ticket_base.flt_published_fare_inv) - float(ins_ticket_base.flt_market_fare_inv))


#                if ins_capture_ticket_data.flt_rm_discount < 0 :    
            ins_ticket_base.flt_discount_given_inv = abs(ins_capture_ticket_data.flt_rm_discount)
#                else :
#                    ins_ticket_base.flt_extra_earning_inv += ins_capture_ticket_data.flt_rm_discount



            if ins_ticket_base.str_pax_type == 'CHD' and ins_capture_ticket_data.bln_rm_discount_chd :
                ins_ticket_base.flt_discount_given_inv = abs(ins_capture_ticket_data.flt_rm_discount_chd)
            elif ins_ticket_base.str_pax_type == 'INF' and ins_capture_ticket_data.bln_rm_discount_inf:
                ins_ticket_base.flt_discount_given_inv = abs(ins_capture_ticket_data.flt_rm_discount_inf)
                
            #45196
            ins_ticket_base.flt_rm_normal_discount = ins_ticket_base.flt_discount_given_inv
            ins_ticket_base.flt_rm_plb_discount = ins_capture_ticket_data.flt_rm_plb_discount
            ins_ticket_base.flt_rm_deal_discount = ins_capture_ticket_data.flt_rm_deal_discount
            
            if ins_ticket_base.flt_rm_plb_discount or ins_ticket_base.flt_rm_deal_discount :
                ins_ticket_base.flt_discount_given_inv += ins_ticket_base.flt_rm_plb_discount + ins_ticket_base.flt_rm_deal_discount

            ins_ticket_base.flt_extra_earning_inv = ins_capture_ticket_data.flt_extra_earning_inv

            # refer 34029 extra earnin for child inf sector wise
            if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_extra_earning_sector_wise:
                ins_ticket_base.flt_extra_earning_inv = ins_general_methods.ins_global.dct_extra_earning_sector_wise[ins_ticket_base.str_sector[:3]]

            if ins_ticket_base.str_pax_type == 'CHD' and ins_capture_ticket_data.bln_extra_earning_chd :
                ins_ticket_base.flt_extra_earning_inv = ins_capture_ticket_data.flt_extra_earning_chd

                if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_child_extra_earning_sector_wise:
                    ins_ticket_base.flt_extra_earning_inv = ins_general_methods.ins_global.dct_child_extra_earning_sector_wise[ins_ticket_base.str_sector[:3]]

            elif ins_ticket_base.str_pax_type == 'INF' and ins_capture_ticket_data.bln_extra_earning_inf:
                ins_ticket_base.flt_extra_earning_inv = ins_capture_ticket_data.flt_extra_earning_inf

                if ins_ticket_base.str_sector[:3] in ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise:
                    ins_ticket_base.flt_extra_earning_inv = ins_general_methods.ins_global.dct_infant_extra_earning_sector_wise[ins_ticket_base.str_sector[:3]]

            if ins_ticket_base.flt_deal_fare_amt and not ins_ticket_base.str_original_issue:
                if ins_capture_ticket_data.str_deal_sharing_string :
                    try :
                        flt_extra_earning_percentage = float(ins_capture_ticket_data.str_deal_sharing_string.split('/')[0].strip())
#                            flt_discount_percentage = float(ins_capture_ticket_data.str_deal_sharing_string.split('/')[1].strip())
                        flt_extra_earning_amount = ins_ticket_base.flt_deal_fare_amt * flt_extra_earning_percentage/100
                        flt_discount_amount = ins_ticket_base.flt_deal_fare_amt - flt_extra_earning_amount
                        pass
                    except :
                        flt_discount_amount = 0
                        flt_extra_earning_amount = ins_ticket_base.flt_deal_fare_amt

                    ins_ticket_base.flt_discount_given_inv += flt_discount_amount
                    ins_ticket_base.flt_extra_earning_inv += flt_extra_earning_amount





#                if ins_ticket_base.flt_deal_fare_amt:
#                    ins_ticket_base.bln_deal_fare = True
            ins_ticket_base.flt_additional_service_charge = ins_capture_ticket_data.flt_additional_service_charge

            if ins_capture_ticket_data.str_return_date:
                ins_ticket_base.str_return_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_return_date, ins_ticket_base.str_ticket_issue_date)
            ins_ticket_base.str_auto_invoice_created_by=ins_capture_ticket_data.str_auto_invoice_created_by
            ins_ticket_base.str_auto_invoice_location=ins_capture_ticket_data.str_auto_invoice_location
            # // RM option data
#                if ins_capture_ticket_data.str_amadeus_c3x_opt_one or ins_capture_ticket_data.str_amadeus_c3x_opt_two:
#                    int_branch_id = None
#                    if ins_general_methods.get_admin_settings('AMADEUSC3XOPT1') == 'Cost Centre Code':
#                        str_location = ins_capture_ticket_data.str_amadeus_c3x_opt_one
#                        str_salesman_code = ins_capture_ticket_data.str_amadeus_c3x_opt_two
#                    else:
#                        str_location = ins_capture_ticket_data.str_amadeus_c3x_opt_two
#                        str_salesman_code = ins_capture_ticket_data.str_amadeus_c3x_opt_one
#                        pass
#
#                    str_branch_code = str_location
#                    if str_location and not ins_general_methods.verify_location_details(str_location)[0]:
#                        str_location = ""
#                        pass
#
#                    if str_branch_code:
#                        int_branch_id = ins_general_methods.verify_department_details(str_branch_code)
#
#                    if int_branch_id:
#                        ins_ticket_base.int_branch_id = int_branch_id
#                    ins_ticket_base.str_auto_invoice_location = str_location
#                    ins_ticket_base.str_salesman_code = str_salesman_code
#                    pass
#                

            ins_ticket_base.int_pay_back_account_id_inv = ins_general_methods.get_account_data(ins_capture_ticket_data.str_pay_back_account_code)[0]
            ins_ticket_base.flt_pay_back_commission_inv = ins_capture_ticket_data.flt_payback_commission_ext

            ins_ticket_base.flt_cc_charge_collected_ext  = ins_capture_ticket_data.flt_cc_charge_collected_ext
            
            #refer CR #45305
            if ins_ticket_base.str_pax_type == 'CHD' and ins_capture_ticket_data.bln_cc_charge_collected_chd :
                if ins_capture_ticket_data.flt_cc_charge_collected_chd:
                    ins_ticket_base.flt_cc_charge_collected_ext = ins_capture_ticket_data.flt_cc_charge_collected_chd
            elif ins_ticket_base.str_pax_type == 'INF' and ins_capture_ticket_data.bln_cc_charge_collected_inf :
                if ins_capture_ticket_data.flt_cc_charge_collected_inf:
                    ins_ticket_base.flt_cc_charge_collected_ext = ins_capture_ticket_data.flt_cc_charge_collected_inf

            if 'ALL' in ins_general_methods.ins_global.dct_pax_no_selling_price and ins_capture_ticket_data.flt_selling_price_ext :
                ins_ticket_base.flt_selling_price_ext =  ins_capture_ticket_data.flt_selling_price_ext

            elif ins_general_methods.ins_global.dct_pax_no_selling_price and 'ALL' not in ins_general_methods.ins_global.dct_pax_no_selling_price and ins_capture_ticket_data.flt_selling_price_ext:
                ins_ticket_base.flt_selling_price_ext = ins_general_methods.ins_global.dct_pax_no_selling_price.get(ins_capture_ticket_data.int_tst_count,0)

            elif ins_capture_ticket_data.flt_selling_price_ext:
                ins_ticket_base.flt_selling_price_ext =  ins_capture_ticket_data.flt_selling_price_ext


            if ins_ticket_base.str_pax_type == 'CHD' and ins_capture_ticket_data.bln_chd_selling_price :
                ins_ticket_base.flt_selling_price_ext = ins_capture_ticket_data.flt_selling_price_child
            elif ins_ticket_base.str_pax_type == 'INF' and ins_capture_ticket_data.bln_inf_selling_price:
                ins_ticket_base.flt_selling_price_ext = ins_capture_ticket_data.flt_selling_price_infant

            if ins_ticket_base.flt_selling_price_ext and ins_general_methods.str_reverse_calculate_dis_extra_amt and not ins_ticket_base.str_original_issue:

                flt_selling_price = ins_ticket_base.flt_market_fare_inv + ins_ticket_base.flt_total_tax_inv +\
                                    ins_ticket_base.flt_service_charge + ins_ticket_base.flt_pay_back_commission_inv -\
                                    ins_ticket_base.flt_discount_given_inv + ins_ticket_base.flt_extra_earning_inv + \
                                    ins_ticket_base.flt_cc_charge_collected_ext

                flt_difference = ins_ticket_base.flt_selling_price_ext - flt_selling_price

                if flt_difference < 0 :

                    ins_ticket_base.flt_discount_given_inv = abs(flt_difference)
                    if ins_general_methods.str_reverse_calculate_dis_extra_amt.upper()  ==  'EXTRA EARNING':
                        ins_ticket_base.flt_extra_earning_inv = 0
                elif flt_difference :
                    ins_ticket_base.flt_discount_given_inv = 0
                    if ins_general_methods.str_reverse_calculate_dis_extra_amt.upper()  ==  'SERVICE FEE':
                        ins_ticket_base.flt_service_charge = flt_difference
                    elif ins_general_methods.str_reverse_calculate_dis_extra_amt.upper()  ==  'EXTRA EARNING':
                        ins_ticket_base.flt_extra_earning_inv = flt_difference

                ins_ticket_base.flt_selling_price = flt_selling_price


            if ins_ticket_base.flt_service_charge and ins_ticket_base.flt_market_fare_inv: #39457
                ins_ticket_base.flt_service_charge_percentage_inv = round((ins_ticket_base.flt_service_charge/ins_ticket_base.flt_market_fare_inv)*100,2)

            if ins_ticket_base.flt_discount_given_inv :
                ins_ticket_base.int_discount_account_id_inv = ins_general_methods.get_system_mapping_account_id('DISCOUNT')
                if ins_ticket_base.flt_market_fare_inv : #39457
                    ins_ticket_base.flt_discount_given_percentage_inv = round((ins_ticket_base.flt_discount_given_inv/ins_ticket_base.flt_market_fare_inv)*100,2)

            if ins_ticket_base.flt_extra_earning_inv :
                if ins_ticket_base.flt_market_fare_inv : #39457
                    ins_ticket_base.flt_extra_earninig_percentage_inv = round((ins_ticket_base.flt_extra_earning_inv/ins_ticket_base.flt_market_fare_inv)*100,2)

                ins_ticket_base.int_extra_earning_account_id_inv = ins_general_methods.get_system_mapping_account_id('EXTRA_EARNING')

            # RM Optional fields
            ins_ticket_base.str_rm_opt1 = ins_capture_ticket_data.vchr_field_1
            ins_ticket_base.str_rm_opt2 = ins_capture_ticket_data.vchr_field_2
            ins_ticket_base.str_rm_opt3 = ins_capture_ticket_data.vchr_field_3
            ins_ticket_base.str_rm_opt4 = ins_capture_ticket_data.vchr_field_4
            ins_ticket_base.str_rm_opt5 = ins_capture_ticket_data.vchr_field_5
            ins_ticket_base.str_rm_opt6 = ins_capture_ticket_data.vchr_field_6
            ins_ticket_base.str_rm_opt7 = ins_capture_ticket_data.vchr_field_7
            ins_ticket_base.str_rm_opt8 = ins_capture_ticket_data.vchr_field_8
            ins_ticket_base.str_rm_opt9 = ins_capture_ticket_data.vchr_field_9
            ins_ticket_base.str_rm_opt10 = ins_capture_ticket_data.vchr_field_10
            ins_ticket_base.str_rm_opt11 = ins_capture_ticket_data.vchr_field_11 #40299
            ins_ticket_base.str_rm_opt12 = ins_capture_ticket_data.vchr_field_12
            ins_ticket_base.str_rm_opt13 = ins_capture_ticket_data.vchr_field_13
            ins_ticket_base.str_rm_opt14 = ins_capture_ticket_data.vchr_field_14
            ins_ticket_base.str_rm_opt15 = ins_capture_ticket_data.vchr_field_15
            ins_ticket_base.str_rm_opt16 = ins_capture_ticket_data.vchr_field_16
            ins_ticket_base.str_rm_opt17 = ins_capture_ticket_data.vchr_field_17
            ins_ticket_base.str_rm_opt18 = ins_capture_ticket_data.vchr_field_18
            ins_ticket_base.str_rm_opt19 = ins_capture_ticket_data.vchr_field_19
            ins_ticket_base.str_rm_opt20 = ins_capture_ticket_data.vchr_field_20
            
            ins_ticket_base.json_user_defined_remark = json.dumps(ins_capture_ticket_data.json_user_defined_remark)

            ins_ticket_base.str_airplus_cust_code    = ins_capture_ticket_data.str_airplus_cust_code
            ins_ticket_base.str_airplus_cost_center   = ins_capture_ticket_data.str_airplus_cost_center
            ins_ticket_base.str_airplus_project_code  = ins_capture_ticket_data.str_airplus_project_code
            ins_ticket_base.str_airplus_employee_id   = ins_capture_ticket_data.str_airplus_employee_id
            ins_ticket_base.str_airplus_dept_code     = ins_capture_ticket_data.str_airplus_dept_code
            ins_ticket_base.str_airplus_action_no     = ins_capture_ticket_data.str_airplus_action_no
            ins_ticket_base.str_airplus_internal_acnt_no = ins_capture_ticket_data.str_airplus_internal_acnt_no
            ins_ticket_base.str_airplus_secretary     = ins_capture_ticket_data.str_airplus_secretary
            ins_ticket_base.str_airplus_trip_reason   = ins_capture_ticket_data.str_airplus_trip_reason
            ins_ticket_base.str_airplus_card_expiry_date = ins_capture_ticket_data.str_airplus_card_expiry_date
            ins_ticket_base.str_airplus_resource_code  = ins_capture_ticket_data.str_airplus_resource_code
            ins_ticket_base.str_airplus_lpo_no         = ins_capture_ticket_data.str_airplus_lpo_no
            ins_ticket_base.str_airplus_pa             = ins_capture_ticket_data.str_airplus_pa
            ins_ticket_base.flt_airplus_service_fee    = ins_capture_ticket_data.flt_airplus_service_fee or 0.00
            ins_ticket_base.str_airplus_card_type      = ins_capture_ticket_data.str_airplus_card_type
            ins_ticket_base.str_airplus_accounting_unit = ins_capture_ticket_data.str_airplus_accounting_unit
            ins_ticket_base.str_airplus_departure_date = ins_capture_ticket_data.str_airplus_departure_date or None
            ins_ticket_base.str_airplus_order_no       = ins_capture_ticket_data.str_airplus_order_no

            ins_ticket_base.str_connection_ticket = ins_capture_ticket_data.str_connection_ticket 

            #35976
            if ins_capture_ticket_data.bln_emd and ins_ticket_base.str_connection_ticket \
                                               and not ins_ticket_base.str_remarks :                    
                #38116
                if ins_ticket_base.str_connection_ticket in ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector:
                    ins_ticket_base.str_remarks = ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector[ins_ticket_base.str_connection_ticket][0]
                    ins_ticket_base.str_region_code = ins_general_methods.ins_global.dct_emd_connection_ticket_and_sector[ins_ticket_base.str_connection_ticket][1] #38326
                    pass
                else:
                    lst_emd_connection_details = ins_general_methods.get_sector_for_emd_tickets(ins_ticket_base.str_connection_ticket)
                    if lst_emd_connection_details and len(lst_emd_connection_details) > 0 :
                        ins_ticket_base.str_remarks = lst_emd_connection_details[0] or ''
                        ins_ticket_base.str_region_code = lst_emd_connection_details[1] or ''
            ins_ticket_base.bln_emd =  ins_capture_ticket_data.bln_emd
            ins_ticket_base.int_cost_centre_id_ext =  ins_ticket_base.int_location_id
            ins_ticket_base.int_department_id_ext = ins_ticket_base.int_branch_id
            ins_ticket_base.str_agency_sales_man = ins_capture_ticket_data.str_agency_sales_man
            ins_ticket_base.str_agency_ticketing_staff = ins_capture_ticket_data.str_agency_ticketing_staff
            ins_ticket_base.str_agency_traacs_user = ins_capture_ticket_data.str_agency_traacs_user
            ins_ticket_base.str_agency_adv_receipt_no = ins_capture_ticket_data.str_agency_adv_receipt_no
            ins_ticket_base.str_agency_internal_remarks = ins_capture_ticket_data.str_agency_internal_remarks
            ins_ticket_base.str_agency_product_code = ins_capture_ticket_data.str_agency_product_code

            if ins_capture_ticket_data.str_agency_product_code :
                int_product_master_id = ins_general_methods.get_product_master_id(ins_capture_ticket_data.str_agency_product_code)
                if not ins_capture_ticket_data.bln_refund:  #...refer #20802
                    ins_ticket_base.int_product_master_id_inv = int_product_master_id
                else:
                    ins_ticket_base.int_product_master_id_rfd = int_product_master_id

            ins_ticket_base.str_agency_sub_product_code = ins_capture_ticket_data.str_agency_sub_product_code
            ins_ticket_base.str_agency_auto_invoice_yes_no = ins_capture_ticket_data.str_agency_auto_invoice_yes_no

#                if ins_ticket_base.str_agency_adv_receipt_no : # Refs #16184
#                    ins_ticket_base.str_agency_auto_invoice_yes_no = 'YES'
#                    ins_capture_ticket_data.str_agency_auto_invoice_yes_no = 'YES'

            ins_ticket_base.str_party_code = ins_capture_ticket_data.str_party_code
            ins_ticket_base.str_party_file_job_card_no = ins_capture_ticket_data.str_party_file_job_card_no
            ins_ticket_base.str_party_lpo_no = ins_capture_ticket_data.str_party_lpo_no
            ins_ticket_base.int_party_maximum_tickets = ins_capture_ticket_data.int_party_maximum_tickets
            ins_ticket_base.str_party_multiple_fop_yes_no = ins_capture_ticket_data.str_party_multiple_fop_yes_no
            ins_ticket_base.str_party_additional_ar = ins_capture_ticket_data.str_party_additional_ar
            ins_ticket_base.str_cust_approver_name = ins_capture_ticket_data.str_cust_approver_name
            ins_ticket_base.str_cust_approver_email = ins_capture_ticket_data.str_cust_approver_email
            ins_ticket_base.str_cust_employee_no = ins_capture_ticket_data.str_cust_employee_no
            ins_ticket_base.str_cust_employee_grade = ins_capture_ticket_data.str_cust_employee_grade
            ins_ticket_base.str_cust_cost_centre = ins_capture_ticket_data.str_cust_cost_centre
            ins_ticket_base.str_cust_department = ins_capture_ticket_data.str_cust_department
            ins_ticket_base.str_cust_accounting_unit = ins_capture_ticket_data.str_cust_accounting_unit
            ins_ticket_base.str_cust_internal_ac_no = ins_capture_ticket_data.str_cust_internal_ac_no
            ins_ticket_base.str_cust_project_code = ins_capture_ticket_data.str_cust_project_code
            ins_ticket_base.str_cust_action_no = ins_capture_ticket_data.str_cust_action_no
            ins_ticket_base.str_cust_job_code = ins_capture_ticket_data.str_cust_job_code
            ins_ticket_base.str_cust_resource_code = ins_capture_ticket_data.str_cust_resource_code
            ins_ticket_base.str_cust_commitment_no = ins_capture_ticket_data.str_cust_commitment_no
            ins_ticket_base.str_cust_purpose_of_travel = ins_capture_ticket_data.str_cust_purpose_of_travel
            ins_ticket_base.str_cust_pax_mobile = ins_capture_ticket_data.str_cust_pax_mobile or str_isection_mobile
            ins_ticket_base.str_cust_pax_mobile = ins_ticket_base.str_cust_pax_mobile.replace(' ','').strip().replace('-','').strip()
            try :
                ins_ticket_base.str_cust_pax_mobile = re.findall(r'\d+',ins_ticket_base.str_cust_pax_mobile)[0]
                ins_ticket_base.str_cust_pax_mobile = ins_ticket_base.str_cust_pax_mobile.lstrip('0')

            except :
                ins_ticket_base.str_cust_pax_mobile = ins_ticket_base.str_cust_pax_mobile[:50]
                pass
            ins_ticket_base.str_cust_pax_email = ins_capture_ticket_data.str_cust_pax_email or str_isection_email
            ins_ticket_base.str_cust_engagement_code = ins_capture_ticket_data.str_cust_engagement_code
            ins_ticket_base.flt_published_fare_ext = ins_capture_ticket_data.flt_published_fare_ext
            ins_ticket_base.flt_original_fare_ext = ins_capture_ticket_data.flt_original_fare_ext
            ins_ticket_base.flt_printing_fare_ext = ins_capture_ticket_data.flt_printing_fare_ext

            #38905
            if str(int_count) in ins_capture_ticket_data.dct_ssr_docs_data and ins_capture_ticket_data.dct_ssr_docs_data[str(int_count)]:
                ins_ticket_base.str_passport_no = ins_capture_ticket_data.dct_ssr_docs_data[str(int_count)][0]
                ins_ticket_base.str_nationality = ins_capture_ticket_data.dct_ssr_docs_data[str(int_count)][1] 
            int_count += 1

            if 'ALL' in ins_general_methods.ins_global.dct_pax_no_service_fee and ins_capture_ticket_data.flt_rm_service_charge :
                ins_ticket_base.flt_service_fee_ext =  ins_capture_ticket_data.flt_rm_service_charge

            elif ins_general_methods.ins_global.dct_pax_no_service_fee and 'ALL' not in ins_general_methods.ins_global.dct_pax_no_service_fee :
                ins_ticket_base.flt_service_fee_ext = ins_general_methods.ins_global.dct_pax_no_service_fee.get(ins_capture_ticket_data.int_tst_count,0)

            elif ins_capture_ticket_data.flt_rm_service_charge:
                ins_ticket_base.flt_service_fee_ext =  ins_capture_ticket_data.flt_rm_service_charge


#                ins_ticket_base.flt_service_fee_ext = ins_capture_ticket_data.flt_service_fee_ext 
            ins_ticket_base.flt_extra_earning_ext = ins_ticket_base.flt_extra_earning_inv
            ins_ticket_base.flt_payback_commission_ext = ins_capture_ticket_data.flt_payback_commission_ext

            ins_ticket_base.flt_discount_given_ext = ins_capture_ticket_data.flt_discount_given_ext or ins_ticket_base.flt_discount_given_inv
#            ins_ticket_base.flt_selling_price_ext = ins_capture_ticket_data.flt_selling_price_ext
            ins_ticket_base.flt_lowest_offered_ext = ins_capture_ticket_data.flt_lowest_offered_ext
            ins_ticket_base.str_reason_for_choose_higher_ext = ins_capture_ticket_data.str_reason_for_choose_higher_ext
            ins_ticket_base.flt_fare_accepted_or_paid_ext = ins_capture_ticket_data.flt_fare_accepted_or_paid_ext
            ins_ticket_base.flt_fare_lost_amount_ext = ins_capture_ticket_data.flt_fare_lost_amount_ext
            ins_ticket_base.str_against_doc_ext = ins_capture_ticket_data.str_against_doc_ext
            ins_ticket_base.str_corp_card_code_ext = ins_capture_ticket_data.str_corp_card_code_ext
            ins_ticket_base.str_compliance_ext = ins_capture_ticket_data.str_compliance_ext
            ins_ticket_base.str_pnr_type_ext = ins_capture_ticket_data.str_pnr_type_ext
            


            if ins_ticket_base.str_party_additional_ar :
                lst_split_lines = ins_ticket_base.str_party_additional_ar.strip().split('/')

                if len(lst_split_lines) > 1 :
                    ins_capture_ticket_data.str_rm_account_code_1 = lst_split_lines[0]
                    ins_capture_ticket_data.flt_rm_amount_1 = lst_split_lines[1] or 0

                if len(lst_split_lines) > 3 :
                    ins_capture_ticket_data.str_rm_account_code_2 = lst_split_lines[2]
                    ins_capture_ticket_data.flt_rm_amount_2 = lst_split_lines[3] or 0

                if len(lst_split_lines) > 5 :
                    ins_capture_ticket_data.str_rm_account_code_3 = lst_split_lines[4]
                    ins_capture_ticket_data.flt_rm_amount_3 = lst_split_lines[5] or 0

                ins_ticket_base.int_rm_account_id_1 = ins_general_methods.get_account_data(ins_capture_ticket_data.str_rm_account_code_1,'C')[0]
                ins_ticket_base.int_rm_account_id_2 = ins_general_methods.get_account_data(ins_capture_ticket_data.str_rm_account_code_2,'C')[0]
                ins_ticket_base.int_rm_account_id_3 = ins_general_methods.get_account_data(ins_capture_ticket_data.str_rm_account_code_3,'C')[0]

                ins_ticket_base.flt_rm_amount_1 = float(ins_capture_ticket_data.flt_rm_amount_1)
                ins_ticket_base.flt_rm_amount_2 = float(ins_capture_ticket_data.flt_rm_amount_2)
                ins_ticket_base.flt_rm_amount_3 = float(ins_capture_ticket_data.flt_rm_amount_3)

            ins_ticket_base.str_quot_option_1 = ins_capture_ticket_data.str_quot_option_1
            ins_ticket_base.str_quot_option_2 = ins_capture_ticket_data.str_quot_option_2
            ins_ticket_base.str_crm_reference = ins_capture_ticket_data.str_crm_reference # Refer 23236
            ins_ticket_base.str_master_reference = ins_capture_ticket_data.str_master_reference #refer 28674
            ins_ticket_base.str_master_narration = ins_capture_ticket_data.str_master_narration #refer 28674
                            
            ins_ticket_base.str_rm_field_data = ','.join(ins_capture_ticket_data.lst_rm_field_data)[:2000]
            ins_ticket_base.str_fop_data = ins_capture_ticket_data.str_fop_data[:2000]
            ins_ticket_base.dat_rm_lpo_date = ins_capture_ticket_data.dat_rm_lpo_date   # Refer 34857
            
            #45745
            ins_ticket_base.int_cust_traveller_id = ins_general_methods.get_passenger_profile_id(ins_capture_ticket_data.str_cust_traveller_id)
                            
            # refer 20879
            int_account_type = None
            ins_ticket_base.int_credit_card_pos_id = None
            if ins_ticket_base.int_account_master_id:
                int_account_type = ins_general_methods.get_account_type(ins_ticket_base.int_account_master_id)
            if ins_ticket_base.int_account_master_id and int_account_type == 5: 
                int_credit_card_pos_id = ins_general_methods.get_credit_card_pos_id(ins_capture_ticket_data.int_credit_card_pos_id)
                if int_credit_card_pos_id :
                   ins_ticket_base.int_credit_card_pos_id = int_credit_card_pos_id
                   ins_ticket_base.str_cc_number = ins_capture_ticket_data.str_cc_number    


            ins_capture_ticket_data.dct_extra_capturing_data.update({
                                "AGENCY_ADV_RECEIPT_NO" : ins_capture_ticket_data.str_agency_adv_receipt_no,
                                "PARTY_MULTIPLE_FOP_YES_NO" : ins_capture_ticket_data.str_party_multiple_fop_yes_no,
                                "PARTY_ADDITIONAL_AR" : ins_capture_ticket_data.str_party_additional_ar,
                                "AGAINST_DOCUMENT_NO" : ins_capture_ticket_data.str_against_doc_ext,
                                "CUST_POS_ID" : ins_capture_ticket_data.int_credit_card_pos_id ,
                                "CUST_CC_NUMBER" : ins_capture_ticket_data.str_cc_number ,
                                "MASTER_REFERENCE" : ins_capture_ticket_data.str_master_reference,
                                "MASTER_NARRATION" : ins_capture_ticket_data.str_master_narration,
                                "RM_FIELD_DATA" : ins_ticket_base.str_rm_field_data
                                })
            
            ins_ticket_base.json_extra_capturing_data = json.dumps(ins_capture_ticket_data.dct_extra_capturing_data)
            
            ins_ticket_base.bln_set_airline_as_supplier = False#ins_capture_ticket_data.bln_set_airline_as_supplier        
            ins_ticket_base.bln_set_airline_as_supplier_rfd = False#ins_capture_ticket_data.bln_set_airline_as_supplier_rfd        
            ins_ticket_base.bln_xo_capturing = ins_capture_ticket_data.bln_xo_capturing # refer 25579
            int_ticket_count += 1

            lst_ticket_capture_details.append(ins_ticket_base)
            pass



        return lst_ticket_capture_details , lst_tickets
        
    def create_voucher_data_to_save(self,ins_capture_ticket_data , str_file ):
         ## Refer 13823
         
        lst_hotel_voucher_data = []
        lst_car_voucher_data = []
        lst_other_voucher_data = []
        
        
        for ins_service_base in ins_capture_ticket_data.lst_ins_voucher :
            
            str_cc_no = ins_service_base.str_credit_card_num.strip()
            if ins_service_base.int_voucher_identifier in ins_capture_ticket_data.lst_cc_seg_nos :
                str_cc_no = ins_capture_ticket_data.str_cc_card_no
            
            ins_service_base.str_voucher_number = ins_service_base.str_voucher_number.upper()
            ins_service_base.str_voucher_issue_date = ins_general_methods.generate_valid_date(ins_capture_ticket_data.str_file_creation_date)
            
            ins_service_base.str_crs_company = ins_capture_ticket_data.str_crs_company
            ins_service_base.str_customer_lpo_number = ins_capture_ticket_data.str_rm_lpo_number
            ins_service_base.dat_rm_lpo_date = ins_capture_ticket_data.dat_rm_lpo_date
            ins_service_base.str_customer_cost_centre = ins_capture_ticket_data.str_cust_cost_centre
            ins_service_base.str_customer_emp_no = ins_capture_ticket_data.str_rm_employee_number
            if ins_service_base.int_pax_identifier in ins_capture_ticket_data.dct_pax_name :
                ins_service_base.str_pax_name = ins_capture_ticket_data.dct_pax_name[int(ins_service_base.int_pax_identifier)]
            else :
                ins_service_base.str_pax_name = ins_capture_ticket_data.str_pax_name
            
            
            if ins_capture_ticket_data.str_rm_cost_centre :
                ins_service_base.int_location_id,ins_service_base.int_company_id = ins_general_methods.verify_location_details(ins_capture_ticket_data.str_rm_cost_centre)

            if ins_capture_ticket_data.str_branch_code :
                ins_service_base.int_department_id = ins_general_methods.verify_department_details(ins_capture_ticket_data.str_branch_code)
               
            ## refer 35080
            if not ins_capture_ticket_data.str_ticketing_agent_code:
                ins_capture_ticket_data.str_ticketing_agent_code = ins_general_methods.ins_capture_base.ticketing_agent_char_code_for_ins_voucher
                ins_capture_ticket_data.str_ticketing_agent_numeric_code = ins_general_methods.ins_capture_base.ticketing_agent_numeric_code_for_ins_voucher 

            (ins_service_base.int_counter_staff_id,
                str_mapping_client_account,
                int_mapping_client_department_id,
                int_mapping_client_location_id ,
                int_counter_staff_cost_center ,
                int_counter_staff_department,
                str_agent_numeric_code,
                temp) = ins_general_methods.get_counter_staff_id(ins_service_base.str_crs_company,
                                                                        ins_capture_ticket_data.str_ticketing_agent_code,
                                                                                    ins_capture_ticket_data.str_ticketing_agent_numeric_code,
                                                                                    ins_general_methods.ins_auto_inv.bln_staff_num_code_match)
            
            
            
            if not ins_service_base.int_counter_staff_id :
                print("Please add this counter staff in TRAACS Character Code : ",ins_capture_ticket_data.str_ticketing_agent_code,' Numeric Code :', ins_capture_ticket_data.str_ticketing_agent_numeric_code)
                raise Exception("Please add this counter staff in TRAACS Character Code : %s Numeric Code : %s"%(ins_capture_ticket_data.str_ticketing_agent_code, ins_capture_ticket_data.str_ticketing_agent_numeric_code))    #refer #39657
             #  refer 25712   
            (ins_service_base.int_booking_staff_id,
                    str_book_mapping_client_account,
                    int_book_mapping_client_department_id,
                    int_book_mapping_client_location_id,
                    int_book_counter_staff_cost_center ,
                        int_book_counter_staff_department,
                    str_agent_numeric_code,
                    temp) = ins_general_methods.get_counter_staff_id(ins_service_base.str_crs_company,
                                                                        ins_capture_ticket_data.str_booking_agent_code,
                                                                            ins_capture_ticket_data.str_booking_agent_numeric_code,
                                                                        ins_general_methods.ins_auto_inv.bln_staff_num_code_match)
            
            
            
            
            if not ins_service_base.int_booking_staff_id and ins_service_base.str_voucher_type == 'H': #40507
                print("Please add this counter staff in TRAACS Character Code : ",ins_capture_ticket_data.str_booking_agent_code,' Numeric Code :', ins_capture_ticket_data.str_booking_agent_numeric_code)
                raise Exception("Please add this counter staff in TRAACS Character Code : %s Numeric Code : %s"%(ins_capture_ticket_data.str_booking_agent_code, ins_capture_ticket_data.str_booking_agent_numeric_code))     #refer #39657
            if int_counter_staff_cost_center and not ins_service_base.int_location_id:
                ins_service_base.int_location_id = int_counter_staff_cost_center
                
            if int_book_counter_staff_cost_center and not ins_service_base.int_location_id:
                ins_service_base.int_location_id = int_book_counter_staff_cost_center
                
            if int_counter_staff_department and not ins_service_base.int_department_id :
                ins_service_base.int_department_id = int_counter_staff_department
                
            if int_book_counter_staff_department and not ins_service_base.int_department_id :
                ins_service_base.int_department_id = int_book_counter_staff_department
                
            # Getting minimum ID of cost center & minimum ID of Department if both are none
            if not ins_service_base.int_location_id :                
                ins_service_base.int_location_id =ins_general_methods.int_min_location_id
                #ins_service_base.int_location_id = 1 
            if not ins_service_base.int_department_id :
                ins_service_base.int_department_id = ins_general_methods.int_min_department_id
                #ins_service_base.int_department_id = 1 
            
            #customer code from Remark section
            ins_service_base.str_customer_code = ins_capture_ticket_data.str_rm_customer_code.split(';')[0].strip()
            #customer code from counter staff  #39635
            if ins_general_methods.ins_auto_inv.bln_capture_use_client_set_in_tick_agent and not ins_service_base.str_customer_code :
                ins_service_base.str_customer_code = str_mapping_client_account
            elif ins_general_methods.ins_auto_inv.bln_capture_use_client_set_in_book_agent and not ins_service_base.str_customer_code :
                ins_service_base.str_customer_code = str_book_mapping_client_account
            
            # // Get Customer data
            int_account_master_id = ins_general_methods.get_account_data(ins_service_base.str_customer_code,'C')[0]

#                refer #13997
            if not int_account_master_id and str_cc_no:
                int_account_master_id = ins_general_methods.get_customer_id_using_card_no(str_cc_no,ins_service_base.str_crs_company) #39593

            ins_service_base.int_customer_account_id_inv = int_account_master_id
            #40225
            if ins_service_base.str_voucher_issue_date:
                    ins_service_base.flt_supplier_currency_roe = ins_general_methods.get_roe_of_currency_for_a_date(ins_service_base.str_voucher_currency_code or ins_capture_ticket_data.str_defult_currency_code,ins_service_base.str_voucher_issue_date)
                    ins_service_base.flt_cust_currency_roe,ins_service_base.str_cust_currency = ins_general_methods.get_customer_currency_roe(ins_service_base.int_customer_account_id_inv,ins_service_base.str_voucher_issue_date,\
                            ins_general_methods.str_base_currency)
            
            int_corp_card_id = None
            int_supplier_id = None
            
            if ins_service_base.int_voucher_identifier in ins_general_methods.ins_global.dct_seg_no_voucher_supplier :
                int_corp_card_id = ins_general_methods.get_corp_card_id(ins_general_methods.ins_global.dct_seg_no_voucher_supplier.get(ins_service_base.int_voucher_identifier,''))
                
                if int_corp_card_id  :
                    int_supplier_id = ins_general_methods.get_account_data(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['DUMMY_SUP_CODE'].strip(':'))[0]
                else :
                    int_supplier_id = ins_general_methods.get_account_data(ins_general_methods.ins_global.dct_seg_no_voucher_supplier.get(ins_service_base.int_voucher_identifier,''))[0]
                    
            elif ins_service_base.str_vendor_code :
                int_corp_card_id = ins_general_methods.get_corp_card_id(ins_service_base.str_vendor_code)
                
                if int_corp_card_id  :
                    int_supplier_id = ins_general_methods.get_account_data(ins_general_methods.ins_capture_base.dct_voucher_conf_data['HOTEL']['DUMMY_SUP_CODE'].strip(':'))[0]
                else :
                    int_supplier_id = ins_general_methods.get_account_data(ins_service_base.str_vendor_code)[0]
                
                
            elif ins_service_base.str_hotel_name:
                int_supplier_id = ins_general_methods.get_account_id_by_name(ins_service_base.str_hotel_name)
                
            if int_supplier_id :
                ins_service_base.int_supplier_id = int_supplier_id
            elif ins_service_base.str_iata_num :
                ins_service_base.int_supplier_id = ins_general_methods.get_supplier_account_id_from_iata_num(ins_service_base.str_iata_num)


            if str_cc_no:
                ins_service_base.str_cc_number_ext = str_cc_no #39948
                (int_account_master_id,int_account_type) = ins_general_methods.get_corporate_card_id(str_cc_no,ins_service_base.str_crs_company)
                if int_account_master_id:
                    ins_service_base.int_corp_card_id = int_account_master_id
                    if int_account_type == 1 : #// bln customer card true
                        ins_service_base.int_card_payment_type  = 3
                    else :
                        ins_service_base.int_card_payment_type  = 2
                if not int_account_master_id :# and ins_capture_ticket_data.str_card_approval_code :
                        ins_service_base.bln_uccf = True
                        ins_service_base.int_card_payment_type = 1
                        
            if  int_corp_card_id :
                ins_service_base.int_corp_card_id = int_corp_card_id
                ins_service_base.int_card_payment_type = 2
                        
#            if dct_seg_no_voucher_discount.has_key('ALL') and ins_capture_ticket_data.flt_rm_voucher_discount and len(dct_seg_no_voucher_discount.keys()) == 1:
#                ins_service_base.flt_discount =  ins_capture_ticket_data.flt_rm_voucher_discount
            if ins_service_base.int_voucher_identifier in ins_general_methods.ins_global.dct_seg_no_voucher_discount and ins_capture_ticket_data.flt_rm_voucher_discount :
                ins_service_base.flt_discount =  ins_general_methods.ins_global.dct_seg_no_voucher_discount.get(ins_service_base.int_voucher_identifier,0)
            
            
            if ins_service_base.int_voucher_identifier in ins_general_methods.ins_global.dct_seg_no_voucher_selling_price and ins_capture_ticket_data.flt_rm_voucher_discount :
                ins_service_base.flt_fare_inv =  ins_general_methods.ins_global.dct_seg_no_voucher_selling_price.get(ins_service_base.int_voucher_identifier,0)
            
            
#            if dct_seg_no_voucher_tax.has_key('ALL') and ins_capture_ticket_data.flt_rm_voucher_tax :
#                ins_service_base.flt_total_tax_inv =  ins_capture_ticket_data.flt_rm_voucher_tax
#                ins_service_base.flt_total_tax_credit_inv =  ins_capture_ticket_data.flt_rm_voucher_tax
            if ins_service_base.int_voucher_identifier in ins_general_methods.ins_global.dct_seg_no_voucher_tax and ins_capture_ticket_data.flt_rm_voucher_tax :
                ins_service_base.flt_total_tax_inv =  ins_general_methods.ins_global.dct_seg_no_voucher_tax.get(ins_service_base.int_voucher_identifier,0)
                ins_service_base.flt_total_tax_credit_inv =  ins_general_methods.ins_global.dct_seg_no_voucher_tax.get(ins_service_base.int_voucher_identifier,0)
            
            
            
            
#            if dct_seg_no_service_fee.has_key('ALL') and ins_capture_ticket_data.flt_rm_service_charge :
#                ins_service_base.flt_service_fee =  ins_capture_ticket_data.flt_rm_service_charge
            if ins_service_base.int_voucher_identifier in ins_general_methods.ins_global.dct_seg_no_service_fee and ins_capture_ticket_data.flt_rm_service_charge :
                ins_service_base.flt_service_fee =  ins_general_methods.ins_global.dct_seg_no_service_fee.get(ins_service_base.int_voucher_identifier,0)

            elif ins_service_base.int_voucher_identifier in ins_general_methods.ins_global.dct_seg_no_voucher_supplier_amount :
                ins_service_base.flt_service_fee =  ins_service_base.flt_fare_inv + ins_service_base.flt_discount - ins_general_methods.ins_global.dct_seg_no_voucher_supplier_amount.get(ins_service_base.int_voucher_identifier,0) 
                
#            if ins_service_base.flt_fare_inv and not ins_service_base.bln_hhl and not dct_seg_no_selling_price.has_key(ins_service_base.int_voucher_identifier):
#                ins_service_base.flt_fare_inv = ins_service_base.flt_fare_inv - ins_service_base.flt_service_fee
                
            
            ins_service_base.flt_fare_inv = ins_service_base.flt_fare_inv - ins_service_base.flt_total_tax_inv - ins_service_base.flt_service_fee + ins_service_base.flt_discount
                
            if str_cc_no and len(str_cc_no) > 10 :

                ins_service_base.flt_fare_credit_card_inv = ins_service_base.flt_fare_inv
                ins_service_base.flt_fare_credit_inv = 0
                
                ins_service_base.flt_total_tax_credit_card_inv = ins_service_base.flt_total_tax_inv
                ins_service_base.flt_total_tax_credit_inv = 0
                
            else :
                ins_service_base.flt_fare_credit_card_inv = 0
                ins_service_base.flt_fare_credit_inv = ins_service_base.flt_fare_inv

                ins_service_base.flt_total_tax_credit_card_inv = 0
                ins_service_base.flt_total_tax_credit_inv = ins_service_base.flt_total_tax_inv


            ins_service_base.str_file_name_inv = os.path.split(str_file)[-1]
            ins_service_base.str_pnr_no = ins_capture_ticket_data.str_pnr_no
            
            ins_service_base.str_base_currency = ins_service_base.str_base_currency_rfd = ins_general_methods.str_base_currency
            ins_service_base.str_tran_currency = ins_service_base.str_tran_currency_rfd =  ins_service_base.str_voucher_currency_code or ins_capture_ticket_data.str_defult_currency_code   #refer #41471
            
            if ins_service_base.flt_fare_inv :
                ins_service_base.flt_service_fee_percentage = 100*ins_service_base.flt_service_fee/ins_service_base.flt_fare_inv
                ins_service_base.flt_discount_percentage = 100*ins_service_base.flt_discount/ins_service_base.flt_fare_inv
            
            ins_service_base.flt_gross_payable_inv = ins_service_base.flt_fare_inv + ins_service_base.flt_total_tax_inv
            ins_service_base.flt_net_payable_inv = ins_service_base.flt_fare_credit_inv + ins_service_base.flt_total_tax_credit_inv
            ins_service_base.flt_actual_cost_inv = ins_service_base.flt_fare_inv + ins_service_base.flt_total_tax_inv
            ins_service_base.flt_selling_price_inv = ins_service_base.flt_fare_inv + ins_service_base.flt_total_tax_inv + ins_service_base.flt_service_fee - ins_service_base.flt_discount
            ins_service_base.flt_debited_amount_inv = ins_service_base.flt_selling_price_inv - ins_service_base.flt_fare_credit_card_inv - ins_service_base.flt_total_tax_credit_card_inv
            ins_service_base.flt_profit_inv = ins_service_base.flt_service_fee - ins_service_base.flt_discount
            
            ins_service_base.str_voucher_booking_date = ins_general_methods.generate_valid_booking_date(ins_capture_ticket_data.str_pnr_creation_date)
            
            if ins_capture_ticket_data.str_agency_product_code :
                int_product_master_id = ins_general_methods.get_product_master_id(ins_capture_ticket_data.str_agency_product_code)
                if not ins_capture_ticket_data.bln_refund:  #...refer #20802
                    ins_service_base.int_product_master_id_inv = int_product_master_id
                else:
                    ins_service_base.int_product_master_id_rfd = int_product_master_id
            #refer #40299
            ins_service_base.str_agency_sales_man = ins_capture_ticket_data.str_agency_sales_man
            ins_service_base.str_agency_ticketing_staff = ins_capture_ticket_data.str_agency_ticketing_staff
            ins_service_base.str_agency_traacs_user = ins_capture_ticket_data.str_agency_traacs_user
            ins_service_base.str_agency_adv_receipt_no = ins_capture_ticket_data.str_agency_adv_receipt_no
            ins_service_base.str_agency_internal_remarks = ins_capture_ticket_data.str_agency_internal_remarks
            ins_service_base.str_agency_product_code = ins_capture_ticket_data.str_agency_product_code
            ins_service_base.str_agency_sub_product_code = ins_capture_ticket_data.str_agency_sub_product_code
            ins_service_base.str_agency_auto_invoice_yes_no = ins_capture_ticket_data.str_agency_auto_invoice_yes_no
            ins_service_base.str_party_file_job_card_no = ins_capture_ticket_data.str_party_file_job_card_no
            ins_service_base.str_party_lpo_no = ins_capture_ticket_data.str_party_lpo_no
            ins_service_base.int_party_maximum_tickets = ins_capture_ticket_data.int_party_maximum_tickets
            ins_service_base.str_party_multiple_fop_yes_no = ins_capture_ticket_data.str_party_multiple_fop_yes_no
            ins_service_base.str_cust_approver_name = ins_capture_ticket_data.str_cust_approver_name
            ins_service_base.str_cust_approver_email = ins_capture_ticket_data.str_cust_approver_email
            ins_service_base.str_cust_employee_grade = ins_capture_ticket_data.str_cust_employee_grade
            ins_service_base.str_cust_department = ins_capture_ticket_data.str_cust_department
            ins_service_base.str_cust_accounting_unit = ins_capture_ticket_data.str_cust_accounting_unit
            ins_service_base.str_cust_internal_ac_no = ins_capture_ticket_data.str_cust_internal_ac_no
            ins_service_base.str_cust_project_code = ins_capture_ticket_data.str_cust_project_code
            ins_service_base.str_cust_action_no = ins_capture_ticket_data.str_cust_action_no
            ins_service_base.str_cust_job_code = ins_capture_ticket_data.str_cust_job_code
            ins_service_base.str_cust_resource_code = ins_capture_ticket_data.str_cust_resource_code
            ins_service_base.str_cust_commitment_no = ins_capture_ticket_data.str_cust_commitment_no
            ins_service_base.str_cust_purpose_of_travel = ins_capture_ticket_data.str_cust_purpose_of_travel
            ins_service_base.str_cust_pax_mobile = ins_capture_ticket_data.str_cust_pax_mobile or ins_capture_ticket_data.str_isection_mobile
            ins_service_base.str_cust_pax_mobile = ins_service_base.str_cust_pax_mobile.replace(' ','').strip().replace('-','').strip()
            try :
                ins_service_base.str_cust_pax_mobile = re.findall(r'\d+',ins_service_base.str_cust_pax_mobile)[0]
                ins_service_base.str_cust_pax_mobile = ins_service_base.str_cust_pax_mobile.lstrip('0')
            
            except :
                ins_service_base.str_cust_pax_mobile = ins_service_base.str_cust_pax_mobile[:50]
                pass
            ins_service_base.str_cust_pax_email = ins_capture_ticket_data.str_cust_pax_email or ins_capture_ticket_data.str_isection_email
            ins_service_base.str_cust_engagement_code = ins_capture_ticket_data.str_cust_engagement_code
            ins_service_base.str_against_doc_ext = ins_capture_ticket_data.str_against_doc_ext
            ins_service_base.str_corp_card_code_ext = ins_capture_ticket_data.str_corp_card_code_ext
            ins_service_base.str_compliance_ext = ins_capture_ticket_data.str_compliance_ext
            ins_service_base.str_pnr_type_ext = ins_capture_ticket_data.str_pnr_type_ext
            ins_service_base.str_quot_option_1 = ins_capture_ticket_data.str_quot_option_1
            ins_service_base.str_quot_option_2 = ins_capture_ticket_data.str_quot_option_2
            ins_service_base.str_master_reference = ins_capture_ticket_data.str_master_reference 
            ins_service_base.str_master_narration = ins_capture_ticket_data.str_master_narration
            ins_service_base.str_rm_field_data = ','.join(ins_capture_ticket_data.lst_rm_field_data)[:2000]
            ins_service_base.str_rm_opt1 = ins_capture_ticket_data.vchr_field_1
            ins_service_base.str_rm_opt2 = ins_capture_ticket_data.vchr_field_2
            ins_service_base.str_rm_opt3 = ins_capture_ticket_data.vchr_field_3
            ins_service_base.str_rm_opt4 = ins_capture_ticket_data.vchr_field_4
            ins_service_base.str_rm_opt5 = ins_capture_ticket_data.vchr_field_5
            ins_service_base.str_rm_opt6 = ins_capture_ticket_data.vchr_field_6
            ins_service_base.str_rm_opt7 = ins_capture_ticket_data.vchr_field_7
            ins_service_base.str_rm_opt8 = ins_capture_ticket_data.vchr_field_8
            ins_service_base.str_rm_opt9 = ins_capture_ticket_data.vchr_field_9
            ins_service_base.str_rm_opt10 = ins_capture_ticket_data.vchr_field_10
            ins_service_base.str_rm_opt11 = ins_capture_ticket_data.vchr_field_11 
            ins_service_base.str_rm_opt12 = ins_capture_ticket_data.vchr_field_12
            ins_service_base.str_rm_opt13 = ins_capture_ticket_data.vchr_field_13
            ins_service_base.str_rm_opt14 = ins_capture_ticket_data.vchr_field_14
            ins_service_base.str_rm_opt15 = ins_capture_ticket_data.vchr_field_15
            ins_service_base.str_rm_opt16 = ins_capture_ticket_data.vchr_field_16
            ins_service_base.str_rm_opt17 = ins_capture_ticket_data.vchr_field_17
            ins_service_base.str_rm_opt18 = ins_capture_ticket_data.vchr_field_18
            ins_service_base.str_rm_opt19 = ins_capture_ticket_data.vchr_field_19
            ins_service_base.str_rm_opt20 = ins_capture_ticket_data.vchr_field_20
            ins_service_base.json_user_defined_remark = json.dumps(ins_capture_ticket_data.json_user_defined_remark)
            ins_service_base.int_cust_traveller_id = ins_general_methods.get_passenger_profile_id(ins_capture_ticket_data.str_cust_traveller_id)
            
            ins_capture_ticket_data.dct_extra_capturing_data.update({
                                "AGENCY_ADV_RECEIPT_NO" : ins_capture_ticket_data.str_agency_adv_receipt_no,
                                "PARTY_MULTIPLE_FOP_YES_NO" : ins_capture_ticket_data.str_party_multiple_fop_yes_no,
                                "PARTY_ADDITIONAL_AR" : ins_capture_ticket_data.str_party_additional_ar,
                                "AGAINST_DOCUMENT_NO" : ins_capture_ticket_data.str_against_doc_ext,
                                "CUST_POS_ID" : ins_capture_ticket_data.int_credit_card_pos_id ,
                                "CUST_CC_NUMBER" : ins_capture_ticket_data.str_cc_number ,
                                "MASTER_REFERENCE" : ins_capture_ticket_data.str_master_reference,
                                "MASTER_NARRATION" : ins_capture_ticket_data.str_master_narration,
                                "RM_FIELD_DATA" : ins_service_base.str_rm_field_data
                                })
            
            ins_service_base.json_extra_capturing_data = json.dumps(ins_capture_ticket_data.dct_extra_capturing_data)
            
            if ins_service_base.str_voucher_type == 'H' :
                
                if ins_general_methods.get_hotel_voucher_details(ins_service_base.str_voucher_number) :
                    print('Duplication : ' + ins_service_base.str_voucher_number)
                    continue

                ins_service_base.str_hotel_check_in_date = ins_general_methods.generate_valid_date(ins_service_base.str_hotel_check_in_date,ins_service_base.str_voucher_issue_date)
                ins_service_base.str_hotel_check_out_date = ins_general_methods.generate_valid_date(ins_service_base.str_hotel_check_out_date,ins_service_base.str_voucher_issue_date)
                ins_service_base.str_hotel_check_in_time = ins_service_base.str_hotel_check_in_date +' 0:0:0'#time.strptime(ins_service_base.str_hotel_check_in_date +' 0:0:0', "%d/%m/%Y  %H:%M:%S").strptime("%d/%m/%Y  %H:%M:%S")
                ins_service_base.str_hotel_check_out_time = ins_service_base.str_hotel_check_out_date +' 0:0:0'#time.strptime(ins_service_base.str_hotel_check_out_date +' 0:0:0', "%d/%m/%Y  %H:%M:%S").strptime("%d/%m/%Y  %H:%M:%S")
                
                ins_service_base.int_service_id = 4
                ins_service_base.int_city_id,ins_service_base.int_country_id = ins_general_methods.get_city_id(ins_service_base.str_city_name)
                
                if not ins_service_base.int_city_id :
                    print("Please add the city %s in TRAACS "%ins_service_base.str_city_name)
                    raise Exception("Please add the city %s in TRAACS "%ins_service_base.str_city_name)    #refer #39657
                
                ins_service_base.int_vendor_id = ins_general_methods.get_hotel_master_id(ins_service_base.str_hotel_name,ins_service_base.int_city_id)
                if not ins_service_base.int_vendor_id : #refs #17717
                    ins_save_or_update_data.save_hotel([ins_service_base.str_hotel_name[:40],
                                                        ins_service_base.str_hotel_name ,
                                                            ins_service_base.str_hotel_address ,
                                                                ins_service_base.str_hotel_phone ,
                                                                ins_service_base.str_hotel_fax ,
                                                                ins_service_base.str_hotel_email ,
                                                                ins_service_base.int_city_id
                                                            ])
                    ins_service_base.int_vendor_id = ins_general_methods.get_hotel_master_id(ins_service_base.str_hotel_name,ins_service_base.int_city_id)                                        
                                                            
                [ins_service_base.int_meals_plan_id,ins_service_base.str_meals_plan] = ins_general_methods.dct_meals_plan_id.get(ins_service_base.str_meals_plan,[None,''])
                ins_service_base.int_no_of_nights = (datetime.date(int(ins_service_base.str_hotel_check_out_date.split('/')[2]),int(ins_service_base.str_hotel_check_out_date.split('/')[1]),int(ins_service_base.str_hotel_check_out_date.split('/')[0])) - datetime.date(int(ins_service_base.str_hotel_check_in_date.split('/')[2]),int(ins_service_base.str_hotel_check_in_date.split('/')[1]),int(ins_service_base.str_hotel_check_in_date.split('/')[0]))).days
                ins_service_base.int_no_of_room_nights = ins_service_base.int_no_of_nights * ins_service_base.int_no_of_rooms
                
                ins_service_base.int_room_type_id = ins_general_methods.get_room_type_id(ins_service_base.str_room_type)
                
                
                ins_service_base.str_booking_details = "Hotel Booking For %s Persons For %s  Nights"%(str(ins_service_base.int_no_of_guest_inv),str(ins_service_base.int_no_of_nights))
                
                lst_check_in_date = ins_service_base.str_hotel_check_in_date.split('/')
                lst_check_out_date = ins_service_base.str_hotel_check_out_date.split('/')
                dct_hotel_details = {}
                dct_hotel_details['dat_check_in'] = lst_check_in_date[2]+'-'+lst_check_in_date[1]+'-'+lst_check_in_date[0]
                dct_hotel_details['dat_check_out'] = lst_check_out_date[2]+'-'+lst_check_out_date[1]+'-'+lst_check_out_date[0]
                dct_hotel_details['vchr_room_type'] = ins_service_base.str_room_type
                dct_hotel_details['fk_bint_room_type_id'] = ins_service_base.int_room_type_id
                dct_hotel_details['vchr_meals_plan'] = ins_service_base.str_meals_plan
                dct_hotel_details['fk_bint_meals_plan_id'] = ins_service_base.int_meals_plan_id
                dct_hotel_details['int_no_of_rooms'] = ins_service_base.int_no_of_rooms
                
                ins_service_base.json_check_in_check_out_details.append(dct_hotel_details)
                
                lst_hotel_voucher_data.append(ins_service_base)
                
            #40359    
            if ins_service_base.str_voucher_type == 'C' :
                if ins_general_methods.get_car_voucher_details(ins_service_base.str_voucher_number) :
                    print('Duplication on car voucher: ' + ins_service_base.str_voucher_number)
                    continue
                    
                ins_service_base.str_pick_up_date = ins_general_methods.generate_valid_date(ins_service_base.str_pick_up_date,ins_service_base.str_voucher_issue_date)
                ins_service_base.str_drop_off_date = ins_general_methods.generate_valid_date(ins_service_base.str_drop_off_date,ins_service_base.str_voucher_issue_date)
                ins_service_base.str_pick_up_time = ins_service_base.str_pick_up_date +' 0:0:0'
                ins_service_base.str_drop_off_time = ins_service_base.str_drop_off_date +' 0:0:0'
                ins_service_base.str_tran_currency = ins_service_base.str_tran_currency_rfd = ins_service_base.str_voucher_currency_code
                ins_service_base.flt_supplier_currency_roe = ins_general_methods.get_roe_of_currency_for_a_date(ins_service_base.str_tran_currency,
                                                                                                ins_service_base.str_voucher_issue_date)
                                                                                                
                #refer #41724                                                                              
                if not ins_service_base.int_no_of_days :   
                    ins_service_base.int_no_of_days = ((datetime.datetime.strptime(ins_service_base.str_drop_off_date,"%d/%m/%Y") - datetime.datetime.strptime(ins_service_base.str_pick_up_date,"%d/%m/%Y")).days) + 1
                    
#                ins_service_base.flt_net_payable_inv = ins_service_base.flt_fare_inv
#                ins_service_base.flt_fare_inv = round(ins_service_base.flt_net_payable_inv/ins_service_base.int_no_of_days,2)
#                ins_service_base.flt_gross_payable_inv = ins_service_base.flt_fare_inv + ins_service_base.flt_total_tax_inv
#                ins_service_base.flt_actual_cost_inv = ins_service_base.flt_fare_inv + ins_service_base.flt_total_tax_inv
#                ins_service_base.flt_selling_price_inv = ins_service_base.flt_net_payable_inv + ins_service_base.flt_service_fee - ins_service_base.flt_discount
#                ins_service_base.flt_debited_amount_inv = ins_service_base.flt_selling_price_inv - ins_service_base.flt_fare_credit_card_inv - ins_service_base.flt_total_tax_credit_card_inv
#                ins_service_base.flt_profit_inv = ins_service_base.flt_service_fee - ins_service_base.flt_discount
                
                ins_service_base.int_service_id = 3
                ins_service_base.int_car_rental_company_id = ins_general_methods.get_rental_company_id(ins_service_base.str_vendor_code) #as per the BA
                ins_service_base.int_city_id,ins_service_base.int_country_id = ins_general_methods.get_city_id(ins_service_base.str_city_name)
                
                if not ins_service_base.int_city_id :
                    print("Please add the city %s in TRAACS "%ins_service_base.str_city_name)
                    raise Exception("Please add the city %s in TRAACS "%ins_service_base.str_city_name)
                
                if not ins_service_base.int_car_rental_company_id :
                    print('Please add Car Rental Company %s in Traacs'%(ins_service_base.str_vendor_code,))
                    raise Exception("Please add Car Rental Company %s in Traacs"%ins_service_base.str_vendor_code)
                
                lst_car_voucher_data.append(ins_service_base)

            elif ins_service_base.str_voucher_type == 'O' :
                
                if ins_general_methods.get_other_voucher_details(ins_service_base.str_voucher_number) :
                    print('duplication : ' + ins_service_base.str_voucher_number)
                    continue
#                ins_service_base.str_from_date = ins_general_methods.generate_valid_date(ins_service_base.str_from_date,ins_service_base.str_voucher_issue_date)
                
                if ins_service_base.bln_from_to_date :
                
                    if not ins_service_base.str_from_date :
                        ins_service_base.str_from_date = ins_service_base.str_voucher_issue_date#ins_general_methods.generate_valid_date(ins_service_base.str_voucher_issue_date,ins_service_base.str_voucher_issue_date)
                    else :
                        ins_service_base.str_from_date = ins_general_methods.generate_valid_date(ins_service_base.str_from_date,ins_service_base.str_voucher_issue_date)

                    if not ins_service_base.str_to_date :
                        date_1 = datetime.datetime.strptime(ins_service_base.str_from_date, "%d/%m/%Y")

                        ins_service_base.str_to_date = (date_1 + datetime.timedelta(days=ins_service_base.int_no_of_days)).strftime("%d/%m/%Y")
                    else :
                        ins_service_base.str_to_date = ins_general_methods.generate_valid_date(ins_service_base.str_to_date,ins_service_base.str_from_date)
                
                else :
                    ins_service_base.str_from_date = None
                    ins_service_base.str_to_date = None
                ins_service_base.int_service_id = ins_general_methods.get_service_id(ins_service_base.str_voucher_code)


                lst_other_voucher_data.append(ins_service_base)
               
        return lst_hotel_voucher_data ,lst_car_voucher_data ,lst_other_voucher_data


