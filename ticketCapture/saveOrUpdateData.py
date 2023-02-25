"""
Purpose: To save ticket voucher data to database
Owner  : KISHOR PS
Date   : 
Re. F/M:
Last Update: 
"""

import os
import os.path
import time
import json
import datetime

try:
    from pyPgSQL import PgSQL
    str_dba_name = 'pypgsql'
except:
    import psycopg2    
    import psycopg2.extras
    str_dba_name = 'psycopg'

class OperationalError(Exception):
    pass

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
except Exception as msg:
    print ('Connection Error..',msg)
    raise

class captureDB:

    def __init__(self, *args):
        try:
            cr = self.create_cursor()
            cr.execute("""SET datestyle = 'DMY'""")
        except Exception as e:
            ins_general_methods.ins_db.rollback()
            cr.close()
            raise (e)
        else:
            ins_general_methods.ins_db.commit()
            cr.close()
        pass
    
    def create_cursor(self, *args):
        cr = ins_general_methods.ins_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cr
    
    def save_captured_ticket_data(self, lst_ticket_capture_details):
        
        cr = self.create_cursor()
        lst_ticket_details = []
        lst_ticket_nos = []
        lst_fare_data = []
        lst_extra_capturing_fields = []
        lst_ticket_sale_details = []
        lst_ticket_refund_details = []
        lst_credit_card_transaction_data = []
        lst_original_ticket_details = []
        lst_setcor_data = []
        lst_tkt_no = []
        lst_setcor_data = []
        dct_tkts = {}

        flt_total_tickets_fare = 0
        flt_total_tickets_tax = 0

        for ins_ticket_base in lst_ticket_capture_details:
           
            ## Check the tkt is valid integers or not
            try:
                int(ins_ticket_base.str_ticket_number)
            except:
                raise Exception("NOTICKET")
            if ins_ticket_base.str_ticket_number in lst_tkt_no:
                continue

            if ins_ticket_base.str_original_issue :
                int_orig_tkt_seg_count = ins_general_methods.get_original_issue_ticket_segment_count(ins_ticket_base.str_original_issue)
                ins_ticket_base.int_no_of_segments = ins_ticket_base.int_no_of_segments - int_orig_tkt_seg_count
            
            if ins_ticket_base.str_ticketing_agency_iata_no:
                cr.execute("""  SELECT ac.pk_bint_account_id
                                FROM tbl_account  ac 
                                INNER JOIN tbl_partner pt 
                                        ON ac.pk_bint_account_id = pt.fk_bint_partner_account_id
                                        AND pt.chr_document_status = 'N'
                                INNER JOIN tbl_supplier_info si
                                        ON pt.pk_bint_partner_id = si.fk_bint_partner_id
                                WHERE substring(si.vchr_iata_no,1,7) = %s
                                        AND %s = ANY(pt.arr_currency)
                                        AND ac.chr_document_status = 'N' """,
                         
                         (  ins_ticket_base.str_ticketing_agency_iata_no[0:7],
                            ins_ticket_base.str_defult_currency_code))

                rst = cr.fetchone()
                if rst:
                    ins_ticket_base.int_supplier_id = rst['pk_bint_account_id']
#                    if rst['vchr_currency_code'] == ins_ticket_base.str_defult_currency_code :
#                        ins_ticket_base.int_supplier_id = rst['fk_bint_creditor_account_id']
#                        ins_ticket_base.chr_supplier_type = 'S'
#                        pass
#                    else :
#                        ins_ticket_base.int_supplier_id = None
#                        ins_ticket_base.chr_supplier_type = ''

            else:
                if  ins_ticket_base.str_ticketing_agency_iata_no == '' and ins_ticket_base.str_pnr_current_owner_iata_no != '':
                    cr.execute("""SELECT ac.pk_bint_account_id
                                FROM tbl_account  ac 
                                INNER JOIN tbl_partner pt 
                                        ON ac.pk_bint_account_id = pt.fk_bint_partner_account_id
                                        AND pt.chr_document_status = 'N'
                                INNER JOIN tbl_supplier_info si
                                        ON pt.pk_bint_partner_id = si.fk_bint_partner_id
                                WHERE substring(si.vchr_iata_no,1,7) = %s
                                        AND %s = ANY(pt.arr_currency)
                                        AND ac.chr_document_status = 'N'""",
                             (ins_ticket_base.str_pnr_current_owner_iata_no[0:7],
                             ins_ticket_base.str_defult_currency_code))
                    rst = cr.fetchone()
                    if rst:
                        ins_ticket_base.int_supplier_id = rst['pk_bint_account_id']
#                        if rst['vchr_currency_code'] == ins_ticket_base.str_defult_currency_code :
#                            ins_ticket_base.int_supplier_id = rst['fk_bint_creditor_account_id']
#                            ins_ticket_base.chr_supplier_type = 'S'
#                            pass
#                        else :
#                            ins_ticket_base.int_supplier_id = None
#                            ins_ticket_base.chr_supplier_type = ''

            lst_tkt_no.append(ins_ticket_base.str_ticket_number)
            rst = ins_general_methods.get_ticket_details(ins_ticket_base.str_ticket_number)
            
            if rst:
                
                if rst[0]['bln_conjunction_ticket'] :
                    str_main_ticket = ins_general_methods.get_conjuntion_ticket_ticket(ins_ticket_base.str_ticket_number)
                    if str_main_ticket:
                        ins_ticket_base.str_ticket_number = str_main_ticket
                        rst = ins_general_methods.get_ticket_details(ins_ticket_base.str_ticket_number)
                
                lst_original_ticket_details = ins_general_methods.get_original_issue_tickets(rst[0]['vchr_original_issue'] )
                
                if ins_ticket_base.str_void_date == '' and not ins_ticket_base.bln_refund:
                    print('Duplication :', ins_ticket_base.str_ticket_number)
                    continue
                elif ins_ticket_base.bln_refund :
                    if rst[0]['dat_refund'] is not None:
                       continue
                    dat_refund_date = datetime.datetime.strptime(ins_ticket_base.str_refund_date,"%d/%m/%Y").date()
                   
                    if rst[0]['dat_issue'] > dat_refund_date:#// refer 22125
                        dat_refund_date = rst[0]['dat_issue']
                        
                    dat_refund_date = dat_refund_date.strftime("%d/%m/%Y")
                    ins_ticket_base.str_refund_date = dat_refund_date

                    if ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                        ins_ticket_base.flt_supplier_currency_roe_rfd = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_refund_date)

                    ins_ticket_base.int_ticket_id = rst[0]['pk_bint_ticket_id']
                    ins_ticket_base.int_ticket_type_id = rst[0]['fk_bint_ticket_type_id']
                    ins_ticket_base.int_region_id = rst[0]['fk_bint_airports_regions_id']
                    ins_ticket_base.int_airline_id = rst[0]['fk_bint_airline_id']
                    ins_ticket_base.int_airline_account_id = rst[0]['fk_bint_airline_account_id']
                    ins_ticket_base.int_class_id = rst[0]['fk_bint_class_id']
                    ins_ticket_base.str_class_group = rst[0]['vchr_class']
                    ins_ticket_base.str_class = rst[0]['vchr_class_rbd']
                    ins_ticket_base.str_return_class = rst[0]['vchr_return_class_rbd']
                    ins_ticket_base.int_return_class_id = rst[0]['fk_bint_return_class_id']
                    ins_ticket_base.str_return_class_group = rst[0]['vchr_return_class']
                    ins_ticket_base.str_last_conjection_ticket_number = rst[0]['vchr_last_conjunction_ticket_number']
                    ins_ticket_base.str_booking_agent_code = rst[0]['vchr_booking_agent_code']
                    ins_ticket_base.str_booking_agent_numeric_code = rst[0]['vchr_booking_agent_numeric_code']
                    if not ins_ticket_base.int_booking_agent_counter_staff_id:
                        ins_ticket_base.int_booking_agent_counter_staff_id = rst[0]['fk_bint_booking_agent_id']
                    ins_ticket_base.int_card_payment_type_rfd = rst[0]['int_card_payment_type_inv']
                    ins_ticket_base.str_original_issue = rst[0]['vchr_original_issue']

                    if not ins_ticket_base.str_ticketing_agent_code :
                        ins_ticket_base.str_ticketing_agent_code = rst[0]['vchr_ticketing_agent_code_inv']
                        ins_ticket_base.str_ticketing_agent_numeric_code = rst[0]['vchr_ticketing_agent_numeric_code_inv']
                        ##Refer #14726
                        ins_ticket_base.int_counter_staff_id_inv = rst[0]['fk_bint_ticketing_agent_id_inv']
                    
#                    if not ins_ticket_base.str_pax_name :
#                        ins_ticket_base.str_pax_name = rst[0]['vchr_pax_name_inv'] or ''
                    if not ins_ticket_base.str_sector or ins_ticket_base.str_sector == '/':
                        ins_ticket_base.str_sector = rst[0]['vchr_sector_inv'] or ''
                    if not ins_ticket_base.int_no_of_pax_inv :
                        ins_ticket_base.int_no_of_pax_inv = rst[0]['int_no_of_pax_inv'] or 0
                    if not ins_ticket_base.int_no_of_segments :
                        ins_ticket_base.int_no_of_segments = rst[0]['int_no_of_segments_inv'] or 0
#                    if ins_general_methods.ins_auto_inv.bln_auto_refund_all_ticket:
#                        ins_ticket_base.int_account_master_id = rst[0]['fk_bint_customer_account_id_inv']
#                        ins_ticket_base.int_location_id = rst[0]['fk_bint_branch_id_inv']
#                        ins_ticket_base.int_branch_id = rst[0]['fk_bint_department_id_inv']
                        
                    ins_ticket_base.str_base_currency_rfd = ins_ticket_base.str_base_currency
                    ins_ticket_base.str_tran_currency_rfd = ins_ticket_base.str_defult_currency_code
                    ins_ticket_base.str_pax_name_rfd = ins_ticket_base.str_pax_name
                    ins_ticket_base.int_number_of_segments_rfd = ins_ticket_base.int_no_of_segments
                    ins_ticket_base.str_ticketing_agent_code_rfd = ins_ticket_base.str_ticketing_agent_code
                    ins_ticket_base.str_ticketing_agent_numeric_code_rfd = ins_ticket_base.str_ticketing_agent_numeric_code
#                    ins_ticket_base.flt_supplier_currency_roe_rfd = ins_ticket_base.flt_supplier_currency_roe
#                    ins_ticket_base.flt_supplier_currency_roe = 1
                    ins_ticket_base.flt_service_charge_rfd = ins_ticket_base.flt_service_charge
                    ins_ticket_base.flt_service_charge = 0.00
                    ins_ticket_base.flt_service_charge_percentage_rfd = ins_ticket_base.flt_service_charge_percentage_inv #39457
                    ins_ticket_base.flt_service_charge_percentage_inv = 0.00
                    ins_ticket_base.int_company_id_rfd = ins_ticket_base.int_company_id or 1
                    ins_ticket_base.int_company_id = None
                    ins_ticket_base.int_no_of_pax_rfd = ins_ticket_base.int_no_of_pax_inv
                    
                    # Refer 14759
                    ins_ticket_base.flt_discount_given_rfd = ins_ticket_base.flt_discount_given_inv
                    ins_ticket_base.flt_discount_given_inv = 0
                    ins_ticket_base.int_discount_account_id_rfd = ins_ticket_base.int_discount_account_id_inv 
                    ins_ticket_base.int_discount_account_id_inv = None
                    ins_ticket_base.flt_discount_given_percentage_rfd = ins_ticket_base.flt_discount_given_percentage_inv
                    ins_ticket_base.flt_discount_given_percentage_inv = 0
                    
                    ins_ticket_base.int_extra_earning_account_id_rfd = ins_ticket_base.int_extra_earning_account_id_inv
                    ins_ticket_base.int_extra_earning_account_id_inv =  None
                    ins_ticket_base.flt_extra_earninig_percentage_rfd = ins_ticket_base.flt_extra_earninig_percentage_inv
                    ins_ticket_base.flt_extra_earninig_percentage_inv = 0.0
                    ins_ticket_base.flt_extra_earning_rfd = ins_ticket_base.flt_extra_earning_inv
                    ins_ticket_base.flt_extra_earning_inv = 0.0
                    
                    ins_ticket_base.flt_pay_back_commission_rfd = ins_ticket_base.flt_pay_back_commission_inv
                    ins_ticket_base.int_pay_back_account_id_rfd = ins_ticket_base.int_pay_back_account_id_inv
                    ins_ticket_base.flt_pay_back_commission_percentage_rfd = ins_ticket_base.flt_pay_back_commission_percentage_inv
                    ins_ticket_base.int_pay_back_account_id_inv = None
                    ins_ticket_base.flt_pay_back_commission_inv = 0.0
                    ins_ticket_base.flt_pay_back_commission_percentage_inv = None
                    
                    ins_ticket_base.int_supplier_id_rfd = ins_ticket_base.int_supplier_id or rst[0]['fk_bint_supplier_account_id_inv']
                    ins_ticket_base.int_supplier_id = None
                    
                    if not ins_ticket_base.int_account_master_id_rfd and rst[0]['fk_bint_customer_account_id_inv'] :
                        ins_ticket_base.int_account_master_id_rfd = rst[0]['fk_bint_customer_account_id_inv']
                            
                    ins_ticket_base.int_counter_staff_id_rfd = ins_ticket_base.int_counter_staff_id_inv or rst[0]['fk_bint_ticketing_agent_id_inv']
                    
                    if not ins_ticket_base.int_location_id_rfd and rst[0]['fk_bint_branch_id_inv'] :
                        ins_ticket_base.int_location_id_rfd = rst[0]['fk_bint_branch_id_inv']
                    
                    ins_ticket_base.flt_market_fare_card_amount_rfd = ins_ticket_base.flt_market_fare_card_amount_inv
                    ins_ticket_base.flt_market_fare_uccf_amount_rfd = ins_ticket_base.flt_market_fare_uccf_amount_inv
                    ins_ticket_base.flt_tax_card_amount_rfd = ins_ticket_base.flt_tax_card_amount_inv	
                    ins_ticket_base.flt_tax_uccf_amount_rfd = ins_ticket_base.flt_tax_uccf_amount_inv
                    
                    ins_ticket_base.flt_market_fare_card_amount_inv = 0
                    ins_ticket_base.flt_market_fare_uccf_amount_inv = 0
                    ins_ticket_base.flt_tax_card_amount_inv = 0
                    ins_ticket_base.flt_tax_uccf_amount_inv = 0
                    ins_ticket_base.str_pnr_no = rst[0]['vchr_pnr_number']
#                    ins_ticket_base.str_fare_basis = rst[0]['vchr_fare_basis']
#                    ins_ticket_base.flt_sup_charge_card_amount_rfd = 0 	
#                    ins_ticket_base.flt_sup_charge_uccf_amount_rfd = 0


                    self.save_refund_tickets(ins_ticket_base)
                    if lst_original_ticket_details:
                        #36508
                        self.save_refund_void_data_of_original_tickets(lst_original_ticket_details , 
                                            ins_ticket_base.str_refund_date , 
                                            'R',
                                            ins_ticket_base.str_file_name_rfd ,
#                                            ins_ticket_base.str_base_currency_rfd ,
#                                            ins_ticket_base.str_tran_currency_rfd ,
                                            ins_ticket_base.int_account_master_id_rfd ,
                                            ins_ticket_base.int_supplier_id_rfd ,
                                            ins_ticket_base.int_location_id_rfd ,
#                                            ins_ticket_base.int_branch_id ,
                                            ins_ticket_base.str_ticketing_agent_code_rfd ,
                                            ins_ticket_base.str_ticketing_agent_numeric_code_rfd ,
                                            ins_ticket_base.int_counter_staff_id_rfd
                                            
                                            )  # refer bug no:19895
                    return
                else:
                    if rst[0]['dat_refund'] is not None:
                        continue
                    if rst[0]['dat_issue']:
                        dat_ticket_issue = rst[0]['dat_issue'].strftime("%d/%m/%Y")
                        if dat_ticket_issue is not None:
                            if ins_ticket_base.str_void_date: #void date format should be a string as '%d/%m/%Y'
                                dat_void_date = datetime.datetime.strptime(ins_ticket_base.str_void_date,"%d/%m/%Y").date()
                                if rst[0]['dat_issue'] > dat_void_date:#// refer 22125
                                    dat_void_date = rst[0]['dat_issue']
                                    
                                dat_void_date = dat_void_date.strftime("%d/%m/%Y")
                                ins_ticket_base.str_refund_date = dat_void_date
#                                ins_ticket_base.str_ticket_issue_date = dat_void_date
                                ins_ticket_base.chr_ticket_status = 'V'
                                
                            if ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                                ins_ticket_base.flt_supplier_currency_roe_rfd = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_refund_date)

                            #If the reissue ticket is void then it will remove the connection between original ticket and reissue ticket
                            #The status of reissue ticket will change to Void but will not change the status of original ticket
                            #Status of original ticket  will remain in previous status-Bug no:9468
                            if rst[0]['vchr_original_issue']:                                
                                str_orig_tkt_status = ''
                                dat_refund_original_ticket = None
                                cr.execute(""" UPDATE tbl_ticket
                                      SET vchr_original_issue = ''
                                         WHERE vchr_ticket_number = %s""", (ins_ticket_base.str_ticket_number,))

                                


                            ins_ticket_base.int_ticket_id = rst[0]['pk_bint_ticket_id']
                            ins_ticket_base.int_ticket_type_id = rst[0]['fk_bint_ticket_type_id']
                            ins_ticket_base.int_region_id = rst[0]['fk_bint_airports_regions_id']
                            ins_ticket_base.int_airline_id = rst[0]['fk_bint_airline_id']
                            ins_ticket_base.int_airline_account_id = rst[0]['fk_bint_airline_account_id']
                            ins_ticket_base.int_class_id = rst[0]['fk_bint_class_id']
                            ins_ticket_base.str_class_group = rst[0]['vchr_class'] or ''
                            ins_ticket_base.str_class = rst[0]['vchr_class_rbd'] or ''
                            ins_ticket_base.str_return_class = rst[0]['vchr_return_class_rbd'] or ''
                            ins_ticket_base.int_return_class_id = rst[0]['fk_bint_return_class_id']
                            ins_ticket_base.str_return_class_group = rst[0]['vchr_return_class'] or ''
                            ins_ticket_base.str_last_conjection_ticket_number = rst[0]['vchr_last_conjunction_ticket_number']
                            ins_ticket_base.str_booking_agent_code = rst[0]['vchr_booking_agent_code']
                            ins_ticket_base.str_booking_agent_numeric_code = rst[0]['vchr_booking_agent_numeric_code']
                            if not ins_ticket_base.int_booking_agent_counter_staff_id:
                                ins_ticket_base.int_booking_agent_counter_staff_id = rst[0]['fk_bint_booking_agent_id']
                            ins_ticket_base.int_card_payment_type_rfd = rst[0]['int_card_payment_type_inv']
                            ins_ticket_base.str_original_issue = rst[0]['vchr_original_issue']

                            if not ins_ticket_base.str_ticketing_agent_code :
                                ins_ticket_base.str_ticketing_agent_code = rst[0]['vchr_ticketing_agent_code_inv']
                                ins_ticket_base.str_ticketing_agent_numeric_code = rst[0]['vchr_ticketing_agent_numeric_code_inv']
                                ins_ticket_base.int_counter_staff_id_inv = rst[0]['fk_bint_ticketing_agent_id_inv']

#                            if not ins_ticket_base.str_pax_name :
#                                ins_ticket_base.str_pax_name = rst[0]['vchr_pax_name_inv'] or ''

                            if not ins_ticket_base.str_sector or ins_ticket_base.str_sector == '/':
                                ins_ticket_base.str_sector = rst[0]['vchr_sector_inv'] or ''
#                            if not ins_ticket_base.int_no_of_pax_inv :
#                                ins_ticket_base.int_no_of_pax_inv = rst[0]['int_no_of_pax_inv'] or 0
                            if not ins_ticket_base.int_no_of_segments :
                                ins_ticket_base.int_no_of_segments = rst[0]['int_no_of_segments_inv'] or 0

                            ins_ticket_base.str_base_currency_rfd = ins_ticket_base.str_base_currency
#                            ins_ticket_base.str_tran_currency_rfd = rst[0]['vchr_tran_currency_inv'] #36583
                            ins_ticket_base.str_pax_name_rfd = ins_ticket_base.str_pax_name or rst[0]['vchr_pax_name']
                            ins_ticket_base.int_number_of_segments_rfd = ins_ticket_base.int_no_of_segments
                            ins_ticket_base.str_ticketing_agent_code_rfd = ins_ticket_base.str_ticketing_agent_code
                            ins_ticket_base.str_ticketing_agent_numeric_code_rfd = ins_ticket_base.str_ticketing_agent_numeric_code
                            ins_ticket_base.flt_supplier_currency_roe_rfd = ins_ticket_base.flt_supplier_currency_roe
#                            ins_ticket_base.flt_supplier_currency_roe = 1   # need to check
                            ins_ticket_base.flt_service_charge_rfd = ins_ticket_base.flt_service_charge
                            ins_ticket_base.flt_service_charge = 0.00
                            ins_ticket_base.flt_service_charge_percentage_rfd = ins_ticket_base.flt_service_charge_percentage_inv
                            ins_ticket_base.flt_service_charge_percentage_inv = 0.00
                            ins_ticket_base.int_company_id_rfd = ins_ticket_base.int_company_id or 1
                            ins_ticket_base.int_company_id = None
                            ins_ticket_base.int_no_of_pax_rfd = ins_ticket_base.int_no_of_pax_inv

                            ins_ticket_base.int_supplier_id_rfd = ins_ticket_base.int_supplier_id
                            ins_ticket_base.int_supplier_id = None
                            if not ins_ticket_base.int_account_master_id_rfd: #37364
                                ins_ticket_base.int_account_master_id_rfd = rst[0]['fk_bint_customer_account_id_inv'] 
                            if not ins_ticket_base.int_location_id_rfd :
                                ins_ticket_base.int_location_id_rfd = rst[0]['fk_bint_branch_id_inv']
                            ins_ticket_base.flt_discount_given_rfd = ins_ticket_base.flt_discount_given_inv
                            ins_ticket_base.flt_discount_given_inv = 0
                            ins_ticket_base.int_discount_account_id_rfd = ins_ticket_base.int_discount_account_id_inv 
                            ins_ticket_base.int_discount_account_id_inv = None
                            ins_ticket_base.flt_discount_given_percentage_rfd = ins_ticket_base.flt_discount_given_percentage_inv
                            ins_ticket_base.flt_discount_given_percentage_inv = 0

                            ins_ticket_base.int_extra_earning_account_id_rfd = ins_ticket_base.int_extra_earning_account_id_inv
                            ins_ticket_base.int_extra_earning_account_id_inv =  None
                            ins_ticket_base.flt_extra_earninig_percentage_rfd = ins_ticket_base.flt_extra_earninig_percentage_inv
                            ins_ticket_base.flt_extra_earninig_percentage_inv = 0.0
                            ins_ticket_base.flt_extra_earning_rfd = ins_ticket_base.flt_extra_earning_inv
                            ins_ticket_base.flt_extra_earning_inv = 0.0
                            
                            ins_ticket_base.flt_pay_back_commission_rfd = ins_ticket_base.flt_pay_back_commission_inv
                            ins_ticket_base.int_pay_back_account_id_rfd = ins_ticket_base.int_pay_back_account_id_inv
                            ins_ticket_base.flt_pay_back_commission_percentage_rfd = ins_ticket_base.flt_pay_back_commission_percentage_inv
                            ins_ticket_base.int_pay_back_account_id_inv = None
                            ins_ticket_base.flt_pay_back_commission_inv = 0.0
                            ins_ticket_base.flt_pay_back_commission_percentage_inv = None
                            
                            ins_ticket_base.int_counter_staff_id_rfd = ins_ticket_base.int_counter_staff_id_inv or rst[0]['fk_bint_ticketing_agent_id_inv']
                            ins_ticket_base.int_corporate_card_id_rfd = rst[0]['fk_bint_corporate_card_account_id_inv']
#                            ins_ticket_base.flt_market_fare_card_amount_rfd = ins_ticket_base.flt_market_fare_card_amount_inv
#                            ins_ticket_base.flt_market_fare_uccf_amount_rfd = ins_ticket_base.flt_market_fare_uccf_amount_inv
#                            ins_ticket_base.flt_tax_card_amount_rfd = ins_ticket_base.flt_tax_card_amount_inv	
#                            ins_ticket_base.flt_tax_uccf_amount_rfd = ins_ticket_base.flt_tax_uccf_amount_inv

#                            ins_ticket_base.flt_market_fare_card_amount_inv = 0
#                            ins_ticket_base.flt_market_fare_uccf_amount_inv = 0
#                            ins_ticket_base.flt_tax_card_amount_inv = 0
#                            ins_ticket_base.flt_tax_uccf_amount_inv = 0
                            
                            self.save_void_ticket(ins_ticket_base)

#                            self.update_refund_sides_when_ticket_void(ins_ticket_base.str_ticket_number)
                            
                            #No need to update the status of original ticket if the reissue ticket is void-Bug No:9468
#                            if lst_original_ticket_details:
#                            self.save_refund_void_data_of_original_tickets(lst_original_ticket_details , ins_ticket_base.str_refund_date ,str_original_ticket_status)
                            print("VOID Ticket : ", ins_ticket_base.str_ticket_number)
                            cr.close()
                            return
            elif ins_ticket_base.bln_refund:
#                if ins_ticket_base.str_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    print(("Refund Date :",ins_ticket_base.str_refund_date.strip()))

                    dat_refund_date = datetime.datetime.strptime(ins_ticket_base.str_refund_date,"%d/%m/%Y").date()
                    dat_refund_date = dat_refund_date.strftime("%d/%m/%Y")
                    ins_ticket_base.str_refund_date = dat_refund_date
                    
                    if ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                                ins_ticket_base.flt_supplier_currency_roe_rfd = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_refund_date)
                    
                    ins_ticket_base.str_ticket_issue_date = None
                    ins_ticket_base.chr_ticket_status = 'R'
                    ins_ticket_base.chr_bsp_ticket_status = 'R'
                    ins_ticket_base.chr_system_sale_part_action = ''
                    ins_ticket_base.chr_system_refund_part_action = 'I'
                    ins_ticket_base.str_ticketing_agent_code_rfd = ins_ticket_base.str_ticketing_agent_code
                    ins_ticket_base.str_ticketing_agent_numeric_code_rfd = ins_ticket_base.str_ticketing_agent_numeric_code
#                    ins_ticket_base.str_ticketing_agent_code = ''
#                    ins_ticket_base.str_ticketing_agent_numeric_code = ''
                    ins_ticket_base.int_number_of_segments_rfd = ins_ticket_base.int_no_of_segments
                    ins_ticket_base.int_no_of_segments = 0
                    ins_ticket_base.str_pax_name_rfd = ins_ticket_base.str_pax_name
                    ins_ticket_base.str_pax_name = ''
                    ins_ticket_base.str_base_currency_rfd = ins_ticket_base.str_base_currency
                    ins_ticket_base.str_tran_currency_rfd = ins_ticket_base.str_defult_currency_code
#                    ins_ticket_base.str_base_currency = ''
#                    ins_ticket_base.str_defult_currency_code = ''
                    ins_ticket_base.int_no_of_pax_rfd = ins_ticket_base.int_no_of_pax_inv
                    ins_ticket_base.int_no_of_pax_inv = 0
                    if not ins_ticket_base.flt_std_commn_percentage_rfd :
                        ins_ticket_base.flt_std_commn_percentage_rfd = ins_ticket_base.flt_std_commn_percentage_inv
                    ins_ticket_base.flt_std_commn_percentage_inv = 0.00
#                    ins_ticket_base.flt_supplier_currency_roe_rfd = ins_ticket_base.flt_supplier_currency_roe
                    ins_ticket_base.flt_supplier_currency_roe = 1
                    ins_ticket_base.flt_service_charge_rfd = ins_ticket_base.flt_service_charge
                    ins_ticket_base.flt_service_charge = 0.00
                    ins_ticket_base.flt_service_charge_percentage_rfd = ins_ticket_base.flt_service_charge_percentage_inv
                    ins_ticket_base.flt_service_charge_percentage_inv = 0.00
                    ins_ticket_base.int_company_id_rfd = ins_ticket_base.int_company_id or 1
                    ins_ticket_base.int_company_id = None
                    
                    ins_ticket_base.flt_discount_given_rfd = ins_ticket_base.flt_discount_given_inv
                    ins_ticket_base.flt_discount_given_inv = 0
                    ins_ticket_base.int_discount_account_id_rfd = ins_ticket_base.int_discount_account_id_inv 
                    ins_ticket_base.int_discount_account_id_inv = None
                    ins_ticket_base.flt_discount_given_percentage_rfd = ins_ticket_base.flt_discount_given_percentage_inv
                    ins_ticket_base.flt_discount_given_percentage_inv = 0
                    
                    ins_ticket_base.int_extra_earning_account_id_rfd = ins_ticket_base.int_extra_earning_account_id_inv
                    ins_ticket_base.int_extra_earning_account_id_inv =  None
                    ins_ticket_base.flt_extra_earninig_percentage_rfd = ins_ticket_base.flt_extra_earninig_percentage_inv
                    ins_ticket_base.flt_extra_earninig_percentage_inv = 0.0
                    ins_ticket_base.flt_extra_earning_rfd = ins_ticket_base.flt_extra_earning_inv
                    ins_ticket_base.flt_extra_earning_inv = 0.0
                    ins_ticket_base.int_supplier_id_rfd = ins_ticket_base.int_supplier_id
                    ins_ticket_base.int_supplier_id = None
                    
                    ins_ticket_base.flt_pay_back_commission_rfd = ins_ticket_base.flt_pay_back_commission_inv
                    ins_ticket_base.int_pay_back_account_id_rfd = ins_ticket_base.int_pay_back_account_id_inv
                    ins_ticket_base.flt_pay_back_commission_percentage_rfd = ins_ticket_base.flt_pay_back_commission_percentage_inv
                    ins_ticket_base.int_pay_back_account_id_inv = None
                    ins_ticket_base.flt_pay_back_commission_inv = 0.0
                    ins_ticket_base.flt_pay_back_commission_percentage_inv = None
                    
                    ins_ticket_base.int_counter_staff_id_rfd = ins_ticket_base.int_counter_staff_id_inv
                    ins_ticket_base.int_counter_staff_id_inv = None
                    
                    ins_ticket_base.int_account_master_id_rfd = ins_ticket_base.int_account_master_id
                    ins_ticket_base.int_account_master_id = None
                    
                    ins_ticket_base.int_location_id_rfd = ins_ticket_base.int_location_id
                    ins_ticket_base.int_location_id = None
                    
                    ins_ticket_base.flt_market_fare_card_amount_rfd = ins_ticket_base.flt_market_fare_card_amount_inv
                    ins_ticket_base.flt_market_fare_uccf_amount_rfd = ins_ticket_base.flt_market_fare_uccf_amount_inv
                    ins_ticket_base.flt_tax_card_amount_rfd = ins_ticket_base.flt_tax_card_amount_inv	
                    ins_ticket_base.flt_tax_uccf_amount_rfd = ins_ticket_base.flt_tax_uccf_amount_inv
                    
                    ins_ticket_base.flt_market_fare_card_amount_inv = 0
                    ins_ticket_base.flt_market_fare_uccf_amount_inv = 0
                    ins_ticket_base.flt_tax_card_amount_inv = 0
                    ins_ticket_base.flt_tax_uccf_amount_inv = 0
                    
                    ins_ticket_base.int_card_payment_type_rfd = ins_ticket_base.int_card_payment_type
                    ins_ticket_base.int_card_payment_type = 0

                    lst_original_ticket_details = ins_general_methods.get_original_issue_tickets(ins_ticket_base.str_original_issue )
                    if lst_original_ticket_details:
                        #36508
                        self.save_refund_void_data_of_original_tickets(lst_original_ticket_details ,  
                                            ins_ticket_base.str_refund_date , 
                                            'R',
                                            ins_ticket_base.str_file_name_rfd ,
#                                            ins_ticket_base.str_base_currency_rfd , currencies are removed in saas tbl_ticket
#                                            ins_ticket_base.str_tran_currency_rfd ,
                                            ins_ticket_base.int_account_master_id ,
                                            ins_ticket_base.int_supplier_id_rfd ,
                                            ins_ticket_base.int_location_id_rfd ,
#                                            ins_ticket_base.int_branch_id ,
                                            ins_ticket_base.str_ticketing_agent_code_rfd ,
                                            ins_ticket_base.str_ticketing_agent_numeric_code_rfd ,
                                            ins_ticket_base.int_counter_staff_id_rfd
                                            )  # refer bug no:19895
                    print(('!!! Add New Row in Ticket - Refund - ', ins_ticket_base.str_ticket_number))
                    
#                else:
#                    cr.close()
#                    ins_general_methods.ins_global.dct_not_parsed_files[ins_ticket_base.str_file_name] = None
#                    raise OperationalError('@@@@@@@@@@ Refund : %s'%ins_ticket_base.str_ticket_number)
            
            elif ins_ticket_base.str_void_date != '':
#                if ins_ticket_base.str_file_name in ins_general_methods.ins_global.dct_not_parsed_files:
                    ins_ticket_base.str_refund_date = ins_ticket_base.str_void_date
                    
                    if ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                                ins_ticket_base.flt_supplier_currency_roe_rfd = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_refund_date)
                    
                    ins_ticket_base.str_ticket_issue_date = ins_ticket_base.str_void_date
                    ins_ticket_base.chr_ticket_status = 'V'
                    ins_ticket_base.chr_bsp_ticket_status = 'V'
                    ins_ticket_base.chr_system_sale_part_action = ''
                    ins_ticket_base.chr_system_refund_part_action = 'I'
                    ins_ticket_base.str_ticketing_agent_code_rfd = ins_ticket_base.str_ticketing_agent_code
                    ins_ticket_base.str_ticketing_agent_numeric_code_rfd = ins_ticket_base.str_ticketing_agent_numeric_code
#                    ins_ticket_base.str_ticketing_agent_code = ''
#                    ins_ticket_base.str_ticketing_agent_numeric_code = ''
                    ins_ticket_base.int_number_of_segments_rfd = ins_ticket_base.int_no_of_segments
                    ins_ticket_base.int_no_of_segments = 0
                    ins_ticket_base.str_pax_name_rfd = ins_ticket_base.str_pax_name
                    ins_ticket_base.str_pax_name = ''
                    ins_ticket_base.str_base_currency_rfd = ins_ticket_base.str_base_currency
                    ins_ticket_base.str_tran_currency_rfd = ins_ticket_base.str_defult_currency_code
#                    ins_ticket_base.str_base_currency = ''
#                    ins_ticket_base.str_defult_currency_code = ''
                    ins_ticket_base.int_no_of_pax_rfd = ins_ticket_base.int_no_of_pax_inv
                    ins_ticket_base.int_no_of_pax_inv = 0
                    if not ins_ticket_base.flt_std_commn_percentage_rfd :
                        ins_ticket_base.flt_std_commn_percentage_rfd = ins_ticket_base.flt_std_commn_percentage_inv
                    ins_ticket_base.flt_std_commn_percentage_inv = 0.00
#                    ins_ticket_base.flt_supplier_currency_roe_rfd = ins_ticket_base.flt_supplier_currency_roe
                    ins_ticket_base.flt_supplier_currency_roe = 1
                    ins_ticket_base.flt_service_charge_rfd = ins_ticket_base.flt_service_charge
                    ins_ticket_base.flt_service_charge = 0.00
                    ins_ticket_base.flt_service_charge_percentage_rfd = ins_ticket_base.flt_service_charge_percentage_inv
                    ins_ticket_base.flt_service_charge_percentage_inv = 0.00
                    ins_ticket_base.int_company_id_rfd = ins_ticket_base.int_company_id or 1
                    ins_ticket_base.int_company_id = None
                    ins_ticket_base.flt_discount_given_rfd = ins_ticket_base.flt_discount_given_inv
                    ins_ticket_base.flt_discount_given_inv = 0
                    
                    ins_ticket_base.int_discount_account_id_rfd = ins_ticket_base.int_discount_account_id_inv 
                    ins_ticket_base.int_discount_account_id_inv = None
                    ins_ticket_base.flt_discount_given_percentage_rfd = ins_ticket_base.flt_discount_given_percentage_inv
                    ins_ticket_base.flt_discount_given_percentage_inv = 0
                    
                    ins_ticket_base.int_extra_earning_account_id_rfd = ins_ticket_base.int_extra_earning_account_id_inv
                    ins_ticket_base.int_extra_earning_account_id_inv =  None
                    ins_ticket_base.flt_extra_earninig_percentage_rfd = ins_ticket_base.flt_extra_earninig_percentage_inv
                    ins_ticket_base.flt_extra_earninig_percentage_inv = 0.0
                    ins_ticket_base.flt_extra_earning_rfd = ins_ticket_base.flt_extra_earning_inv
                    ins_ticket_base.flt_extra_earning_inv = 0.0

                    ins_ticket_base.int_supplier_id_rfd = ins_ticket_base.int_supplier_id
                    ins_ticket_base.int_supplier_id = None
                    
                    ins_ticket_base.flt_pay_back_commission_rfd = ins_ticket_base.flt_pay_back_commission_inv
                    ins_ticket_base.int_pay_back_account_id_rfd = ins_ticket_base.int_pay_back_account_id_inv
                    ins_ticket_base.flt_pay_back_commission_percentage_rfd = ins_ticket_base.flt_pay_back_commission_percentage_inv
                    ins_ticket_base.int_pay_back_account_id_inv = None
                    ins_ticket_base.flt_pay_back_commission_inv = 0.0
                    ins_ticket_base.flt_pay_back_commission_percentage_inv = None
                    
                    ins_ticket_base.int_counter_staff_id_rfd = ins_ticket_base.int_counter_staff_id_inv
                    ins_ticket_base.int_counter_staff_id_inv = None
                    
                    ins_ticket_base.int_account_master_id_rfd = ins_ticket_base.int_account_master_id
                    ins_ticket_base.int_account_master_id = None
                    
                    ins_ticket_base.int_location_id_rfd = ins_ticket_base.int_location_id
                    ins_ticket_base.int_location_id = None
                    
                    ins_ticket_base.flt_market_fare_card_amount_rfd = ins_ticket_base.flt_market_fare_card_amount_inv
                    ins_ticket_base.flt_market_fare_uccf_amount_rfd = ins_ticket_base.flt_market_fare_uccf_amount_inv
                    ins_ticket_base.flt_tax_card_amount_rfd = ins_ticket_base.flt_tax_card_amount_inv	
                    ins_ticket_base.flt_tax_uccf_amount_rfd = ins_ticket_base.flt_tax_uccf_amount_inv
                    
                    ins_ticket_base.flt_market_fare_card_amount_inv = 0
                    ins_ticket_base.flt_market_fare_uccf_amount_inv = 0
                    ins_ticket_base.flt_tax_card_amount_inv = 0
                    ins_ticket_base.flt_tax_uccf_amount_inv = 0
                    
                    ins_ticket_base.int_card_payment_type_rfd = ins_ticket_base.int_card_payment_type
                    ins_ticket_base.int_card_payment_type = 0

                    lst_original_ticket_details = ins_general_methods.get_original_issue_tickets(ins_ticket_base.str_original_issue)
                    if lst_original_ticket_details:
                        #36508
                        self.save_refund_void_data_of_original_tickets(lst_original_ticket_details , 
                                            ins_ticket_base.str_refund_date , 
                                            'V',
                                            ins_ticket_base.str_file_name_rfd ,
#                                            ins_ticket_base.str_base_currency_rfd ,
#                                            ins_ticket_base.str_tran_currency_rfd ,
                                            ins_ticket_base.int_account_master_id ,
                                            ins_ticket_base.int_supplier_id_rfd ,
                                            ins_ticket_base.int_location_id_rfd ,
#                                            ins_ticket_base.int_branch_id ,
                                            ins_ticket_base.str_ticketing_agent_code_rfd ,
                                            ins_ticket_base.str_ticketing_agent_numeric_code_rfd ,
                                            ins_ticket_base.int_counter_staff_id_rfd
                                            )  # refer bug no:19895
                    print(('!!! Add New Row in Ticket - Void - ', ins_ticket_base.str_ticket_number))
#                else:
#                    cr.close()
#                    raise OperationalError('Void Ticket')

                    
            if ins_ticket_base.str_ticket_issue_date and ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                ins_ticket_base.flt_supplier_currency_roe = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_ticket_issue_date)

         # -----------------------------------------------------------------
            
            # reissue ticket fare calculations
            if (ins_ticket_base.str_original_issue or \
                (ins_ticket_base.str_pricing_code in ('B','G','F','M') \
                and ins_ticket_base.bln_k_section_fare and \
                (ins_ticket_base.str_hand_ticket_number != '' or ins_ticket_base.bln_reissue_fop)
                )) \
                and not ins_ticket_base.bln_refund:
                # reissue fare is calculated by subtracting tax from collection amount.
                
                
                ### 01/08/2016 There is some problems with the below logic

                # // Since Tax balance and fare balance from ATC section was not reliable,
                # // Decided to Take Total amount from 7th Position ( TST Total ) and Subtract tax amount To get fare
                # // After discussion with Mohan Das Sir Refer Bug #9875
                
                
                ################## So changed it as below
                
                """ If there is value in either K section or in KS section then that value will be used instead of 
                    ATC section IT total. 
                    
                    refer 22003
                    """
                    
                if ins_ticket_base.bln_k_section_fare or ins_ticket_base.flt_total_amount_collected:
                    ins_ticket_base.flt_published_fare_inv = float(ins_ticket_base.flt_total_amount_collected)- ins_ticket_base.flt_total_tax_inv
                    ins_ticket_base.flt_market_fare_inv = ins_ticket_base.flt_published_fare_inv

                elif ins_ticket_base.str_it_total:
                    ins_ticket_base.flt_published_fare_inv = ins_ticket_base.flt_it_total - ins_ticket_base.flt_total_tax_inv
                    ins_ticket_base.flt_market_fare_inv = ins_ticket_base.flt_published_fare_inv
                    pass
                
                if ins_ticket_base.str_crs_company == 'Sabre':
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
                        
                if ins_ticket_base.flt_published_fare_inv < 0:
                    ins_ticket_base.flt_published_fare_inv = 0

                if ins_ticket_base.flt_market_fare_inv < 0:
                    ins_ticket_base.flt_market_fare_inv = 0

                if ins_ticket_base.flt_total_tax_inv < 0:
                    ins_ticket_base.flt_total_tax_inv = 0

                if ins_ticket_base.str_card_approval_code or (ins_ticket_base.str_cc_card_no.strip() and len(ins_ticket_base.str_cc_card_no) > 10 ):
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
                # if partial uccf is true , that means they have paid the amount by both card and cash.
                if ins_ticket_base.bln_partial_uccf:
                    int_currency_precision = ins_general_methods.get_rounding_value(ins_ticket_base.str_defult_currency_code)
                    if ins_ticket_base.flt_market_fare_inv > ins_ticket_base.flt_uccf_amount:
                        ins_ticket_base.flt_market_fare_credit_inv = ins_ticket_base.flt_uccf_amount
                        ins_ticket_base.flt_market_fare_cash_inv = round(ins_ticket_base.flt_market_fare_inv - ins_ticket_base.flt_uccf_amount,int_currency_precision)

                    else:
                        ins_ticket_base.flt_market_fare_credit_inv = ins_ticket_base.flt_market_fare_inv
                        ins_ticket_base.flt_market_fare_cash_inv = 0.0
                        flt_uccf_rem_amount = round(ins_ticket_base.flt_uccf_amount - ins_ticket_base.flt_market_fare_inv,int_currency_precision)

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



            if not ins_ticket_base.flt_published_fare_inv :
                ins_ticket_base.flt_published_fare_inv = ins_ticket_base.flt_market_fare_inv
                
            if ins_ticket_base.flt_published_fare_ext : ## 22431 
                ins_ticket_base.flt_published_fare_inv = ins_ticket_base.flt_published_fare_ext
            # refer 28892   
            if not ins_ticket_base.flt_published_fare_inv and ins_ticket_base.str_currency_type_code not in (ins_ticket_base.str_defult_currency_code,)\
                    and ins_ticket_base.flt_total_amount_collected and (not ins_general_methods.ins_capture_base.bln_multi_currency):
                    ins_ticket_base.flt_published_fare_inv = float(ins_ticket_base.flt_total_amount_collected) -ins_ticket_base.flt_total_tax_inv
                    ins_ticket_base.flt_market_fare_inv = float(ins_ticket_base.flt_total_amount_collected) -ins_ticket_base.flt_total_tax_inv
            #if ins_ticket_base.flt_special_fare_inv == 0.00 :
            ins_ticket_base.flt_special_fare_inv = ins_ticket_base.flt_market_fare_inv

            # For calculating service fee from collection amount
            flt_total_tickets_fare = float(flt_total_tickets_fare) + float(ins_ticket_base.flt_market_fare_inv)
            flt_total_tickets_tax = float(flt_total_tickets_tax) + float(ins_ticket_base.flt_total_tax_inv)
            
            if ins_ticket_base.flt_selling_price_ext and ins_ticket_base.str_original_issue and ins_general_methods.str_reverse_calculate_dis_extra_amt:
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

            if ins_ticket_base.str_ticket_type and not ins_ticket_base.int_ticket_type_id :
                ins_ticket_base.int_ticket_type_id = ins_general_methods.get_ticket_type_id(ins_ticket_base.str_ticket_type)
            ins_ticket_base.int_region_id = ins_general_methods.get_region_id(ins_ticket_base.str_region_code)
            if ins_ticket_base.str_class :
                ins_ticket_base.int_class_id,ins_ticket_base.str_class_group = ins_general_methods.get_class_id('%' + ins_ticket_base.str_class.split('/')[0] + '%',ins_ticket_base.int_airline_account_id)
                if ins_ticket_base.str_return_date :
                    ins_ticket_base.str_return_class = ins_ticket_base.str_class.split('/')[-1]
                    
                ins_ticket_base.str_class = ins_ticket_base.str_class.split('/')[0]
                
            if ins_ticket_base.str_return_class :
                ins_ticket_base.int_return_class_id,ins_ticket_base.str_return_class_group = ins_general_methods.get_class_id('%' + ins_ticket_base.str_return_class + '%',ins_ticket_base.int_airline_account_id)
            else :
                ins_ticket_base.int_return_class_id = None
            
            
            #calculate std commision amount    STD_comm_amount_inv = (MF_inv * STD_perc / 100)
            if ins_ticket_base.flt_standard_commission_captured: #ref 19763  
                ins_ticket_base.flt_standard_commission = ins_ticket_base.flt_standard_commission_captured
                if  ins_ticket_base.flt_market_fare_inv:# STD_perc = (STD_comm_amount_inv/MF_inv)*100
                    ins_ticket_base.flt_std_commn_percentage_inv = (ins_ticket_base.flt_standard_commission/ins_ticket_base.flt_market_fare_inv)*100
                    
                elif ins_ticket_base.flt_published_fare_inv :
                    ins_ticket_base.flt_std_commn_percentage_inv = (ins_ticket_base.flt_standard_commission/ins_ticket_base.flt_published_fare_inv)*100
                    
                else:
                    ins_ticket_base.flt_std_commn_percentage_inv = 0.0
                    ins_ticket_base.flt_standard_commission = 0.0
            else:
                ins_ticket_base.flt_standard_commission = ins_ticket_base.flt_market_fare_inv * ins_ticket_base.flt_std_commn_percentage_inv/100
                ins_ticket_base.flt_standard_commission_rfd = ins_ticket_base.flt_market_fare_rfd * ins_ticket_base.flt_std_commn_percentage_rfd/100
            
            #ST 1987
            if ins_general_methods.get_admin_settings("STR_AIT_TAX_FIELD_IN_INVOICE_AND_REFUND"):
                ins_ticket_base.flt_standard_commission = round(ins_ticket_base.flt_standard_commission)
                ins_ticket_base.flt_standard_commission_rfd = round(ins_ticket_base.flt_standard_commission_rfd)
            #// Supll fee and iata insu not capturing now

            ins_ticket_base.flt_supplier_amount = ins_ticket_base.flt_market_fare_inv + ins_ticket_base.flt_total_tax_inv - ins_ticket_base.flt_standard_commission
            ins_ticket_base.flt_supplier_amount_rfd = ins_ticket_base.flt_market_fare_rfd + ins_ticket_base.flt_total_tax_rfd  - ins_ticket_base.flt_standard_commission_rfd 

#            net_payable_inv = (MF_credit_inv + tax_credit_inv + supplier_fee_inv + iata_insurance_payable_inv) - STD_comm_amount_inv

            ins_ticket_base.flt_net_payable_inv = ins_ticket_base.flt_market_fare_cash_inv + ins_ticket_base.flt_total_tax_cash_inv - ins_ticket_base.flt_standard_commission
            ins_ticket_base.flt_net_payable_rfd = ins_ticket_base.flt_market_fare_cash_rfd + ins_ticket_base.flt_total_tax_cash_rfd - ins_ticket_base.flt_standard_commission_rfd 

#            selling_price_inv_a = special_fare_inv + tax_inv + iata_insurance_collected_inv + service_fee_inv + extra_earning_inv 
#							+ payback_service_fee_inv + billing_tax_inv - discount_given_inv + cc_charge

            if ins_ticket_base.flt_total_tax_inv == 0:
                ins_ticket_base.flt_vat_in_inv = 0.00
                ins_ticket_base.str_tax_details = '' #36273
                
            if ins_ticket_base.flt_total_tax_rfd == 0:
                ins_ticket_base.flt_vat_in_rfd = 0.00
                
            if ins_ticket_base.str_region_code != 'DOM' and ins_general_methods.bln_capture_input_vat_only_for_dom_tickets:
                ins_ticket_base.flt_vat_in_inv = 0.00
                ins_ticket_base.flt_vat_in_rfd = 0.00
            # refer 35652
            ins_ticket_base.flt_selling_price = ins_ticket_base.flt_market_fare_inv + ins_ticket_base.flt_total_tax_inv +\
                                            ins_ticket_base.flt_service_charge + ins_ticket_base.flt_pay_back_commission_inv -\
                                            ins_ticket_base.flt_discount_given_inv + ins_ticket_base.flt_extra_earning_inv + \
                                            ins_ticket_base.flt_cc_charge_collected_ext - ins_ticket_base.flt_vat_in_inv
                                                
            ins_ticket_base.flt_selling_price_rfd = ins_ticket_base.flt_market_fare_rfd + ins_ticket_base.flt_total_tax_rfd +\
                                                    ins_ticket_base.flt_service_charge_rfd + ins_ticket_base.flt_pay_back_commission_rfd -\
                                                    ins_ticket_base.flt_discount_given_rfd + ins_ticket_base.flt_extra_earning_rfd - ins_ticket_base.flt_vat_in_rfd 

#            actual_cost_inv = ((MF_inv + tax_inv + supplier_fee_inv + iata_insurance_payable_inv) 
#								- STD_comm_amount_inv - fare_differece_inv - GLPO_commission_inv)

            ins_ticket_base.flt_actual_cost_inv = ins_ticket_base.flt_market_fare_inv + ins_ticket_base.flt_total_tax_inv \
                                                            - ins_ticket_base.flt_standard_commission


#            debited_amount_inv = selling_price_inv_a - MF_credit_card_inv - tax_credit_card_inv ==>>(dbl_base_currency_debited_amount_inv)
#                
#            inv_debited_amount_rfd =  selling_price_rfd - MF_credit_card_rfd - tax_credit_card_rfd ==>>(dbl_base_currency_inv_debited_amount_rfd)
#            credited_amount_rfd = inv_debited_amount_rfd - client_charge_rfd (dbl_base_currency_credited_amount_rfd)
#            debited_amount_rfd = inv_net_payable_rfd - sup_charge_credit_rfd ==>> (dbl_base_currency_debited_amount_rfd)
#                    
#            supplier_net_rfd = gross_payable_rfd - supplier_charge_rfd (dbl_base_currency_supplier_net_rfd)
#            client_net_rfd = selling_price_rfd - client_charge_rfd (dbl_base_currency_client_net_rfd)

            ins_ticket_base.flt_debited_amount_inv = ins_ticket_base.flt_selling_price - ins_ticket_base.flt_market_fare_credit_inv - ins_ticket_base.flt_total_tax_credit_inv
            ins_ticket_base.flt_debited_amount_rfd = ins_ticket_base.flt_net_payable_rfd - ins_ticket_base.flt_supplier_refund_charge_cash
            ins_ticket_base.flt_inv_debited_amount_rfd = ins_ticket_base.flt_selling_price_rfd - ins_ticket_base.flt_market_fare_credit_rfd - ins_ticket_base.flt_total_tax_credit_rfd

            ins_ticket_base.flt_credited_amount_rfd = ins_ticket_base.flt_inv_debited_amount_rfd - ins_ticket_base.flt_client_refund_charge


            ins_ticket_base.flt_supplier_refund_net = ins_ticket_base.flt_supplier_amount_rfd - ins_ticket_base.flt_supplier_refund_charge
            ins_ticket_base.flt_client_refund_net = ins_ticket_base.flt_selling_price_rfd - ins_ticket_base.flt_client_refund_charge



#            profit_inv = (service_fee_inv + extra_earning_inv + iata_insurance_collected_inv + STD_comm_amount_inv) 
#						- (iata_insurance_payable_inv + discount_given_inv)

            if ins_general_methods.bln_set_cc_chrge_as_profit :
                ins_ticket_base.flt_profit_inv = ins_ticket_base.flt_service_charge + ins_ticket_base.flt_standard_commission -\
                                            ins_ticket_base.flt_discount_given_inv + ins_ticket_base.flt_extra_earning_inv + ins_ticket_base.flt_cc_charge_collected_ext
            else :
                ins_ticket_base.flt_profit_inv = ins_ticket_base.flt_service_charge + ins_ticket_base.flt_standard_commission -\
                                            ins_ticket_base.flt_discount_given_inv + ins_ticket_base.flt_extra_earning_inv


#             profit_rfd = client_charge_rfd - sup_charge_credit_rfd

            ins_ticket_base.flt_profit_rfd = ins_ticket_base.flt_client_refund_charge - ins_ticket_base.flt_supplier_refund_charge_cash


            
            flt_uccf_amount = 0
            flt_cor_card_amount = 0
            int_card_type = 0
            bln_cust_card = False
            bln_agency_card = False
        
            for tpl_card_data in ins_ticket_base.lst_card_data :
                if len(ins_ticket_base.lst_card_data) == 1 :
                    if ins_ticket_base.int_card_payment_type == 1  :
                        flt_uccf_amount += ins_ticket_base.flt_market_fare_credit_inv + ins_ticket_base.flt_total_tax_credit_inv + ins_ticket_base.flt_market_fare_credit_rfd + ins_ticket_base.flt_total_tax_credit_rfd 
                        int_card_type = 2
                    elif ins_ticket_base.int_card_payment_type == 2  :
                        flt_cor_card_amount += ins_ticket_base.flt_market_fare_credit_inv + ins_ticket_base.flt_total_tax_credit_inv + ins_ticket_base.flt_market_fare_credit_rfd + ins_ticket_base.flt_total_tax_credit_rfd 
                        int_card_type = 0
                    else  :
                        int_card_type = 1
                        flt_cor_card_amount += ins_ticket_base.flt_market_fare_credit_inv + ins_ticket_base.flt_total_tax_credit_inv + ins_ticket_base.flt_market_fare_credit_rfd + ins_ticket_base.flt_total_tax_credit_rfd 
                    ins_ticket_base.str_cc_type_inv = tpl_card_data[0]
                    ins_ticket_base.str_cc_number_ext = tpl_card_data[1]
                    
                    lst_credit_card_transaction_data.append((ins_ticket_base.str_ticket_number ,
                                                            'T' ,
                                                            int_card_type ,
                                                             ins_ticket_base.str_card_approval_code ,
                                                             ins_ticket_base.str_defult_currency_code ,
                                                            ins_ticket_base.flt_market_fare_credit_inv + ins_ticket_base.flt_total_tax_credit_inv 
                                                            
                                                            ))
                else :
                    
                    if tpl_card_data[1] :
                        (int_account_master_id,int_account_type) = ins_general_methods.get_corporate_card_id(tpl_card_data[1],ins_ticket_base.str_crs_company)
                        if int_account_master_id:
                            
                            if int_account_type == 1 : #// bln customer card true
                                int_card_type = 1
                                bln_cust_card = True
                            else :
                                int_card_type = 0
                                bln_agency_card = True
                                
                        else :#if not int_account_master_id and ins_ticket_base.str_card_approval_code :
                            int_card_type = 2
                            
                    if int_card_type == 2 :
                        flt_uccf_amount += float(tpl_card_data[2] or 0)
                    else :
                        flt_cor_card_amount += float(tpl_card_data[2] or 0)
                    ins_ticket_base.str_cc_type_inv = tpl_card_data[0]
                    ins_ticket_base.str_cc_number_ext = tpl_card_data[1]
                    
                    lst_credit_card_transaction_data.append((
                                                            ins_ticket_base.str_ticket_number ,
                                                            'T' ,
                                                            int_card_type ,
                                                            tpl_card_data[3] ,
                                                            ins_ticket_base.str_defult_currency_code ,
                                                            tpl_card_data[2]
                                                            ))
                    
            if flt_uccf_amount or flt_cor_card_amount :
                if not ins_ticket_base.str_refund_date :
                    flt_mf_credit = ins_ticket_base.flt_market_fare_credit_inv
                    flt_tax_credit = ins_ticket_base.flt_total_tax_credit_inv
                    if len(ins_ticket_base.lst_card_data) > 1 :
                        if bln_cust_card :
                            ins_ticket_base.int_card_payment_type = 6
                        elif bln_agency_card :
                            ins_ticket_base.int_card_payment_type = 5
                    
                    if flt_uccf_amount :
                        if flt_uccf_amount >  flt_mf_credit:
                            ins_ticket_base.flt_market_fare_uccf_amount_inv = flt_mf_credit
                            flt_uccf_amount = flt_uccf_amount - flt_mf_credit
                            flt_mf_credit = 0
                            ins_ticket_base.flt_tax_uccf_amount_inv  = flt_uccf_amount
                            flt_uccf_amount = 0
                            flt_tax_credit = flt_tax_credit - ins_ticket_base.flt_tax_uccf_amount_inv
                        else :
                            ins_ticket_base.flt_market_fare_uccf_amount_inv = flt_uccf_amount
                            ins_ticket_base.flt_tax_uccf_amount_inv = 0
                            flt_mf_credit = flt_mf_credit - ins_ticket_base.flt_market_fare_uccf_amount_inv
                            flt_uccf_amount = 0
                            
                    if flt_cor_card_amount :
                        
                        if flt_cor_card_amount >  flt_mf_credit:
                            ins_ticket_base.flt_market_fare_card_amount_inv = flt_mf_credit
                            flt_cor_card_amount = flt_cor_card_amount - flt_mf_credit
                            flt_mf_credit = 0
                            ins_ticket_base.flt_tax_card_amount_inv  = flt_cor_card_amount
                            flt_cor_card_amount = 0
                            flt_tax_credit = flt_tax_credit - ins_ticket_base.flt_tax_card_amount_inv
                        else :
                            ins_ticket_base.flt_market_fare_card_amount_inv = flt_cor_card_amount
                            ins_ticket_base.flt_tax_card_amount_inv = 0
                            flt_mf_credit = flt_mf_credit - ins_ticket_base.flt_market_fare_card_amount_inv
                            flt_cor_card_amount = 0
                        

                    pass
                else :
                    flt_mf_credit = ins_ticket_base.flt_market_fare_credit_rfd
                    flt_tax_credit = ins_ticket_base.flt_total_tax_credit_rfd
                    flt_charg_credit = ins_ticket_base.flt_supplier_refund_charge_credit
                    
                    if len(ins_ticket_base.lst_card_data) > 1 :
                        if bln_cust_card :
                            ins_ticket_base.int_card_payment_type_rfd = 6
                        elif bln_agency_card :
                            ins_ticket_base.int_card_payment_type_rfd = 5
                    
                    if flt_uccf_amount :
                        if flt_uccf_amount >  flt_mf_credit:
                            ins_ticket_base.flt_market_fare_uccf_amount_rfd = flt_mf_credit
                            flt_uccf_amount = flt_uccf_amount - flt_mf_credit
                            flt_mf_credit = 0
                            if flt_uccf_amount > flt_tax_credit:
                                ins_ticket_base.flt_tax_uccf_amount_rfd = flt_tax_credit
                                flt_uccf_amount = flt_uccf_amount - flt_tax_credit
                                ins_ticket_base.flt_sup_charge_uccf_amount_rfd = flt_uccf_amount
                                flt_uccf_amount = 0
                                pass
                            else :
                                ins_ticket_base.flt_tax_uccf_amount_rfd  = flt_uccf_amount
                                flt_uccf_amount = 0
                                ins_ticket_base.flt_sup_charge_uccf_amount_rfd = 0
                                flt_tax_credit = flt_tax_credit - ins_ticket_base.flt_tax_uccf_amount_rfd
                        else :
                            ins_ticket_base.flt_market_fare_uccf_amount_rfd = flt_uccf_amount
                            ins_ticket_base.flt_tax_uccf_amount_rfd = 0
                            ins_ticket_base.flt_sup_charge_uccf_amount_rfd = 0
                            flt_mf_credit = flt_mf_credit - ins_ticket_base.flt_market_fare_uccf_amount_rfd
                            flt_uccf_amount = 0
                            
                    if flt_cor_card_amount :
                        
                        if flt_cor_card_amount >  flt_mf_credit:
                            ins_ticket_base.flt_market_fare_card_amount_rfd = flt_mf_credit
                            flt_cor_card_amount = flt_cor_card_amount - flt_mf_credit
                            flt_mf_credit = 0

                            if flt_cor_card_amount > flt_tax_credit :
                                ins_ticket_base.flt_tax_card_amount_rfd  = flt_tax_credit
                                flt_cor_card_amount = flt_cor_card_amount - flt_tax_credit
                                flt_tax_credit = 0
                                ins_ticket_base.flt_sup_charge_card_amount_rfd = flt_cor_card_amount
                            else :
                                ins_ticket_base.flt_tax_card_amount_rfd = flt_cor_card_amount
                                flt_cor_card_amount = 0
                                ins_ticket_base.flt_sup_charge_card_amount_rfd = 0

                        else :
                            ins_ticket_base.flt_market_fare_card_amount_rfd = flt_cor_card_amount
                            ins_ticket_base.flt_tax_card_amount_rfd = 0
                            ins_ticket_base.flt_sup_charge_card_amount_rfd = 0
                            flt_mf_credit = flt_mf_credit - ins_ticket_base.flt_market_fare_card_amount_rfd
                            flt_cor_card_amount = 0
                    
                    
            if bln_agency_card or ins_ticket_base.int_card_payment_type == 2:
                ins_ticket_base.flt_debited_amount_inv = ins_ticket_base.flt_selling_price 
                ins_ticket_base.flt_debited_amount_rfd = ins_ticket_base.flt_net_payable_rfd 
                ins_ticket_base.flt_inv_debited_amount_rfd = ins_ticket_base.flt_selling_price_rfd
            
            #get customer currency and its roe
            if ins_ticket_base.int_account_master_id and (ins_ticket_base.str_ticket_issue_date  or ins_ticket_base.str_refund_date):
                ins_ticket_base.str_cust_currency_code,ins_ticket_base.flt_cust_currency_roe = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,\
                                    ins_ticket_base.str_ticket_issue_date or ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
            
            
            str_current_date_time = ins_general_methods.get_current_date_time()

            
            int_from = int(ins_ticket_base.str_ticket_number)
            int_to = int(ins_ticket_base.str_last_conjection_ticket_number or ins_ticket_base.str_ticket_number)

            if (int_from-int_to) > 4 :
                raise Exception("Unable to update non iata list..")

            for int_tkt_no in range(int_from,int_to+1) :
                lst_ticket_nos.append([str(int_tkt_no),
                                    ins_ticket_base.str_ticket_number,
                                    ins_ticket_base.str_last_conjection_ticket_number,
                                    int_from != int_tkt_no,
                                    ])

            lst_ticket_details.append([ins_ticket_base.str_ticket_number,
                                ins_ticket_base.str_pnr_no,
                                ins_ticket_base.str_ticket_type, #to do
                                ins_ticket_base.str_original_issue,
                                ins_ticket_base.str_last_conjection_ticket_number,
                                ins_ticket_base.str_connection_ticket,
                                ins_ticket_base.chr_ticket_status,
                                ins_ticket_base.chr_system_sale_part_action,
                                ins_ticket_base.chr_system_refund_part_action,
                                ins_ticket_base.str_crs_company,
                                ins_ticket_base.str_file_name_inv,
                                ins_ticket_base.str_file_name_rfd,
                                ins_ticket_base.chr_ticket_category,
                                ins_ticket_base.str_booking_agent_code,
                                ins_ticket_base.str_booking_agent_numeric_code,
                                ins_ticket_base.str_ticketing_agent_code,
                                ins_ticket_base.str_ticketing_agent_numeric_code,
                                ins_ticket_base.str_ticketing_agent_code_rfd,
                                ins_ticket_base.str_ticketing_agent_numeric_code_rfd,
                                ins_ticket_base.str_booking_agency_iata_no,
                                ins_ticket_base.str_ticketing_agency_iata_no,
                                ins_ticket_base.str_booking_agency_office_id,
                                ins_ticket_base.str_ticketing_agency_office_id,
                                ins_ticket_base.str_pnr_first_owner_office_id,
                                ins_ticket_base.str_pnr_current_owner_office_id,
                                ins_ticket_base.str_pax_name,
                                ins_ticket_base.str_pax_type,
                                ins_ticket_base.int_no_of_pax_inv,
                                ins_ticket_base.int_ticket_type_id,
                                ins_ticket_base.bln_glpo,
                                ins_ticket_base.str_ticket_booking_date,
                                ins_ticket_base.str_ticket_issue_date,
                                ins_ticket_base.str_travel_date,
                                ins_ticket_base.str_return_date,
                                ins_ticket_base.dat_rm_lpo_date,
                                ins_ticket_base.int_booking_agent_counter_staff_id,
                                ins_ticket_base.int_counter_staff_id_inv,
                                ins_ticket_base.int_airline_account_id,
                                ins_ticket_base.int_airline_id,
                                ins_ticket_base.str_class_group,
                                ins_ticket_base.str_class,
                                ins_ticket_base.int_class_id,
                                ins_ticket_base.str_return_class_group,
                                ins_ticket_base.str_return_class,
                                ins_ticket_base.int_return_class_id,
                                ins_ticket_base.int_region_id,
                                ins_ticket_base.str_sector,
                                ins_ticket_base.int_no_of_segments,
                                ins_ticket_base.str_destination,
                                ins_ticket_base.int_location_id, #cost centre is branch
                                ins_ticket_base.int_account_master_id,
                                ins_ticket_base.int_supplier_id,
                                ins_ticket_base.str_tax_details,
                                #ins_ticket_base.int_dc_card,#fk_bint_agency_corporate_card_account_id_inv
                                ins_ticket_base.int_dc_card,#fk_bint_corporate_card_account_id_inv
                                ins_ticket_base.str_card_approval_code,
                                ins_ticket_base.int_card_payment_type,
                                #ins_ticket_base.str_agency_adv_receipt_no,
                                ins_ticket_base.str_remarks,
                                ins_ticket_base.str_refund_date,
                                ins_ticket_base.int_counter_staff_id_rfd,
                                ins_ticket_base.str_sector_rfd, #sector refunded --to do
                                ins_ticket_base.int_number_of_segments_rfd, #no of segments refunded --to do
                                ins_ticket_base.int_location_id_rfd,
                                ins_ticket_base.int_account_master_id_rfd, #--to do
                                ins_ticket_base.int_supplier_id_rfd, #--to do
                                ins_ticket_base.str_tax_details_rfd,#ins_ticket_base.str_tax_details,#--to do
                                #ins_ticket_base.int_dc_card_rfd,#fk_bint_agency_corporate_card_account_id_rfd --to do
                                ins_ticket_base.int_corporate_card_id_rfd,#fk_bint_corporate_card_account_id_rfd --check
                                ins_ticket_base.int_card_payment_type_rfd, #--to do
                                ins_ticket_base.str_remarks_rfd,
                                ins_ticket_base.str_cust_cost_centre,
                                ins_ticket_base.str_employee_number,
                                ins_ticket_base.str_lpo_number,
                                ins_ticket_base.str_crm_reference,
                                ins_ticket_base.int_distribution_type_inv,
                                ins_ticket_base.int_distribution_type_rfd,
                                1, #service id
                                ins_ticket_base.json_extra_capturing_data,
                                2, #created user id
                                str_current_date_time
                                 ])


            
            lst_temp_fare_common_data = []
            bln_cust_base = False
            if not ins_ticket_base.str_cust_currency_code :
                bln_cust_base = True
            if ins_ticket_base.str_defult_currency_code == ins_general_methods.str_base_currency :
                if ins_ticket_base.str_defult_currency_code == ins_ticket_base.str_cust_currency_code:
#                    print ('all are same')
                    lst_temp_fare_common_data = [[True, #bln_sup_base_currency
                                                True, #bln_tran_currency
                                                True, #bln_cust_base_currency
                                                True, #bln_cust_currency
                                                ins_ticket_base.str_defult_currency_code, #currency -all are same
                                                1,#roe
                                                False]] #bln_refund
                else:
#                    print ('Tran and base are same, Cust different')
                    lst_temp_fare_common_data = [[True, #bln_sup_base_currency
                                                True, #bln_tran_currency
                                                bln_cust_base, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_ticket_base.str_defult_currency_code, #currency -Tran and base are same
                                                1,#roe
                                                False]] #bln_refund
                    if ins_ticket_base.str_cust_currency_code:
                        lst_temp_fare_common_data.extend([[False, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                False, #bln_cust_base_currency
                                                True, #bln_cust_currency
                                                ins_ticket_base.str_cust_currency_code, #currency - Cust different
                                                ins_ticket_base.flt_supplier_currency_roe, #roe
                                                False],#bln_refund
                                                [False, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                True, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_general_methods.str_base_currency, #currency - Cust different
                                                ins_ticket_base.flt_supplier_currency_roe, #roe
                                                False]
                                                ]) 
            elif ins_ticket_base.str_defult_currency_code == ins_ticket_base.str_cust_currency_code:
#                print ('Tran and cust are same, Base different')
                lst_temp_fare_common_data = [[False, #bln_sup_base_currency
                                                True, #bln_tran_currency
                                                False, #bln_cust_base_currency
                                                True, #bln_cust_currency
                                                ins_ticket_base.str_defult_currency_code, #currency -Tran and Cust are same
                                                1, #roe 
                                                False ],#bln_refund
                                                [True, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                True, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_general_methods.str_base_currency, #currency - base different
                                                ins_ticket_base.flt_supplier_currency_roe,#roe
                                                False] #bln_refund
                                                ] 
            elif ins_ticket_base.str_cust_currency_code == ins_general_methods.str_base_currency:
#                print ('Cust and base are same, Tran different')
                lst_temp_fare_common_data = [[False, #bln_sup_base_currency
                                                True, #bln_tran_currency
                                                False, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_ticket_base.str_defult_currency_code, #currency - Tran different
                                                1, #roe
                                                False],#bln_refund
                                                [True, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                False, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_general_methods.str_base_currency, #currency - Tran different
                                                ins_ticket_base.flt_supplier_currency_roe, #roe
                                                False],
                                                [False, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                True, #bln_cust_base_currency
                                                True, #bln_cust_currency
                                                ins_general_methods.str_base_currency, #currency -Cust and base are same
                                                ins_ticket_base.flt_supplier_currency_roe,#roe
                                                False] #bln_refund
                                            ] 
            else:
#                print ('all are different')
                lst_temp_fare_common_data = [[False, #bln_sup_base_currency
                                                True, #bln_tran_currency
                                                False, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_ticket_base.str_defult_currency_code, #currency -Tran 
                                                1,#roe
                                                False],#bln_refund
                                                [True, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                bln_cust_base, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_general_methods.str_base_currency, #currency - base
                                                ins_ticket_base.flt_supplier_currency_roe,#roe
                                                False] #bln_refund
                                            ] 
                if ins_ticket_base.str_cust_currency_code:
                    lst_temp_fare_common_data.extend([[False, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                False, #bln_cust_base_currency
                                                True, #bln_cust_currency
                                                ins_ticket_base.str_cust_currency_code, #currency - Cust
                                                ins_ticket_base.flt_supplier_currency_roe, #roe
                                                False],#bln_refund
                                                [False, #bln_sup_base_currency
                                                False, #bln_tran_currency
                                                True, #bln_cust_base_currency
                                                False, #bln_cust_currency
                                                ins_general_methods.str_base_currency, #currency - Cust
                                                ins_ticket_base.flt_supplier_currency_roe, #roe
                                                False]] #bln_refund
                                                ) 
                        
            if ins_ticket_base.bln_refund:
                for lst_fare_temp in lst_temp_fare_common_data:
                    lst_fare_temp[6] = True #bln_refund - true
            elif ins_ticket_base.str_void_date:
                for lst_fare_temp in lst_temp_fare_common_data:
                    lst_fare_temp[6] = True #bln_refund - true
            
            lst_temp_fare_data = []
            lst_temp_fare_data = self.create_fare_data_list(ins_ticket_base,
                                                            lst_temp_fare_common_data,
                                                            ins_ticket_base.flt_supplier_currency_roe,
                                                            ins_ticket_base.flt_cust_currency_roe)
            
            lst_fare_data.extend(lst_temp_fare_data)

            
            # preparing list to Inserting data to sector details --------------

            int_sector_count = 0
            int_dest_index = 0   #40613
            int_ticket_no = int(ins_ticket_base.str_ticket_number)
            if ins_ticket_base.str_last_conjection_ticket_number:
                int_last_ticket_no = int(ins_ticket_base.str_last_conjection_ticket_number)
            else:
                int_last_ticket_no = None


            ins_ticket_base.str_return_date  = None
            if not ins_ticket_base.bln_emd:
                for lst_sec in ins_ticket_base.lst_sector_details:
                    [str_orgin_airport_code,
                    str_dest_code,
                    str_airline_code,
                    str_airline_no,
                    str_flight_number,
                    str_class_of_service,
                    str_class_of_booking,
                    str_arrival_date,
                    str_departure_date,
                    bln_stopover_permitted,
                    int_mileage,
                    dbl_sector_wise_fare,
                    str_arrival_time,
                    str_departure_time,
                    bln_open_segment] = lst_sec
                    int_sector_count += 1
                    if int_sector_count >= 5:
                        int_ticket_no += 1
                        if int_last_ticket_no and int_last_ticket_no < int_ticket_no:
                            raise
                            pass
                        int_sector_count = 1
                    if str_airline_code != 'VOID': # if the segment is void we are not adding it to the table
                                                                           # but it is counted as a segment and the conjunction is created if needed

                        if ins_ticket_base.str_destination == str_orgin_airport_code and not ins_ticket_base.str_return_date:

                            ins_ticket_base.str_return_date = str_departure_date
                            ins_ticket_base.str_return_class = str_class_of_booking

                            if ins_ticket_base.str_return_class :
                                ins_ticket_base.int_return_class_id,ins_ticket_base.str_return_class_group = ins_general_methods.get_class_id('%' + ins_ticket_base.str_return_class + '%',ins_ticket_base.int_airline_account_id)
                            else :
                                ins_ticket_base.int_return_class_id = None


                        lst_setcor_data.append([
                                            ins_ticket_base.int_ticket_id,
                                            int_ticket_no,
                                            str_flight_number,
                                            str_departure_date,
                                            str_departure_time,
                                            str_arrival_date,
                                            str_arrival_time,
                                            str_orgin_airport_code,
                                            str_dest_code,
                                            ins_ticket_base.str_fare_basis,
                                            str_class_of_service,
                                            bln_stopover_permitted,
                                            int_mileage#,bln_open_segment
                                            ])
    #                    int_sector_id += 1


            #40613    
            if ins_ticket_base.bln_return and not ins_ticket_base.str_return_date and ins_ticket_base.str_sector.find('/') != -1:
                lst_sectors = [str_code for str_code in ins_ticket_base.str_sector.split('/') if str_code]
                int_dest_index= lst_sectors.index(ins_ticket_base.str_destination)
                if len(ins_ticket_base.lst_sector_details) > int_dest_index+1 and ins_ticket_base.lst_sector_details[int_dest_index+1][2] != 'VOID':
                    ins_ticket_base.str_return_date = ins_ticket_base.lst_sector_details[int_dest_index+1][8]


            ## Non IATA ticket capture # 23450
            if ins_general_methods.bln_enable_non_iata_capture :
                ins_general_methods.ins_global.lst_process_list.append(ins_ticket_base)

                


        for lst_data in lst_ticket_details :
            lst_data[33] = ins_ticket_base.str_return_date
            lst_data[42] = ins_ticket_base.str_return_class_group
            lst_data[43] = ins_ticket_base.str_return_class
            lst_data[44] = ins_ticket_base.int_return_class_id

        try:
            if lst_ticket_details :
                cr.executemany("""INSERT INTO tbl_ticket
                                    (   vchr_ticket_number,
                                        vchr_pnr_number,
                                        vchr_type,
                                        vchr_original_issue,
                                        vchr_last_conjunction_ticket_number,
                                        vchr_connection_ticket_number,
                                        chr_ticket_status,
                                        chr_system_sale_part_action,
                                        chr_system_refund_part_action,
                                        vchr_gds_company,
                                        vchr_gds_file_name_inv,
                                        vchr_gds_file_name_rfd,
                                        chr_ticket_source,
                                        vchr_booking_agent_code,
                                        vchr_booking_agent_numeric_code,
                                        vchr_ticketing_agent_code_inv,
                                        vchr_ticketing_agent_numeric_code_inv,
                                        vchr_ticketing_agent_code_rfd,
                                        vchr_ticketing_agent_numeric_code_rfd,
                                        vchr_booking_agency_iata_no,
                                        vchr_ticketing_agency_iata_no,
                                        vchr_booking_agency_office_id,
                                        vchr_ticketing_agency_office_id,
                                        vchr_pnr_first_owner_office_id,
                                        vchr_pnr_current_owner_office_id,
                                        vchr_pax_name,
                                        vchr_pax_type,
                                        int_no_of_pax,
                                        fk_bint_ticket_type_id,
                                        bln_glpo,
                                        dat_booking,
                                        dat_issue,
                                        dat_travel_date,
                                        dat_of_return,
                                        dat_customer_lpo_date,
                                        fk_bint_booking_agent_id,
                                        fk_bint_ticketing_agent_id_inv,
                                        fk_bint_airline_account_id,
                                        fk_bint_airline_id,
                                        vchr_class,
                                        vchr_class_rbd,
                                        fk_bint_class_id,
                                        vchr_return_class,
                                        vchr_return_class_rbd,
                                        fk_bint_return_class_id,
                                        fk_bint_airports_regions_id,
                                        vchr_sector_inv,
                                        int_no_of_segments_inv,
                                        vchr_destination_airport,
                                        fk_bint_branch_id_inv,
                                        fk_bint_customer_account_id_inv,
                                        fk_bint_supplier_account_id_inv,
                                        vchr_tax_details_inv,
                                        -- fk_bint_agency_corporate_card_account_id_inv,
                                        fk_bint_corporate_card_account_id_inv,
                                        vchr_card_approval_code,
                                        int_card_payment_type_inv,
                                        --vchr_advance_receipt_number,
                                        vchr_remarks_inv,
                                        dat_refund,
                                        fk_bint_ticketing_agent_id_rfd,
                                        vchr_sector_rfd,
                                        int_no_of_segments_rfd,
                                        fk_bint_branch_id_rfd,
                                        fk_bint_customer_account_id_rfd,
                                        fk_bint_supplier_account_id_rfd,
                                        vchr_tax_details_rfd,
                                        fk_bint_corporate_card_account_id_rfd,
                                        int_card_payment_type_rfd,
                                        vchr_remarks_rfd,
                                        vchr_customer_cost_centre,
                                        vchr_customer_employee_number,
                                        vchr_customer_lpo_or_to_number,
                                        vchr_crm_reference,
                                        int_distribution_type_inv,
                                        int_distribution_type_rfd,
                                        fk_bint_service_id,
                                        json_extra_capturing_data,
                                        fk_bint_created_user_id,
                                        tim_created
                                        )
                                    VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" , lst_ticket_details)

                
                dct_tkts = ins_general_methods.get_ticket_table_id(lst_tkt_no)
                
                for lst_tkt_data in lst_ticket_nos :
                    lst_tkt_data.append(dct_tkts[lst_tkt_data[1]])

                cr.executemany("""INSERT INTO tbl_ticket_numbers(
                                                vchr_ticket_number,
                                                vchr_master_ticket_number,
                                                vchr_last_ticket_number,
                                                bln_conjunction_ticket,
                                                fk_bint_ticket_id )
                                            VALUES (%s, %s, %s, %s, %s) 
                                            """,lst_ticket_nos)
                
                if ins_general_methods.ins_global.lst_process_list :
                    for ins_ticket_base1 in ins_general_methods.ins_global.lst_process_list : #38619
                        if ins_ticket_base1.str_ticket_number in dct_tkts :
                            ins_ticket_base1.int_ticket_id = dct_tkts[ins_ticket_base1.str_ticket_number]
                        else :
                            raise Exception("Unable to update non iata list..")
                        
                
                if lst_fare_data :
                    for lst_data in lst_fare_data :
                        if lst_data[0] in dct_tkts :
                            lst_data[1] = dct_tkts[lst_data[0]]
                        else :
                            raise Exception("Unable to update tbl_fare_details list..")                                
                    self.save_fare_details_data(lst_fare_data) #insert into fare details

#                if lst_credit_card_transaction_data : #need to check
#
#                    self.save_credit_card_transaction_data(lst_credit_card_transaction_data)
                pass
            if lst_setcor_data:
                for lst_data_tmp in lst_setcor_data :        
                    if str(lst_data_tmp[1]) in dct_tkts :
                        lst_data_tmp[0] = dct_tkts[str(lst_data_tmp[1])]
                    else :
                        raise Exception("Unable to save sector details..")

                cr.executemany("""INSERT INTO tbl_sector_details
                                    (
                                      fk_bint_ticket_id,
                                      vchr_ticket_number,
                                      vchr_flight_no,
                                      dat_departure_date,
                                      vchr_departure_time,
                                      dat_arrival_date,
                                      vchr_arrival_time,
                                      vchr_origin_code,
                                      vchr_destination_code,
                                      vchr_fare_basis,
                                      vchr_class_of_service,
                                      bln_stopover_permitted,
                                      bint_mileage
                                      
                                      --,bln_open_segment 
                                      )
                                    VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s)""" ,lst_setcor_data)
                                            
            #save ticket log into tbl_ticket_log
            for str_tkt_number in dct_tkts.keys():
                self.save_ticket_log_data(str_tkt_number,str_current_date_time)

        except Exception as msg:
            print(msg)
            cr.close()
#            if bln_raise :
#                print(lst_ticket_details,'\n',lst_setcor_data,'\n',lst_extra_capturing_fields)
#                raise
            OperationalError.ins_ticket_base = ins_ticket_base
            try:
                chr_ticket_status = ins_ticket_base.chr_ticket_status

                str_file_name = os.path.split(ins_ticket_base.str_file_name)[1].split('.')[0]
                str_file_name_ticket_num = str_file_name + ':' + str(ins_ticket_base.str_ticket_number)
                if str_file_name_ticket_num not in dct_error_messages:
                    str_line_no = str(sys.exc_info()[2].tb_lineno)
                    str_message = str(sys.exc_info()[1])
                    dct_error_messages[str_file_name_ticket_num] = [ins_ticket_base.str_ticket_number, chr_ticket_status, str_line_no,str_message]
            except:
                pass
            raise OperationalError
        cr.close()
        
    def save_captured_hotel_voucher_data(self,lst_ins_hv) :
        
        ## Refer #13823 
        cr = self.create_cursor()
        lst_hv = []
        lst_hotel_extra_capturing_fields = []    #refer #40299
        str_tim_created = ins_general_methods.get_current_date_time()
        for ins_service_base in lst_ins_hv :
        
            if ins_service_base.int_card_payment_type == 1  :
                ins_service_base.flt_fare_uccf_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_uccf_amount = ins_service_base.flt_total_tax_credit_card_inv 
            else :
                ins_service_base.flt_fare_card_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_card_amount = ins_service_base.flt_total_tax_credit_card_inv 
        
        
            lst_hv.append((
                    ins_service_base.str_voucher_number ,
                    ins_service_base.str_pnr_no ,
                    'I' ,
                    'P' ,
                    ins_service_base.int_vendor_id ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_city_id ,
                    ins_service_base.str_pax_name ,
                    ins_service_base.int_service_id ,
                    ins_service_base.str_crs_company ,
                    '',
                    ins_service_base.int_booking_staff_id,
                    ins_service_base.str_booking_details ,
                    ins_service_base.str_supplier_confirm_number ,
                    ins_service_base.str_hotel_confirm_number ,
                    ins_service_base.str_hotel_check_in_time ,
                    ins_service_base.str_hotel_check_out_time ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_meals_plan_id ,
                    ins_service_base.int_room_type_id ,
                    json.dumps({}),
                    ins_service_base.dat_rm_lpo_date,
                    ins_service_base.str_customer_lpo_number ,
                    ins_service_base.str_customer_cost_centre ,
                    ins_service_base.str_customer_emp_no ,
                    None,
                    ins_service_base.str_voucher_issue_date ,
                    ins_service_base.int_counter_staff_id ,
                    ins_service_base.int_no_of_adults_inv ,
                    ins_service_base.int_no_of_child_inv ,
                    ins_service_base.int_no_of_guest_inv ,
                    ins_service_base.int_no_of_nights ,
                    ins_service_base.int_no_of_rooms ,
                    ins_service_base.int_no_of_room_nights ,
                    ins_service_base.flt_fare_inv ,
                    ins_service_base.flt_total_tax_inv ,
                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_amount_data(ins_service_base.flt_total_tax_inv,ins_service_base.flt_supplier_currency_roe),
                    0,#cust fare
                    0,
                    0,
                    0,
                    ins_service_base.int_location_id ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.int_supplier_id ,
                    ins_service_base.int_corp_card_id ,
                    None,
                    ins_service_base.int_card_payment_type ,
                    ins_service_base.str_remarks ,
                    ins_service_base.str_file_name_inv ,
                    ins_service_base.json_extra_capturing_data,
                    2,
                    str_tim_created  
                    ))
        
        
        
        
        
        
        lst_hotel_vouchers = [tpl_data[0] for tpl_data in lst_hv]
        try:
            cr.executemany("""

                INSERT INTO tbl_hotel_voucher

                (
                    vchr_voucher_number,
                    vchr_pnr_number,
                    chr_voucher_status,
                    chr_system_sale_part_action,
                    fk_bint_hotel_master_id,
                    fk_bint_hotel_country_id,
                    fk_bint_hotel_city_id,
                    vchr_guest_name,
                    fk_bint_service_id,
                    vchr_gds_company,
                    chr_voucher_source,
                    fk_bint_booking_staff_id,
                    vchr_booking_details,
                    vchr_supplier_confirm_no,
                    vchr_hotel_confirm_no,
                    tim_check_in,
                    tim_check_out,
                    fk_bint_origin_country_id,
                    fk_bint_hotel_meals_plan_id,
                    fk_bint_hotel_room_type_id,
                    json_travel_details,
                    dat_customer_lpo_date,
                    vchr_customer_lpo_or_to_number,
                    vchr_customer_cost_centre,
                    vchr_customer_employee_number,
                    dat_booking,
                    dat_issue,
                    fk_bint_counter_staff_id_inv,
                    int_no_of_adults_inv,
                    int_no_of_children_inv,
                    int_no_of_guests_inv,
                    int_no_of_nights_inv,
                    int_no_of_rooms_inv,
                    int_no_of_room_nights_inv,
                    dbl_sup_currency_fare_per_night_inv,
                    dbl_sup_currency_tax_per_night_inv,
                    dbl_sup_base_currency_fare_per_night_inv,
                    dbl_sup_base_currency_tax_per_night_inv,
                    dbl_cust_currency_fare_per_night_inv,
                    dbl_cust_currency_tax_per_night_inv,
                    dbl_cust_base_currency_fare_per_night_inv,
                    dbl_cust_base_currency_tax_per_night_inv,
                    fk_bint_branch_id_inv,
                    fk_bint_customer_account_id_inv,
                    fk_bint_supplier_account_id_inv,
                    fk_bint_corporate_card_account_id_inv,
                    fk_bint_agency_corporate_card_account_id_inv,
                    int_card_payment_type_inv,
                    vchr_remarks_inv,
                    vchr_gds_file_name_inv,
                    json_extra_capturing_data,
                    fk_bint_created_user_id,
                    tim_created

                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s

                )


                """,lst_hv)
                
            cr.close()
            lst_voucher_fare_details = self.create_fare_data_list_for_voucher(ins_service_base)
            #save data fare_details
            self.save_fare_details_data(lst_voucher_fare_details)
            
            #save log data
            for str_vocuher in lst_hotel_vouchers :
                self.save_other_voucher_log_data(str_vocuher,str_tim_created,'H')
                
            return lst_hotel_vouchers

        except Exception as msg:
            cr.close()
            print (msg)
            raise
    
    def save_captured_car_voucher_data(self,lst_ins_cv): #40359
        cr = self.create_cursor()
        lst_cv = []
        lst_car_extra_capturing_fields = []
        str_tim_created = ins_general_methods.get_current_date_time()
        for ins_service_base in lst_ins_cv :
        
            if ins_service_base.int_card_payment_type == 1  :
                ins_service_base.flt_fare_uccf_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_uccf_amount = ins_service_base.flt_total_tax_credit_card_inv 
            else :
                ins_service_base.flt_fare_card_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_card_amount = ins_service_base.flt_total_tax_credit_card_inv 
        
        
            lst_cv.append((
                    ins_service_base.str_voucher_number ,
                    ins_service_base.str_pnr_no ,
                    'I' ,
                    'P' ,
                    ins_service_base.int_car_rental_company_id,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_city_id,
                    ins_service_base.str_pax_name ,
                    ins_service_base.int_service_id ,
                    ins_service_base.str_crs_company ,
                    '',
                    ins_service_base.int_booking_staff_id,
                    ins_service_base.str_pick_up_time ,
                    ins_service_base.str_drop_off_time ,
                    ins_service_base.str_booking_details ,
                    ins_service_base.str_supplier_confirm_number ,
                    ins_service_base.str_supplier_confirm_number,
                    '',
                    '',
                    '',
                    False,
                    False,
                    '',
                    ins_service_base.str_pick_up_location ,
                    ins_service_base.str_drop_off_location ,
                    json.dumps({}),
                    ins_service_base.dat_rm_lpo_date,
                    ins_service_base.str_customer_lpo_number ,
                    ins_service_base.str_customer_cost_centre ,
                    ins_service_base.str_customer_emp_no ,
                    None,
                    ins_service_base.str_voucher_issue_date ,
                    ins_service_base.int_counter_staff_id ,
                    ins_service_base.int_no_of_days ,
                    ins_service_base.int_no_of_car ,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    ins_service_base.int_location_id ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.int_supplier_id ,
                    ins_service_base.int_corp_card_id ,
                    None,
                    ins_service_base.int_card_payment_type ,
                    ins_service_base.str_remarks ,
                    ins_service_base.str_file_name_inv ,
                    ins_service_base.json_extra_capturing_data,
                    2,
                    str_tim_created
                    
                    ))
        
        
        lst_car_vouchers = [tpl_data[0] for tpl_data in lst_cv]
        try:
            cr.executemany("""

                INSERT INTO tbl_car_voucher

                (
                vchr_voucher_number,
                vchr_pnr_number,
                chr_voucher_status,
                chr_system_sale_part_action,
                fk_bint_car_rental_company_id,
                fk_bint_car_rental_company_country_id,
                fk_bint_car_rental_company_city_id,
                vchr_pax_name,
                fk_bint_service_id,
                vchr_gds_company,
                chr_voucher_source,
                fk_bint_booking_staff_id,
                tim_car_start,
                tim_car_end,
                vchr_booking_details,
                vchr_supplier_confirm_no,
                vchr_rental_company_confirm_no,
                vchr_car_category,
                vchr_car_vehicle,
                vchr_car_model,
                bln_car_auto_or_manual,
                bln_car_chauffeur,
                vchr_car_renting_period,
                vchr_car_renting_station,
                vchr_car_drop_station,
                json_travel_details,
                dat_customer_lpo_date,
                vchr_customer_lpo_or_to_number,
                vchr_customer_cost_centre,
                vchr_customer_employee_number,
                dat_booking,
                dat_issue,
                fk_bint_counter_staff_id_inv,
                int_no_of_days_inv,
                int_no_of_cars_inv,
                dbl_sup_currency_fare_per_day_inv,
                dbl_sup_currency_tax_per_day_inv,
                dbl_sup_base_currency_fare_per_day_inv,
                dbl_sup_base_currency_tax_per_day_inv,
                dbl_cust_currency_fare_per_day_inv,
                dbl_cust_currency_tax_per_day_inv,
                dbl_cust_base_currency_fare_per_day_inv,
                dbl_cust_base_currency_tax_per_day_inv,
                fk_bint_branch_id_inv,
                fk_bint_customer_account_id_inv,
                fk_bint_supplier_account_id_inv,
                fk_bint_corporate_card_account_id_inv,
                fk_bint_agency_corporate_card_account_id_inv,
                int_card_payment_type_inv,
                vchr_remarks_inv,
                vchr_gds_file_name_inv,
                json_extra_capturing_data,
                fk_bint_created_user_id,
                tim_created

                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )


                """,lst_cv)

            cr.close()
            
            lst_voucher_fare_details = self.create_fare_data_list_for_voucher(ins_service_base)
            #save data fare_details
            self.save_fare_details_data(lst_voucher_fare_details)
            
            #save log data
            for str_vocuher in lst_car_vouchers :
                self.save_other_voucher_log_data(str_vocuher,str_tim_created,'C')
                
            return lst_car_vouchers
        except Exception as msg:
            cr.close()
            print (msg)
            raise
        
    def save_captured_other_service_voucher_data(self,lst_os):
        
        cr = self.create_cursor()
        
        lst_other_vouchers = [ins_service_base.str_voucher_number for ins_service_base in lst_os]
        str_tim_created = ins_general_methods.get_current_date_time()
        
        lst_other_voucher_data = []
        lst_os_sale_details = []
        lst_other_extra_capturing_fields = []   #refer #40299
        for ins_service_base in lst_os :
            
            if ins_service_base.int_card_payment_type == 1  :
                ins_service_base.flt_fare_uccf_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_uccf_amount = ins_service_base.flt_total_tax_credit_card_inv 
            else :
                ins_service_base.flt_fare_card_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_card_amount = ins_service_base.flt_total_tax_credit_card_inv 
                
            
            lst_other_voucher_data.append((
                ins_service_base.str_voucher_number ,
                ins_service_base.str_pnr_no ,
                'I' ,
                'P' ,
                None,
                None,
                ins_service_base.str_pax_name ,
                ins_service_base.int_service_id ,
                ins_service_base.str_crs_company ,
                "",
                ins_service_base.int_booking_staff_id,
                ins_service_base.str_from_date,
                ins_service_base.str_to_date,
                ins_service_base.dat_rm_lpo_date,
                ins_service_base.str_customer_lpo_number ,
                ins_service_base.str_customer_cost_centre ,
                ins_service_base.str_customer_emp_no ,
                "",
                ins_service_base.str_voucher_booking_date ,
                ins_service_base.str_voucher_issue_date ,
                ins_service_base.int_counter_staff_id ,
                ins_service_base.int_no_of_guest_inv ,
                ins_service_base.str_particulars ,
                ins_service_base.str_supplier_confirm_number,
                ins_service_base.int_location_id ,
                ins_service_base.int_customer_account_id_inv ,
                ins_service_base.int_supplier_id ,
                ins_service_base.int_corp_card_id ,
                None,
                ins_service_base.int_card_payment_type ,
                ins_service_base.str_remarks ,
                ins_service_base.str_file_name_inv ,
                ins_service_base.json_extra_capturing_data,
                2 ,
                str_tim_created  

                ))
        
        
        try:
            cr.executemany("""
                    INSERT INTO tbl_other_voucher
                    (
                        vchr_voucher_number,
                        vchr_pnr_number,
                        chr_voucher_status,
                        chr_system_sale_part_action,
                        fk_bint_country_id,
                        fk_bint_city_id,
                        vchr_pax_name,
                        fk_bint_service_id,
                        vchr_gds_company,
                        chr_voucher_source,
                        fk_bint_booking_staff_id,
                        tim_from,
                        tim_to,
                        dat_customer_lpo_date,
                        vchr_customer_lpo_or_to_number,
                        vchr_customer_cost_centre,
                        vchr_customer_employee_number,
                        vchr_booking_details,
                        dat_booking,
                        dat_issue,
                        fk_bint_counter_staff_id_inv,
                        int_no_of_pax_inv,
                        vchr_particulars_inv,
                        vchr_supplier_reference_inv,
                        fk_bint_branch_id_inv,
                        fk_bint_customer_account_id_inv,
                        fk_bint_supplier_account_id_inv,
                        fk_bint_corporate_card_account_id_inv,
                        fk_bint_agency_corporate_card_account_id_inv,
                        int_card_payment_type_inv,
                        vchr_remarks_inv,
                        vchr_gds_file_name_inv,
                        json_extra_capturing_data,
                        fk_bint_created_user_id,
                        tim_created

                    )
                    VALUES
                    (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s
                    )

                    """,lst_other_voucher_data)


            cr.close()
            
            lst_voucher_fare_details = self.create_fare_data_list_for_voucher(ins_service_base)
            #save data fare_details
            self.save_fare_details_data(lst_voucher_fare_details)
            
            #save log data
            for str_vocuher in lst_other_vouchers :
                self.save_other_voucher_log_data(str_vocuher,str_tim_created,'O')
                
            return lst_other_vouchers
        except Exception as msg:
            cr.close()
            print (msg)
            raise
        
    
    def save_booking_ticket_data(self, lst_ticket_capture_details): # refer 25579
        cr = self.create_cursor()
        
        str_current_date_time = ins_general_methods.get_current_date_time()
        
        int_booking_details_id = ins_general_methods.get_max_value('tbl_booking_details', 'pk_bint_booking_details_id')

        lst_booking_details = []
        lst_booking_sector_data = []
        
        
        
        for ins_ticket_base in lst_ticket_capture_details:
            
            if not ins_ticket_base.str_ticketing_agency_iata_no:
                ins_ticket_base.str_ticketing_agency_iata_no = ins_ticket_base.str_booking_agency_iata_no
            
            if ins_ticket_base.str_ticketing_agency_iata_no :
                cr.execute(""" SELECT pk_bint_account_id
                                FROM tbl_account  ac 
                                INNER JOIN tbl_partner pt 
                                        ON ac.pk_bint_account_id = pt.fk_bint_partner_account_id
                                        AND pt.chr_document_status = 'N'
                                INNER JOIN tbl_supplier_info si
                                        ON pt.pk_bint_partner_id = si.fk_bint_partner_id
                                WHERE substring(si.vchr_iata_no,1,7) = %s
                                        AND %s = ANY(pt.arr_currency)
                                        AND ac.chr_document_status = 'N' """,
                         
                         ( ins_ticket_base.str_ticketing_agency_iata_no[0:7],
                           ins_ticket_base.str_defult_currency_code))

                rst = cr.fetchone()
                if rst:
                    ins_ticket_base.int_supplier_id = rst['pk_bint_account_id']
#                    if rst['vchr_currency_code'] == ins_ticket_base.str_defult_currency_code :
#                        ins_ticket_base.int_supplier_id = rst['fk_bint_creditor_account_id']
#                        ins_ticket_base.chr_supplier_type = 'S'
#                        pass
#                    else :
#                        ins_ticket_base.int_supplier_id = None
#                        ins_ticket_base.chr_supplier_type = ''
                        
            
            ins_ticket_base.flt_supplier_currency_roe = 1

            if ins_ticket_base.str_ticket_issue_date and ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                ins_ticket_base.flt_supplier_currency_roe = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_ticket_issue_date)
            

            ins_ticket_base.chr_ticket_status = 'B'                
            lst_booking_details.append([int_booking_details_id,
                                        ins_ticket_base.int_location_id,
                                        ins_ticket_base.str_pnr_no,
                                        ins_ticket_base.str_ticket_number,
                                        ins_ticket_base.str_last_conjection_ticket_number,
                                        ins_ticket_base.str_original_issue,
                                        ins_ticket_base.str_lpo_number,
                                        
                                        
                                        ins_ticket_base.int_dc_card,
                                        ins_ticket_base.int_supplier_id,
                                        ins_ticket_base.str_ticket_issue_date,
                                        
                                        ins_ticket_base.str_booking_agent_code,
                                        ins_ticket_base.str_booking_agent_numeric_code,
                                        ins_ticket_base.str_ticketing_agent_code,
                                        ins_ticket_base.str_ticketing_agent_numeric_code,
                                        ins_ticket_base.int_airline_id,
                                        ins_ticket_base.int_airline_account_id,
                                        ins_ticket_base.int_region_id,
                                        ins_ticket_base.str_sector,
                                        ins_ticket_base.int_no_of_segments,
                                        ins_ticket_base.str_class,
                                        ins_ticket_base.int_class_id,
                                        ins_ticket_base.str_class_group,
                                        ins_ticket_base.str_return_class,
                                        ins_ticket_base.int_return_class_id,
                                        ins_ticket_base.str_return_class_group,
                                        ins_ticket_base.str_tour_code,
                                        ins_ticket_base.str_fare_basis,
                                        ins_ticket_base.str_fare_construction,
                                        ins_ticket_base.str_ticket_designator,
                                        ins_ticket_base.str_crs_company,
                                        ins_ticket_base.str_pax_type,
                                        ins_ticket_base.str_pax_name,
                                        ins_ticket_base.str_remarks,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_published_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_published_fare_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_market_fare_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_special_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_special_fare_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_total_tax_inv,
                                        ins_ticket_base.str_tax_details,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_selling_price,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_selling_price,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_service_charge,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_service_charge,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_standard_commission,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_standard_commission,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amount,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_supplier_amount,
                                        ins_ticket_base.flt_std_commn_percentage_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_discount_given_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_discount_given_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_fare_differece,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_fare_differece,
                                         
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amt_as_per_bsp_issue,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_supplier_amt_as_per_bsp_issue,
                                        ins_ticket_base.int_branch_id,
                                        ins_ticket_base.str_booking_agency_iata_no,
                                        ins_ticket_base.str_ticketing_agency_iata_no,
                                        ins_ticket_base.str_travel_date,
                                        ins_ticket_base.int_account_master_id,
                                        ins_ticket_base.str_return_date,
                                        ins_ticket_base.str_booking_agency_office_id,
                                        ins_ticket_base.str_pnr_first_owner_office_id,
                                        ins_ticket_base.str_pnr_current_owner_office_id,
                                        ins_ticket_base.str_ticketing_agency_office_id,
                                        ins_ticket_base.str_ticket_booking_date,
                                        ins_ticket_base.str_file_name_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_market_fare_cash_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_market_fare_credit_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_total_tax_cash_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_total_tax_credit_inv,
                                        ins_ticket_base.str_base_currency,
                                        ins_ticket_base.str_defult_currency_code,
                                        ins_ticket_base.flt_supplier_currency_roe,
                                        ins_ticket_base.int_company_id,
                                        2 , #fk_bint_created_user_id
                                        str_current_date_time,
                                        ins_ticket_base.str_destination,
                                        ins_ticket_base.str_cust_cost_centre,
                                        
                                        ins_ticket_base.int_no_of_pax_inv,
                                        ins_ticket_base.str_employee_number,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_net_payable_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_net_payable_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_actual_cost_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_actual_cost_inv,
                                        ins_ticket_base.str_card_approval_code,
                                        ins_ticket_base.int_card_payment_type,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_debited_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_debited_amount_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_profit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_profit_inv,
                                        ins_ticket_base.str_connection_ticket ,
                                        ins_ticket_base.int_profile_id ,
                                        ins_ticket_base.str_agency_adv_receipt_no ,
                                        ins_ticket_base.int_discount_account_id_inv ,
                                        ins_ticket_base.flt_discount_given_percentage_inv ,
                                        ins_ticket_base.int_extra_earning_account_id_inv ,
                                        ins_ticket_base.flt_extra_earninig_percentage_inv ,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_extra_earning_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_extra_earning_inv ,
                                        ins_ticket_base.int_counter_staff_id_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_market_fare_inv,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_cc_charge_collected_ext,
                                        ins_ticket_base.int_booking_agent_counter_staff_id ,
                                       
                                      
                                        ins_ticket_base.str_cc_number ,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_card_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_market_fare_card_amount_inv ,
                                        ins_general_methods.convert_amount(ins_ticket_base.flt_tax_card_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                        ins_ticket_base.flt_tax_card_amount_inv ,
                                        ins_ticket_base.str_crm_reference, # Refer 23236 
                                        ins_ticket_base.dat_rm_lpo_date
                                ])
                                    
                                    
            vchr_ticket_no = ins_ticket_base.str_ticket_number
            ins_ticket_base.str_return_date  = None
            int_sector_count = 0
            
            for lst_sec in ins_ticket_base.lst_sector_details:
                [str_orgin_airport_code,
                        str_dest_code,
                        str_airline_code,
                        str_airline_no,
                        str_flight_number,
                        str_class_of_service,
                        str_class_of_booking,
                        str_arrival_date,
                        str_departure_date,
                        bln_stopover_permitted,
                        int_mileage,
                        dbl_sector_wise_fare,
                        str_arrival_time,
                        str_departure_time,
                        bln_open_segment] = lst_sec
                int_sector_count += 1

                

                if str_airline_code != 'VOID': # if the segment is void we are not adding the it to the table

                                                                    # but it is counted as a segment and the conjunction is created if needed
                    if ins_ticket_base.str_destination == str_orgin_airport_code and not ins_ticket_base.str_return_date:

                                ins_ticket_base.str_return_date = str_departure_date
                                ins_ticket_base.str_return_class = str_class_of_booking

                                if ins_ticket_base.str_return_class :
                                    ins_ticket_base.int_return_class_id,ins_ticket_base.str_return_class_group = ins_general_methods.get_class_id('%' + ins_ticket_base.str_return_class + '%',ins_ticket_base.int_airline_account_id)
                                else :
                                    ins_ticket_base.int_return_class_id = None
                                    
                    lst_booking_sector_data.append([
                                                vchr_ticket_no,
                                                str_airline_no,
                                                str_airline_code,
                                                str_flight_number,
                                                str_arrival_date,
                                                str_departure_date,
                                                str_orgin_airport_code,
                                                str_dest_code,
                                                ins_ticket_base.str_fare_basis,
                                                bln_stopover_permitted,
                                                str_class_of_service,
                                                str_class_of_booking,
                                                int_mileage,
                                                dbl_sector_wise_fare,
                                                str_arrival_time,
                                                str_departure_time,
                                                int_booking_details_id
                                                ])
                    
                    pass
            int_booking_details_id = int_booking_details_id + 1
        
        try:

                cr.execute("""SELECT vchr_pnr_number FROM tbl_booking_details WHERE UPPER(vchr_pnr_number) = %s""" , (ins_ticket_base.str_pnr_no.upper(),))
                rst_booking_data = cr.fetchone()
                if rst_booking_data:   
                    cr.execute("""DELETE FROM tbl_booking_sector_details WHERE UPPER(vchr_ticket_number) ILIKE %s""" , (ins_ticket_base.str_pnr_no.upper() + "-%",))
                    cr.execute("""DELETE FROM tbl_booking_details WHERE UPPER(vchr_pnr_number) = %s""" , (ins_ticket_base.str_pnr_no.upper(),))
                    

                
                if lst_booking_details:
                    cr.executemany("""INSERT INTO tbl_booking_details
                                        (pk_bint_booking_details_id,
                                        fk_bint_cost_center_id_inv,
                                        vchr_pnr_number,
                                        vchr_ticket_number,
                                        vchr_last_conjunction_ticket_number,
                                        vchr_original_issue,
                                        vchr_customer_lpo_or_to_number,
                                        
                                        
                                        fk_bint_corporate_card_account_id_inv,
                                        fk_bint_supplier_account_id_inv,
                                        dat_ticket_issue,
                                        
                                        vchr_booking_agent_code,
                                        vchr_booking_agent_numeric_code,
                                        vchr_ticketing_agent_code_inv,
                                        vchr_ticketing_agent_numeric_code_inv,
                                        fk_bint_airline_id,
                                        fk_bint_airline_account_id,
                                        fk_bint_airports_regions_id,
                                        vchr_sector_inv,
                                        int_no_of_segments_inv,
                                        vchr_class_chr,
                                        fk_bint_booking_class_id,
                                        vchr_class,
                                        vchr_return_class_chr,
                                        fk_bint_return_booking_class_id,
                                        vchr_return_class,
                                        vchr_tour_code,
                                        vchr_fare_basis,
                                        vchr_fare_construction,
                                        vchr_ticket_designator,
                                        vchr_gds_company,
                                        vchr_pax_type,
                                        vchr_pax_name_inv,
                                        vchr_remarks_inv,
                                        dbl_base_currency_published_fare_inv,
                                        dbl_tran_currency_published_fare_inv,
                                        dbl_base_currency_market_fare_inv,
                                        dbl_tran_currency_market_fare_inv,
                                        dbl_base_currency_special_fare_inv,
                                        dbl_tran_currency_special_fare_inv,
                                        dbl_base_currency_tax_inv,
                                        dbl_tran_currency_tax_inv,
                                        vchr_tax_details_inv,
                                        dbl_base_currency_selling_price_inv,
                                        dbl_tran_currency_selling_price_inv,
                                        dbl_base_currency_service_fee_inv,
                                        dbl_tran_currency_service_fee_inv,
                                        dbl_base_currency_std_commission_amount_inv,
                                        dbl_tran_currency_std_commission_amount_inv,
                                        dbl_base_currency_gross_payable_inv,
                                        dbl_tran_currency_gross_payable_inv,
                                        dbl_std_commission_percentage_inv,
                                        dbl_base_currency_discount_given_inv,
                                        dbl_tran_currency_discount_given_inv,
                                        dbl_base_currency_fare_differece_inv,
                                        dbl_tran_currency_fare_differece_inv,
                                        
                                        dbl_base_currency_supplier_bsp_file_amount_inv,
                                        dbl_tran_currency_supplier_bsp_file_amount_inv,
                                        fk_bint_department_id_inv,
                                        vchr_booking_agency_iata_no,
                                        vchr_ticketing_agency_iata_no,
                                        dat_travel_date,
                                        fk_bint_customer_account_id_inv,
                                        dat_of_return,
                                        vchr_booking_agency_office_id,
                                        vchr_pnr_first_owner_office_id,
                                        vchr_pnr_current_owner_office_id,
                                        vchr_ticketing_agency_office_id,
                                        dat_booking,
                                        vchr_gds_file_name_inv,
                                        dbl_base_currency_market_fare_credit_inv,
                                        dbl_tran_currency_market_fare_credit_inv,
                                        dbl_base_currency_market_fare_credit_card_inv,
                                        dbl_tran_currency_market_fare_credit_card_inv,
                                        dbl_base_currency_tax_credit_inv,
                                        dbl_tran_currency_tax_credit_inv,
                                        dbl_base_currency_tax_credit_card_inv,
                                        dbl_tran_currency_tax_credit_card_inv,
                                        vchr_base_currency_inv,
                                        vchr_tran_currency_inv,
                                        dbl_tran_currency_roe_inv,
                                        fk_bint_company_id_inv,
                                        fk_bint_created_user_id,
                                        tim_created,
                                        vchr_destination_airport,
                                        vchr_customer_cost_centre,
                                        int_no_of_pax_inv,
                                        vchr_customer_employee_number,
                                        dbl_base_currency_net_payable_inv,
                                        dbl_tran_currency_net_payable_inv,
                                        dbl_base_currency_actual_cost_inv,
                                        dbl_tran_currency_actual_cost_inv,
                                        vchr_card_approval_code,
                                        int_card_payment_type,
                                        dbl_base_currency_debited_amount_inv,
                                        dbl_tran_currency_debited_amount_inv,
                                        dbl_base_currency_profit_inv ,
                                        dbl_tran_currency_profit_inv ,
                                        vchr_connection_ticket_number ,
                                        fk_bint_profile_id ,
                                        vchr_advance_receipt_number ,                                                                                                                     
                                        fk_bint_discount_account_id_inv ,
                                        dbl_discount_given_percentage_inv ,                                      
                                        fk_bint_extra_earning_account_id_inv ,
                                        dbl_extra_earning_percentage_inv ,
                                        dbl_base_currency_extra_earning_inv ,
                                        dbl_tran_currency_extra_earning_inv ,                                        
                                        fk_bint_counter_staff_id_inv ,                                   
                                        dbl_base_currency_printing_fare_inv ,
                                        dbl_tran_currency_printing_fare_inv ,                                     
                                        dbl_base_currency_cc_charge_collected_inv ,
                                        dbl_tran_currency_cc_charge_collected_inv,
                                        fk_bint_booking_agent_counter_staff_id,
                                        
                                        
                                        vchr_cc_number,
                                        dbl_base_currency_market_fare_card_amount_inv, 	
                                        dbl_tran_currency_market_fare_card_amount_inv,                                     
                                        dbl_base_currency_tax_card_amount_inv, 	
                                        dbl_tran_currency_tax_card_amount_inv,
                                        vchr_crm_reference,
                                        dat_customer_lpo_date
                                        )
                                        
                                        VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s,%s
                                               
                                             )""" ,lst_booking_details)
                                             
                
                if lst_booking_sector_data:
                    cr.executemany("""INSERT INTO tbl_booking_sector_details
                                    ( 
                                      vchr_ticket_number,
                                      vchr_airline_number,
                                      vchr_airline_chr_code,
                                      vchr_flight_no,
                                      dat_arrival_date,
                                      dat_departure_date,
                                      vchr_origin_code,
                                      vchr_destination_code,
                                      vchr_fare_basis,
                                      bln_stopover_permitted,
                                      vchr_class_of_service,
                                      vchr_class_of_booking,
                                      bint_mileage,
                                      dbl_tran_currency_sector_wise_fare,
                                      vchr_departure_time,
                                      vchr_arrival_time,
                                      fk_bint_booking_details_id

                                      )
                                    VALUES(%s, %s, %s, %s,
                                           %s, %s, %s, %s,
                                           %s, %s, %s, %s,
                                           %s, %s, %s, %s, 
                                           %s)""" ,lst_booking_sector_data)


                cr.execute("""SELECT setval('tbl_booking_details_pk_bint_booking_details_id_seq'::regclass, %s)""",(int_booking_details_id,))
        except Exception as msg:
#            if bln_raise :
#            	raise
            OperationalError.ins_ticket_base = ins_ticket_base
            raise OperationalError
        pass   
    
    
    def update_refund_sides_when_ticket_void(self,str_ticket_number):

        cr = self.create_cursor()

        cr.execute("""UPDATE tbl_ticket
                        SET dbl_std_commission_percentage_rfd = dbl_std_commission_percentage_inv ,
                            dbl_service_fee_percentage_rfd = dbl_service_fee_percentage_inv ,
                            dbl_extra_earning_percentage_rfd = dbl_extra_earning_percentage_inv ,
                            dbl_payback_service_percentage_rfd = dbl_payback_service_percentage_inv,
                            dbl_discount_given_percentage_rfd = dbl_discount_given_percentage_inv,
                            dbl_billing_tax_percentage_rfd = dbl_billing_tax_percentage_inv,
                            dbl_cc_charge_collected_percentage_rfd = dbl_cc_charge_collected_percentage_inv,
                            dbl_base_currency_published_fare_rfd = dbl_base_currency_published_fare_inv ,
                            dbl_base_currency_market_fare_rfd = dbl_base_currency_market_fare_inv ,
                            dbl_base_currency_market_fare_credit_card_rfd = dbl_base_currency_market_fare_credit_card_inv ,
                            dbl_base_currency_market_fare_credit_rfd = dbl_base_currency_market_fare_credit_inv ,
                            dbl_base_currency_special_fare_rfd = dbl_base_currency_special_fare_inv ,
                            dbl_base_currency_printing_fare_rfd = dbl_base_currency_printing_fare_inv ,
                            dbl_base_currency_tax_rfd = dbl_base_currency_tax_inv,
                            dbl_base_currency_tax_credit_card_rfd = dbl_base_currency_tax_credit_card_inv,
                            dbl_base_currency_tax_credit_rfd = dbl_base_currency_tax_credit_inv,
                            dbl_base_currency_supplier_fee_rfd = dbl_base_currency_supplier_fee_inv,
                            dbl_base_currency_std_commission_amount_rfd = dbl_base_currency_std_commission_amount_inv,
                            dbl_base_currency_gross_payable_rfd = dbl_base_currency_gross_payable_inv,
                            dbl_base_currency_inv_net_payable_rfd = dbl_base_currency_net_payable_inv,
                            dbl_base_currency_actual_cost_rfd = dbl_base_currency_actual_cost_inv,
                            dbl_base_currency_supplier_bsp_file_amount_rfd = dbl_base_currency_supplier_bsp_file_amount_inv,
                            dbl_base_currency_service_fee_rfd = dbl_base_currency_service_fee_inv,
                            dbl_base_currency_extra_earning_rfd = dbl_base_currency_extra_earning_inv,
                            dbl_base_currency_payback_service_fee_rfd = dbl_base_currency_payback_service_fee_inv,
                            dbl_base_currency_iata_insurance_payable_rfd = dbl_base_currency_iata_insurance_payable_inv,
                            dbl_base_currency_iata_insurance_collected_rfd = dbl_base_currency_iata_insurance_collected_inv,
                            dbl_base_currency_discount_given_rfd = dbl_base_currency_discount_given_inv,
                            dbl_base_currency_billing_tax_rfd = dbl_base_currency_billing_tax_inv,
                            dbl_base_currency_cc_charge_collected_rfd = dbl_base_currency_cc_charge_collected_inv,
                            dbl_base_currency_selling_price_rfd = dbl_base_currency_selling_price_inv,
                            dbl_base_currency_client_net_rfd = dbl_base_currency_selling_price_inv,
                            dbl_base_currency_credited_amount_rfd = dbl_base_currency_debited_amount_inv,
                            dbl_base_currency_debited_amount_rfd = dbl_base_currency_net_payable_inv,
                            dbl_base_currency_supplier_net_rfd = dbl_base_currency_gross_payable_inv ,
                            dbl_base_currency_profit_rfd = dbl_base_currency_profit_inv*-1,
                            dbl_base_currency_fare_differece_rfd = dbl_base_currency_fare_differece_inv,
                            dbl_base_currency_adm_expect_rfd = dbl_base_currency_adm_expect_inv,
                            dbl_base_currency_acm_expect_rfd = dbl_base_currency_acm_expect_inv,

                            dbl_tran_currency_roe_rfd = dbl_tran_currency_roe_inv,
                            dbl_tran_currency_published_fare_rfd = dbl_tran_currency_published_fare_inv,
                            dbl_tran_currency_market_fare_rfd = dbl_tran_currency_market_fare_inv,
                            dbl_tran_currency_market_fare_credit_card_rfd = dbl_tran_currency_market_fare_credit_card_inv,
                            dbl_tran_currency_market_fare_credit_rfd = dbl_tran_currency_market_fare_credit_inv,
                            dbl_tran_currency_special_fare_rfd = dbl_tran_currency_special_fare_inv,
                            dbl_tran_currency_printing_fare_rfd = dbl_tran_currency_printing_fare_inv,
                            dbl_tran_currency_tax_rfd  = dbl_tran_currency_tax_inv,
                            dbl_tran_currency_tax_credit_card_rfd = dbl_tran_currency_tax_credit_card_inv,
                            dbl_tran_currency_tax_credit_rfd = dbl_tran_currency_tax_credit_inv,
                            dbl_tran_currency_supplier_fee_rfd = dbl_tran_currency_supplier_fee_inv,
                            dbl_tran_currency_std_commission_amount_rfd = dbl_tran_currency_std_commission_amount_inv,
                            dbl_tran_currency_gross_payable_rfd = dbl_tran_currency_gross_payable_inv,
                            dbl_tran_currency_inv_net_payable_rfd = dbl_tran_currency_net_payable_inv,
                            dbl_tran_currency_actual_cost_rfd = dbl_tran_currency_actual_cost_inv,
                            dbl_tran_currency_supplier_bsp_file_amount_rfd = dbl_tran_currency_supplier_bsp_file_amount_inv,
                            dbl_tran_currency_service_fee_rfd = dbl_tran_currency_service_fee_inv,
                            dbl_tran_currency_payback_service_fee_rfd = dbl_tran_currency_payback_service_fee_inv,
                            dbl_tran_currency_iata_insurance_payable_rfd = dbl_tran_currency_iata_insurance_payable_inv,
                            dbl_tran_currency_iata_insurance_collected_rfd = dbl_tran_currency_iata_insurance_collected_inv,
                            dbl_tran_currency_discount_given_rfd = dbl_tran_currency_discount_given_inv,
                            dbl_tran_currency_billing_tax_rfd = dbl_tran_currency_billing_tax_inv,
                            dbl_tran_currency_cc_charge_collected_rfd = dbl_tran_currency_cc_charge_collected_inv,
                            dbl_tran_currency_selling_price_rfd = dbl_tran_currency_selling_price_inv,

                            dbl_tran_currency_profit_rfd = dbl_tran_currency_profit_inv*-1,
                            dbl_tran_currency_fare_differece_rfd = dbl_tran_currency_fare_differece_inv,
                            dbl_tran_currency_adm_expect_rfd = dbl_tran_currency_adm_expect_inv,
                            dbl_tran_currency_acm_expect_rfd = dbl_tran_currency_acm_expect_inv,
                            dbl_trancurrency_extra_earning_rfd = dbl_tran_currency_extra_earning_inv ,


                            dbl_tran_currency_client_net_rfd = dbl_tran_currency_selling_price_inv,
                            dbl_tran_currency_credited_amount_rfd = dbl_tran_currency_debited_amount_inv,
                            dbl_tran_currency_debited_amount_rfd = dbl_tran_currency_net_payable_inv,
                            dbl_tran_currency_supplier_net_rfd = dbl_tran_currency_gross_payable_inv ,
                            int_card_payment_type_rfd = int_card_payment_type_inv ,
                            
                            dbl_base_currency_market_fare_card_amount_rfd = dbl_base_currency_market_fare_card_amount_inv , 	
                            dbl_base_currency_market_fare_uccf_amount_rfd = dbl_base_currency_market_fare_uccf_amount_inv , 	
                            dbl_tran_currency_market_fare_card_amount_rfd = dbl_tran_currency_market_fare_card_amount_inv , 	
                            dbl_tran_currency_market_fare_uccf_amount_rfd = dbl_tran_currency_market_fare_uccf_amount_inv , 	
                            dbl_base_currency_tax_card_amount_rfd = dbl_base_currency_tax_card_amount_inv , 	
                            dbl_base_currency_tax_uccf_amount_rfd = dbl_base_currency_tax_uccf_amount_inv , 	
                            dbl_tran_currency_tax_card_amount_rfd = dbl_tran_currency_tax_card_amount_inv , 	
                            dbl_tran_currency_tax_uccf_amount_rfd = dbl_tran_currency_tax_uccf_amount_inv,
                            dbl_base_currency_vat_in_rfd = dbl_base_currency_vat_in_inv,
                            dbl_tran_currency_vat_in_rfd = dbl_tran_currency_vat_in_inv


                        WHERE vchr_ticket_number = %s""",(str_ticket_number,))

#                        dbl_tran_currency_debited_amount_rfd = dbl_tran_currency_debited_amount_inv,
        cr.execute("""UPDATE tbl_ticket_refund tr
                        SET dbl_std_commission_percentage_rfd = tkt.dbl_std_commission_percentage_inv ,
                            dbl_service_fee_percentage_rfd = tkt.dbl_service_fee_percentage_inv ,
                            dbl_extra_earning_percentage_rfd = tkt.dbl_extra_earning_percentage_inv ,
                            dbl_payback_service_percentage_rfd = tkt.dbl_payback_service_percentage_inv,
                            dbl_discount_given_percentage_rfd = tkt.dbl_discount_given_percentage_inv,
                            dbl_billing_tax_percentage_rfd = tkt.dbl_billing_tax_percentage_inv,
                            dbl_base_currency_published_fare_rfd = tkt.dbl_base_currency_published_fare_inv ,
                            dbl_base_currency_market_fare_rfd = tkt.dbl_base_currency_market_fare_inv ,
                            dbl_base_currency_market_fare_credit_card_rfd = tkt.dbl_base_currency_market_fare_credit_card_inv ,
                            dbl_base_currency_market_fare_credit_rfd = tkt.dbl_base_currency_market_fare_credit_inv ,
                            dbl_base_currency_special_fare_rfd = tkt.dbl_base_currency_special_fare_inv ,
                            dbl_base_currency_printing_fare_rfd = tkt.dbl_base_currency_printing_fare_inv ,
                            dbl_base_currency_tax_rfd = tkt.dbl_base_currency_tax_inv,
                            dbl_base_currency_tax_credit_card_rfd = tkt.dbl_base_currency_tax_credit_card_inv,
                            dbl_base_currency_tax_credit_rfd = tkt.dbl_base_currency_tax_credit_inv,
                            dbl_base_currency_supplier_fee_rfd = tkt.dbl_base_currency_supplier_fee_inv,
                            dbl_base_currency_std_commission_amount_rfd = tkt.dbl_base_currency_std_commission_amount_inv,
                            dbl_base_currency_gross_payable_rfd = tkt.dbl_base_currency_gross_payable_inv,
                            dbl_base_currency_inv_net_payable_rfd = tkt.dbl_base_currency_net_payable_inv,
                            dbl_base_currency_actual_cost_rfd = tkt.dbl_base_currency_actual_cost_inv,
                            dbl_base_currency_supplier_bsp_file_amount_rfd = tkt.dbl_base_currency_supplier_bsp_file_amount_inv,
                            dbl_base_currency_service_fee_rfd = tkt.dbl_base_currency_service_fee_inv,
                            dbl_base_currency_extra_earning_rfd = tkt.dbl_base_currency_extra_earning_inv,
                            dbl_base_currency_payback_service_fee_rfd = tkt.dbl_base_currency_payback_service_fee_inv,
                            dbl_base_currency_iata_insurance_payable_rfd = tkt.dbl_base_currency_iata_insurance_payable_inv,
                            dbl_base_currency_iata_insurance_collected_rfd = tkt.dbl_base_currency_iata_insurance_collected_inv,
                            dbl_base_currency_discount_given_rfd = tkt.dbl_base_currency_discount_given_inv,
                            dbl_base_currency_billing_tax_rfd = tkt.dbl_base_currency_billing_tax_inv,
                            dbl_base_currency_selling_price_rfd = tkt.dbl_base_currency_selling_price_inv,
                            dbl_base_currency_client_net_rfd = tkt.dbl_base_currency_selling_price_inv,
                            dbl_base_currency_credited_amount_rfd = tkt.dbl_base_currency_debited_amount_inv,
                            dbl_base_currency_debited_amount_rfd = tkt.dbl_base_currency_net_payable_inv,
                            dbl_base_currency_supplier_net_rfd = tkt.dbl_base_currency_gross_payable_inv ,
                            dbl_base_currency_profit_rfd = tkt.dbl_base_currency_profit_inv*-1,
                            dbl_base_currency_fare_differece_rfd = tkt.dbl_base_currency_fare_differece_inv,
                            dbl_base_currency_adm_expect_rfd = tkt.dbl_base_currency_adm_expect_inv,
                            dbl_base_currency_acm_expect_rfd = tkt.dbl_base_currency_acm_expect_inv,
                            dbl_tran_currency_roe_rfd = tkt.dbl_tran_currency_roe_inv,
                            dbl_tran_currency_published_fare_rfd = tkt.dbl_tran_currency_published_fare_inv,
                            dbl_tran_currency_market_fare_rfd = tkt.dbl_tran_currency_market_fare_inv,
                            dbl_tran_currency_market_fare_credit_card_rfd = tkt.dbl_tran_currency_market_fare_credit_card_inv,
                            dbl_tran_currency_market_fare_credit_rfd = tkt.dbl_tran_currency_market_fare_credit_inv,
                            dbl_tran_currency_special_fare_rfd = tkt.dbl_tran_currency_special_fare_inv,
                            dbl_tran_currency_printing_fare_rfd = tkt.dbl_tran_currency_printing_fare_inv,
                            dbl_tran_currency_tax_rfd  = tkt.dbl_tran_currency_tax_inv,
                            dbl_tran_currency_tax_credit_card_rfd = tkt.dbl_tran_currency_tax_credit_card_inv,
                            dbl_tran_currency_tax_credit_rfd = tkt.dbl_tran_currency_tax_credit_inv,
                            dbl_tran_currency_supplier_fee_rfd = tkt.dbl_tran_currency_supplier_fee_inv,
                            dbl_tran_currency_std_commission_amount_rfd = tkt.dbl_tran_currency_std_commission_amount_inv,
                            dbl_tran_currency_gross_payable_rfd = tkt.dbl_tran_currency_gross_payable_inv,
                            dbl_tran_currency_inv_net_payable_rfd = tkt.dbl_tran_currency_net_payable_inv,
                            dbl_tran_currency_actual_cost_rfd = tkt.dbl_tran_currency_actual_cost_inv,
                            dbl_tran_currency_supplier_bsp_file_amount_rfd = tkt.dbl_tran_currency_supplier_bsp_file_amount_inv,
                            dbl_tran_currency_service_fee_rfd = tkt.dbl_tran_currency_service_fee_inv,
                            dbl_tran_currency_payback_service_fee_rfd = tkt.dbl_tran_currency_payback_service_fee_inv,
                            dbl_tran_currency_iata_insurance_payable_rfd = tkt.dbl_tran_currency_iata_insurance_payable_inv,
                            dbl_tran_currency_iata_insurance_collected_rfd = tkt.dbl_tran_currency_iata_insurance_collected_inv,
                            dbl_tran_currency_discount_given_rfd = tkt.dbl_tran_currency_discount_given_inv,
                            dbl_tran_currency_billing_tax_rfd = tkt.dbl_tran_currency_billing_tax_inv,
                            dbl_tran_currency_selling_price_rfd = tkt.dbl_tran_currency_selling_price_inv,

                            dbl_tran_currency_profit_rfd = tkt.dbl_tran_currency_profit_inv*-1,
                            dbl_tran_currency_fare_differece_rfd = tkt.dbl_tran_currency_fare_differece_inv,
                            dbl_tran_currency_adm_expect_rfd = tkt.dbl_tran_currency_adm_expect_inv,
                            dbl_tran_currency_acm_expect_rfd = tkt.dbl_tran_currency_acm_expect_inv,
                            dbl_trancurrency_extra_earning_rfd = tkt.dbl_tran_currency_extra_earning_inv ,

                            dbl_tran_currency_client_net_rfd = tkt.dbl_tran_currency_selling_price_inv,
                            dbl_tran_currency_credited_amount_rfd = tkt.dbl_tran_currency_debited_amount_inv,
                            dbl_tran_currency_debited_amount_rfd = tkt.dbl_tran_currency_net_payable_inv,
                            dbl_tran_currency_supplier_net_rfd = tkt.dbl_tran_currency_gross_payable_inv ,
                            int_card_payment_type = tkt.int_card_payment_type,
                            
                            dbl_base_currency_market_fare_card_amount_rfd = tkt.dbl_base_currency_market_fare_card_amount_inv , 	
                            dbl_base_currency_market_fare_uccf_amount_rfd = tkt.dbl_base_currency_market_fare_uccf_amount_inv , 	
                            dbl_tran_currency_market_fare_card_amount_rfd = tkt.dbl_tran_currency_market_fare_card_amount_inv , 	
                            dbl_tran_currency_market_fare_uccf_amount_rfd = tkt.dbl_tran_currency_market_fare_uccf_amount_inv , 	
                            dbl_base_currency_tax_card_amount_rfd = tkt.dbl_base_currency_tax_card_amount_inv , 	
                            dbl_base_currency_tax_uccf_amount_rfd = tkt.dbl_base_currency_tax_uccf_amount_inv , 	
                            dbl_tran_currency_tax_card_amount_rfd = tkt.dbl_tran_currency_tax_card_amount_inv , 	
                            dbl_tran_currency_tax_uccf_amount_rfd = tkt.dbl_tran_currency_tax_uccf_amount_inv,
                            dbl_base_currency_vat_in_rfd = tkt.dbl_base_currency_vat_in_inv,
                            dbl_tran_currency_vat_in_rfd = tkt.dbl_tran_currency_vat_in_inv

                        FROM tbl_ticket tkt
                        WHERE tr.fk_bint_ticket_id = tkt.pk_bint_ticket_id
                        AND tr.chr_system_refund_part_action = 'I'
                        AND tkt.chr_system_refund_part_action = 'I'
                        AND tr.chr_ticket_status = 'V'
                        AND tkt.chr_ticket_status = 'V'
                        AND tr.chr_document_status = 'N'

                        AND tr.vchr_ticket_number = %s """,(str_ticket_number,))
                        
        cr.close()
        pass
    
    def save_void_ticket(self, ins_ticket_base):
        cr = self.create_cursor()
        
        #36781
#        if ins_general_methods.check_ticket_already_exists_in_tbl_ticket_refund_or_not(ins_ticket_base.str_ticket_number):
#            print(('\n\nDuplication on refund:',ins_ticket_base.str_ticket_number,'\n\n'))
#            raise
        
        str_current_date_time = ins_general_methods.get_current_date_time()

        cr.execute("""UPDATE tbl_ticket SET chr_ticket_status = 'V',
                                            chr_system_refund_part_action = 'I',
                                            vchr_gds_file_name_rfd = %s,
                                            vchr_ticketing_agent_code_rfd = %s,
                                            vchr_ticketing_agent_numeric_code_rfd = %s,
                                            dat_refund = %s,
                                            int_no_of_segments_rfd = %s,
                                            vchr_sector_rfd = %s,
                                            fk_bint_ticketing_agent_id_rfd = %s,
					    fk_bint_branch_id_rfd = %s,
                                            fk_bint_customer_account_id_rfd = %s,
                                            fk_bint_supplier_account_id_rfd = %s,
                                            fk_bint_corporate_card_account_id_rfd = %s,
                                            int_card_payment_type_rfd = %s,
                                            vchr_remarks_rfd = %s,
                                            int_distribution_type_rfd = %s,
                                            fk_bint_modified_user_id = 2,
                                            tim_modified = now()

                        WHERE vchr_ticket_number = %s
                            AND chr_system_refund_part_action = '' 
                            AND COALESCE(vchr_document_no_rfd,'') = '' """,
                        (
                        ins_ticket_base.str_file_name_rfd,
                        ins_ticket_base.str_ticketing_agent_code_rfd,
                        ins_ticket_base.str_ticketing_agent_numeric_code_rfd,
                        ins_ticket_base.str_refund_date,
                        ins_ticket_base.int_number_of_segments_rfd ,
                        ins_ticket_base.str_sector ,
                        ins_ticket_base.int_counter_staff_id_rfd ,
                        ins_ticket_base.int_location_id ,
                        ins_ticket_base.int_account_master_id ,
                        ins_ticket_base.int_supplier_id_rfd ,
                        ins_ticket_base.int_corporate_card_id_rfd ,
                        ins_ticket_base.int_card_payment_type_rfd ,
                        ins_ticket_base.str_remarks_rfd,
                        ins_ticket_base.int_distribution_type_rfd,
                      
                        ins_ticket_base.str_ticket_number))

        if not cr.rowcount: #39067
            raise
        cr.execute("""INSERT INTO tbl_fare_details( 
                                            vchr_sup_doc_number,
                                            fk_bint_sup_doc_id,
                                            int_service_type,
                                            bln_sup_base_currency_data,
                                            bln_sup_currency_data,
                                            bln_cust_base_currency_data,
                                            bln_cust_currency_data,
                                            vchr_currency,
                                            dbl_roe,
                                            bln_refund_side ,
                                            fk_bint_payback_account_id_1,
                                            fk_bint_payback_account_id_2,
                                            fk_bint_payback_account_id_3,

                                            dbl_published_fare,
                                            dbl_special_fare,
                                            dbl_printing_fare,
                                            dbl_market_fare,
                                            dbl_market_fare_credit,
                                            dbl_market_fare_credit_card,
                                            dbl_market_fare_card_amount,
                                            dbl_market_fare_uccf_amount,
                                            dbl_tax,
                                            dbl_tax_credit,
                                            dbl_tax_credit_card,
                                            dbl_tax_card_amount,
                                            dbl_tax_uccf_amount,

                                            dbl_input_tax,
                                            dbl_payback_amount_percentage_1,
                                            dbl_payback_amount_1,
                                            dbl_income_amount_percentage_1,
                                            dbl_income_amount_1,
                                            dbl_income_amount_percentage_2,
                                            dbl_income_amount_2,
                                            dbl_income_amount_percentage_3,
                                            dbl_income_amount_3,
                                            dbl_expense_amount_percentage_1,
                                            dbl_expense_amount_1,
                                            dbl_adm_expect,
                                            dbl_client_amount,
                                            dbl_client_amount_posting,
                                            dbl_supplier_amount,
                                            dbl_supplier_amount_posting,
                                            dbl_profit,
                                            dbl_fare_difference,
                                            --dbl_supplier_bsp_file_amount,
                                            dbl_agency_charge_rfd,
                                            dbl_sup_charge_rfd,
                                            dbl_sup_charge_credit_rfd,
                                            dbl_sup_charge_card_amount_rfd,
                                            dbl_sup_charge_credit_card_rfd,
                                            dbl_sup_charge_uccf_amount_rfd
                                            --dbl_client_net_rfd,
                                            --dbl_supplier_net_rfd,
                                            --dbl_credited_amount_rfd
                                            )
                                    SELECT vchr_sup_doc_number,
                                            fk_bint_sup_doc_id,
                                            int_service_type,
                                            bln_sup_base_currency_data,
                                            bln_sup_currency_data,
                                            bln_cust_base_currency_data,
                                            bln_cust_currency_data,
                                            vchr_currency,
                                            dbl_roe,
                                            'TRUE' AS bln_refund_side ,
                                            fk_bint_payback_account_id_1,
                                            fk_bint_payback_account_id_2,
                                            fk_bint_payback_account_id_3,

                                            dbl_published_fare,
                                            dbl_special_fare,
                                            dbl_printing_fare,
                                            dbl_market_fare,
                                            dbl_market_fare_credit,
                                            dbl_market_fare_credit_card,
                                            dbl_market_fare_card_amount,
                                            dbl_market_fare_uccf_amount,
                                            dbl_tax,
                                            dbl_tax_credit,
                                            dbl_tax_credit_card,
                                            dbl_tax_card_amount,
                                            dbl_tax_uccf_amount,

                                            dbl_input_tax,
                                            dbl_payback_amount_percentage_1,
                                            dbl_payback_amount_1,
                                            dbl_income_amount_percentage_1,
                                            dbl_income_amount_1,
                                            dbl_income_amount_percentage_2,
                                            dbl_income_amount_2,
                                            dbl_income_amount_percentage_3,
                                            dbl_income_amount_3,
                                            dbl_expense_amount_percentage_1,
                                            dbl_expense_amount_1,
                                            dbl_adm_expect,
                                            dbl_client_amount,
                                            dbl_client_amount_posting,
                                            dbl_supplier_amount,
                                            dbl_supplier_amount_posting,
                                            dbl_profit,
                                            dbl_fare_difference, 
                                            --dbl_supplier_bsp_file_amount,
                                            dbl_agency_charge_rfd,
                                            dbl_sup_charge_rfd,
                                            dbl_sup_charge_credit_rfd,
                                            dbl_sup_charge_card_amount_rfd,
                                            dbl_sup_charge_credit_card_rfd,
                                            dbl_sup_charge_uccf_amount_rfd
                                            --dbl_client_net_rfd,
                                            --dbl_supplier_net_rfd,
                                            --dbl_credited_amount_rfd
                                        FROM tbl_fare_details 
                                        WHERE vchr_sup_doc_number = %s 
                                        ORDER BY pk_bint_fare_details_id """,(ins_ticket_base.str_ticket_number,))
        
        cr.close()
        #save ticket log into tbl_ticket_log
        self.save_ticket_log_data(ins_ticket_base.str_ticket_number,str_current_date_time,'REFUND')
        
        if ins_general_methods.bln_enable_non_iata_capture :
            ins_general_methods.ins_global.lst_process_list_void.append(ins_ticket_base)    #37364
#            thread = Thread(target = ins_general_methods.create_json_and_upload,args = [ins_ticket_base])
#            thread.start()
#            thread.join()

    def save_refund_tickets(self, ins_ticket_base):
        cr = self.create_cursor()
        
        #36781
#        if ins_general_methods.check_ticket_already_exists_in_tbl_ticket_refund_or_not(ins_ticket_base.str_ticket_number):
#            print(('\n\nDuplication on refund:',ins_ticket_base.str_ticket_number,'\n\n'))
#            raise
        
        str_current_date_time = ins_general_methods.get_current_date_time()
        
        if ins_general_methods.ins_auto_inv.bln_auto_refund:
            ins_ticket_base = self.save_auto_refund_details(ins_ticket_base)
            
        str_region_code = ins_general_methods.get_region_code(ins_ticket_base.int_region_id)
        if str_region_code != 'DOM' and  ins_general_methods.bln_capture_input_vat_only_for_dom_tickets:
            ins_ticket_base.flt_vat_in_rfd = 0.00
            
        if ins_ticket_base.flt_total_tax_rfd == 0:
            ins_ticket_base.flt_vat_in_rfd = 0.00
        
        ins_ticket_base.flt_supplier_currency_roe_rfd = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_tran_currency_rfd,
                                                                                                ins_ticket_base.str_refund_date)
                                                                                                
        ins_ticket_base.flt_standard_commission_rfd = ins_ticket_base.flt_market_fare_rfd * ins_ticket_base.flt_std_commn_percentage_rfd/100

        if ins_general_methods.get_admin_settings('STR_AIT_TAX_FIELD_IN_INVOICE_AND_REFUND') :
            ins_ticket_base.flt_standard_commission_rfd = round(ins_ticket_base.flt_standard_commission_rfd)
        ins_ticket_base.flt_supplier_amount_rfd = ins_ticket_base.flt_market_fare_rfd + ins_ticket_base.flt_total_tax_rfd  - ins_ticket_base.flt_standard_commission_rfd #+ ins_ticket_base.flt_supplier_refund_charge
        ins_ticket_base.flt_net_payable_rfd = ins_ticket_base.flt_market_fare_cash_rfd + ins_ticket_base.flt_total_tax_cash_rfd - ins_ticket_base.flt_standard_commission_rfd # + ins_ticket_base.flt_supplier_refund_charge_cash
        # refer 35652
        ins_ticket_base.flt_selling_price_rfd = ins_ticket_base.flt_market_fare_rfd + ins_ticket_base.flt_total_tax_rfd +\
                                                ins_ticket_base.flt_service_charge_rfd - ins_ticket_base.flt_discount_given_rfd +\
                                                    ins_ticket_base.flt_extra_earning_rfd + ins_ticket_base.flt_pay_back_commission_rfd -\
                                                    ins_ticket_base.flt_vat_in_rfd

        ins_ticket_base.flt_debited_amount_rfd = ins_ticket_base.flt_net_payable_rfd - ins_ticket_base.flt_supplier_refund_charge_cash
        ins_ticket_base.flt_inv_debited_amount_rfd = ins_ticket_base.flt_selling_price_rfd - ins_ticket_base.flt_market_fare_credit_rfd - ins_ticket_base.flt_total_tax_credit_rfd
        ins_ticket_base.flt_credited_amount_rfd = ins_ticket_base.flt_inv_debited_amount_rfd - ins_ticket_base.flt_client_refund_charge


        ins_ticket_base.flt_supplier_refund_net = ins_ticket_base.flt_supplier_amount_rfd - ins_ticket_base.flt_supplier_refund_charge
        ins_ticket_base.flt_client_refund_net = ins_ticket_base.flt_selling_price_rfd - ins_ticket_base.flt_client_refund_charge

        ins_ticket_base.flt_profit_rfd = ins_ticket_base.flt_client_refund_charge - ins_ticket_base.flt_supplier_refund_charge_cash
        ins_ticket_base.flt_actual_cost_rfd = ins_ticket_base.flt_supplier_amount_rfd
        
        
        flt_uccf_amount = 0
        flt_cor_card_amount = 0
        int_card_type = 0
        int_corporate_card_id_rfd = None #22937
        int_corporate_card_id = None
        int_account_master_id = None
        
        
        bln_cust_card = False
        bln_agency_card = False
        for tpl_card_data in ins_ticket_base.lst_card_data :
            if len(ins_ticket_base.lst_card_data) == 1 :
                if tpl_card_data[1] :
                    (int_corporate_card_id,int_coperate_card_account_type) = ins_general_methods.get_corporate_card_id(tpl_card_data[1],ins_ticket_base.str_crs_company)
                if ins_ticket_base.int_card_payment_type_rfd == 1  :
                    flt_uccf_amount += ins_ticket_base.flt_market_fare_credit_rfd + ins_ticket_base.flt_total_tax_credit_rfd + ins_ticket_base.flt_supplier_refund_charge_credit
                    int_card_type = 2
                elif ins_ticket_base.int_card_payment_type_rfd == 2  :
                    flt_cor_card_amount +=  ins_ticket_base.flt_market_fare_credit_rfd + ins_ticket_base.flt_total_tax_credit_rfd + ins_ticket_base.flt_supplier_refund_charge_credit
                    int_card_type = 0
                elif ins_ticket_base.int_card_payment_type_rfd == 3 : #22937
#                    int_corporate_card_id_rfd = int_corporate_card_id
                    int_card_type = 1
                    flt_cor_card_amount += ins_ticket_base.flt_market_fare_credit_rfd + ins_ticket_base.flt_total_tax_credit_rfd + ins_ticket_base.flt_supplier_refund_charge_credit
                else  :
                    int_card_type = 1
                    flt_cor_card_amount += ins_ticket_base.flt_market_fare_credit_rfd + ins_ticket_base.flt_total_tax_credit_rfd  + ins_ticket_base.flt_supplier_refund_charge_credit
                if int_corporate_card_id:
                    int_corporate_card_id_rfd = int_corporate_card_id
                    
                ins_ticket_base.str_cc_type_inv = tpl_card_data[0]
                ins_ticket_base.str_cc_number_ext = tpl_card_data[1] #
                
            else :

                if tpl_card_data[1] :
                    (int_account_master_id,int_account_type) = ins_general_methods.get_corporate_card_id(tpl_card_data[1],ins_ticket_base.str_crs_company)
                    if int_account_master_id:
                        int_corporate_card_id_rfd = int_account_master_id

                        if int_account_type == 1 : #// bln customer card true
                            int_card_type = 1
                            bln_cust_card = True
                        else :
                            int_card_type = 0
                            bln_agency_card = True
                    else :#if not int_account_master_id and ins_ticket_base.str_card_approval_code :
                        int_card_type = 2

                if int_card_type == 2 :
                    flt_uccf_amount += float(tpl_card_data[2] or 0)
                else :
                    flt_cor_card_amount += float(tpl_card_data[2] or 0)
                ins_ticket_base.str_cc_type_inv = tpl_card_data[0] #39948
                ins_ticket_base.str_cc_number_ext = tpl_card_data[1] 
                
        if len(ins_ticket_base.lst_card_data) > 1 :
            if bln_cust_card :
                ins_ticket_base.int_card_payment_type_rfd = 6
            elif bln_agency_card :
                ins_ticket_base.int_card_payment_type_rfd = 5
        
        if flt_uccf_amount or flt_cor_card_amount :
            flt_mf_credit = ins_ticket_base.flt_market_fare_credit_rfd
            flt_tax_credit = ins_ticket_base.flt_total_tax_credit_rfd
            flt_charg_credit = ins_ticket_base.flt_supplier_refund_charge_credit

            if flt_uccf_amount :
                if flt_uccf_amount >  flt_mf_credit:
                    ins_ticket_base.flt_market_fare_uccf_amount_rfd = flt_mf_credit
                    flt_uccf_amount = flt_uccf_amount - flt_mf_credit
                    flt_mf_credit = 0
                    if flt_uccf_amount > flt_tax_credit:
                        ins_ticket_base.flt_tax_uccf_amount_rfd = flt_tax_credit
                        flt_uccf_amount = flt_uccf_amount - flt_tax_credit
                        ins_ticket_base.flt_sup_charge_uccf_amount_rfd = flt_uccf_amount
                        flt_uccf_amount = 0
                        pass
                    else :
                        ins_ticket_base.flt_tax_uccf_amount_rfd  = flt_uccf_amount
                        flt_uccf_amount = 0
                        ins_ticket_base.flt_sup_charge_uccf_amount_rfd = 0
                        flt_tax_credit = flt_tax_credit - ins_ticket_base.flt_tax_uccf_amount_rfd
                else :
                    ins_ticket_base.flt_market_fare_uccf_amount_rfd = flt_uccf_amount
                    ins_ticket_base.flt_tax_uccf_amount_rfd = 0
                    ins_ticket_base.flt_sup_charge_uccf_amount_rfd = 0
                    flt_mf_credit = flt_mf_credit - ins_ticket_base.flt_market_fare_uccf_amount_rfd
                    flt_uccf_amount = 0
            
            if flt_cor_card_amount :

                if flt_cor_card_amount >  flt_mf_credit:
                    ins_ticket_base.flt_market_fare_card_amount_rfd = flt_mf_credit
                    flt_cor_card_amount = flt_cor_card_amount - flt_mf_credit
                    flt_mf_credit = 0
                    
                    if flt_cor_card_amount > flt_tax_credit :
                        ins_ticket_base.flt_tax_card_amount_rfd  = flt_tax_credit
                        flt_cor_card_amount = flt_cor_card_amount - flt_tax_credit
                        flt_tax_credit = 0
                        ins_ticket_base.flt_sup_charge_card_amount_rfd = flt_cor_card_amount
                    else :
                        ins_ticket_base.flt_tax_card_amount_rfd = flt_cor_card_amount
                        flt_cor_card_amount = 0
                        ins_ticket_base.flt_sup_charge_card_amount_rfd = 0
                        
                else :
                    ins_ticket_base.flt_market_fare_card_amount_rfd = flt_cor_card_amount
                    ins_ticket_base.flt_tax_card_amount_rfd = 0
                    ins_ticket_base.flt_sup_charge_card_amount_rfd = 0
                    flt_mf_credit = flt_mf_credit - ins_ticket_base.flt_market_fare_card_amount_rfd
                    flt_cor_card_amount = 0

        if not int_corporate_card_id_rfd and ins_ticket_base.int_corporate_card_id_rfd :
            int_corporate_card_id_rfd =ins_ticket_base.int_corporate_card_id_rfd 
        
        if bln_agency_card or ins_ticket_base.int_card_payment_type_rfd == 2:
            ins_ticket_base.flt_credited_amount_rfd = ins_ticket_base.flt_selling_price_rfd-ins_ticket_base.flt_client_refund_charge
            ins_ticket_base.flt_inv_debited_amount_rfd = ins_ticket_base.flt_selling_price_rfd
        
        #get customer currency and its roe
        if ins_ticket_base.int_account_master_id and ins_ticket_base.str_refund_date:
            ins_ticket_base.str_cust_currency_code,ins_ticket_base.flt_cust_currency_roe = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,\
                                ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
        
        cr.execute("""UPDATE tbl_ticket SET chr_ticket_status = 'R',
                                            chr_system_refund_part_action = 'I',
                                            vchr_gds_file_name_rfd = %s,
                                            vchr_ticketing_agent_code_rfd = %s,
                                            vchr_ticketing_agent_numeric_code_rfd = %s,
                                            dat_refund = %s,
                                            int_no_of_segments_rfd = %s,
                                            vchr_sector_rfd = %s ,
                                            fk_bint_ticketing_agent_id_rfd = %s,
					    fk_bint_branch_id_rfd = %s,
                                            fk_bint_customer_account_id_rfd = %s,
                                            fk_bint_supplier_account_id_rfd = %s,
                                            fk_bint_corporate_card_account_id_rfd = %s,
                                            int_card_payment_type_rfd = %s,
                                            vchr_remarks_rfd = %s,
                                            int_distribution_type_rfd = %s,
                                            fk_bint_modified_user_id = 2,
                                            tim_modified = now()
                                            
                        WHERE vchr_ticket_number = %s 
                            AND chr_system_refund_part_action = '' 
                            AND COALESCE(vchr_document_no_rfd,'') = '' """,
                        (ins_ticket_base.str_file_name_rfd ,
                        ins_ticket_base.str_ticketing_agent_code_rfd ,
                        ins_ticket_base.str_ticketing_agent_numeric_code_rfd ,
                        ins_ticket_base.str_refund_date ,
                        ins_ticket_base.int_number_of_segments_rfd ,
                        ins_ticket_base.str_sector ,
                        ins_ticket_base.int_counter_staff_id_rfd ,
			ins_ticket_base.int_location_id ,
                        ins_ticket_base.int_account_master_id ,
                        ins_ticket_base.int_supplier_id_rfd ,
                        int_corporate_card_id_rfd ,
                        ins_ticket_base.int_card_payment_type_rfd ,
                        ins_ticket_base.str_remarks_rfd,
                        ins_ticket_base.int_distribution_type_rfd,
                        
                        ins_ticket_base.str_ticket_number

                       ))
                       
        if not cr.rowcount: #39067
            raise
        
        #creating fare list for saving refund data
        lst_fare_data = []
        lst_fare_common_data = []
        bln_cust_base_rfd = False
        if not ins_ticket_base.str_cust_currency_code :
            bln_cust_base_rfd = True
        if ins_ticket_base.str_tran_currency_rfd == ins_ticket_base.str_base_currency_rfd :
            if ins_ticket_base.str_tran_currency_rfd == ins_ticket_base.str_cust_currency_code:
#                print ('all are same') 
                lst_fare_common_data = [[True, #bln_sup_base_currency
                                            True, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_ticket_base.str_tran_currency_rfd, #currency -all are same
                                            1,#roe
                                            True]] #bln_refund
            else:
#                print ('Tran and base are same, Cust different')
                lst_fare_common_data = [[True, #bln_base_currency
                                        True, #bln_tran_currency
                                        bln_cust_base_rfd, #bln_cust_base_currency
                                        False, #bln_cust_currency
                                        ins_ticket_base.str_tran_currency_rfd, #currency -Tran and base are same
                                        1,#roe
                                        True]] #bln_refund
                if ins_ticket_base.str_cust_currency_code:
                    lst_fare_common_data.extend([[False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_ticket_base.str_cust_currency_code, #currency - Cust different
                                            ins_ticket_base.flt_supplier_currency_roe_rfd, #roe
                                            True],#bln_refund
                                            [False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_ticket_base.str_base_currency_rfd, #currency - Cust base different
                                            ins_ticket_base.flt_supplier_currency_roe_rfd, #roe
                                            True]
                                            ])
        elif ins_ticket_base.str_tran_currency_rfd == ins_ticket_base.str_cust_currency_code:
#            print ('Tran and cust are same, Base different')
            lst_fare_common_data = [[False, #bln_sup_base_currency
                                    True, #bln_tran_currency
                                    False, #bln_cust_base_currency
                                    True, #bln_cust_currency
                                    ins_ticket_base.str_tran_currency_rfd, #currency - Tran and Cust are same
                                    1,#roe
                                    True], #bln_refund
                                    [True, #bln_sup_base_currency
                                    False, #bln_tran_currency
                                    True, #bln_cust_base_currency
                                    False, #bln_cust_currency
                                    ins_ticket_base.str_base_currency_rfd, #currency - base different
                                    ins_ticket_base.flt_supplier_currency_roe_rfd, #roe 
                                    True ]] #bln_refund
        elif ins_ticket_base.str_cust_currency_code == ins_ticket_base.str_base_currency_rfd:
#            print ('Cust and base are same, Tran different')
            lst_fare_common_data = [[False, #bln_sup_base_currency
                                    True, #bln_tran_currency
                                    False, #bln_cust_base_currency
                                    False, #bln_cust_currency
                                    ins_ticket_base.str_tran_currency_rfd, #currency - Tran different
                                    1,#roe
                                    True], #bln_refund
                                    [True, #bln_sup_base_currency
                                    False, #bln_tran_currency
                                    False, #bln_cust_base_currency
                                    False, #bln_cust_currency
                                    ins_ticket_base.str_base_currency_rfd, #currency - Tran base different
                                    ins_ticket_base.flt_supplier_currency_roe_rfd, #roe
                                    True],
                                    [False, #bln_sup_base_currency
                                    False, #bln_tran_currency
                                    True, #bln_cust_base_currency
                                    True, #bln_cust_currency
                                    ins_ticket_base.str_base_currency_rfd, #currency - Cust and base are same
                                    ins_ticket_base.flt_supplier_currency_roe_rfd, #roe
                                    True]] #bln_refund
        else:
#            print ('all are different')
            lst_fare_common_data = [[False, #bln_sup_base_currency
                                    True, #bln_tran_currency
                                    False, #bln_cust_base_currency
                                    False, #bln_cust_currency
                                    ins_ticket_base.str_tran_currency_rfd, #currency - tran
                                    1,#roe
                                    True], #bln_refund
                                    [True, #bln_sup_base_currency
                                    False, #bln_tran_currency
                                    bln_cust_base_rfd, #bln_cust_base_currency
                                    False, #bln_cust_currency
                                    ins_ticket_base.str_base_currency_rfd, #currency - tran bsae
                                    ins_ticket_base.flt_supplier_currency_roe_rfd, #roe
                                    True]] #bln_refund
            if ins_ticket_base.str_cust_currency_code:
                lst_fare_common_data.extend([[False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_ticket_base.str_cust_currency_code, #currency - Cust
                                            ins_ticket_base.flt_supplier_currency_roe_rfd, #roe
                                            True],#bln_refund
                                            [False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_ticket_base.str_base_currency_rfd, #currency - Cust base
                                            ins_ticket_base.flt_supplier_currency_roe_rfd, #roe
                                            True]] #bln_refund
                                            ) 

        lst_fare_data = self.create_fare_data_list(ins_ticket_base,lst_fare_common_data,ins_ticket_base.flt_supplier_currency_roe_rfd)
        #save data fare_details
        self.save_fare_details_data(lst_fare_data)
        
        #save ticket log into tbl_ticket_log
        self.save_ticket_log_data(ins_ticket_base.str_ticket_number,str_current_date_time,'REFUND')

        print ("Saved Refund Ticket :" + str(ins_ticket_base.str_ticket_number))
        cr.close()

    def clear_tickets_refund_data(self, str_ticket_number):

        cr = self.create_cursor()


        cr.execute("""UPDATE tbl_ticket SET dat_refund = NULL,
                                            bl_base_currency_market_fare_rfd = 0.00,
                                            dbl_tran_currency_market_fare_rfd = 0.00,
                                            dbl_base_currency_tax_rfd = 0.00,
                                            dbl_tran_currency_tax_rfd = 0.00,
                                            dbl_base_currency_supplier_refund_charge_rfd = 0.00,
                                            dbl_tran_currency_supplier_refund_charge_rfd = 0.00,
                                            chr_ticket_status = CASE WHEN vchr_document_no_inv = ''
                                                THEN 'I'
                                                ELSE 'S'
                                                END,
                                            vchr_ticketing_agent_code_rfd = '',
                                            dbl_base_currency_client_charge_rfd = 0.00,
                                            dbl_tran_currency_client_charge_rfd = 0.00,
                                            vchr_gds_file_name_rfd = ''
                      WHERE vchr_ticket_number = %s """,(str_ticket_number, ))

        cr.close()
            
    def save_refund_void_data_of_original_tickets(self , lst_original_ticket_details ,
                                                        str_refund_date ,
                                                        str_status,
                                                        str_file_name_rfd,
                                                        int_customer_id_rfd,
                                                        int_supplier_id_rfd,
                                                        int_branch_id_rfd,
                                                        str_ticketing_agent_code_rfd,
                                                        vchr_ticketing_agent_numeric_code_rfd,
                                                        int_counter_staff_id_rfd
                                                        ):#36508
        cr = self.create_cursor()
        
        str_query = """UPDATE tbl_ticket
                       SET dat_refund = '%s',
                          chr_ticket_status = '%s',
                          chr_system_refund_part_action = 'I',
                          vchr_gds_file_name_rfd = '%s',
                          fk_bint_customer_account_id_rfd = %s ,
                          fk_bint_supplier_account_id_rfd = %s ,
                          fk_bint_branch_id_rfd = %s ,
                          vchr_ticketing_agent_code_rfd = '%s' ,
                          vchr_ticketing_agent_numeric_code_rfd = '%s' ,
                          fk_bint_ticketing_agent_id_rfd = %s 
                       WHERE vchr_ticket_number IN (%s)
                    """%(str_refund_date ,
                        str_status,
                        str_file_name_rfd,
                        int_customer_id_rfd  or 'NULL',
                        int_supplier_id_rfd or 'NULL',
                        int_branch_id_rfd  or 'NULL',
                        str_ticketing_agent_code_rfd,
                        vchr_ticketing_agent_numeric_code_rfd,
                        int_counter_staff_id_rfd  or 'NULL',
                        ','.join(["'%s'"]*len(lst_original_ticket_details))%tuple(lst_original_ticket_details))
        
        cr.execute(str_query)
        cr.close() 
    
    def save_auto_invoice_data(self,ins_capture_ticket_data,
                                    lst_tickets, 
                                    lst_ticket_capture_details,
                                    lst_emd_tickets = [], 
                                    lst_ticket_emd_capture_details = [],
                                    lst_hotel_vouchers = [],
                                    lst_other_service_details = []):
        # // Auto Invoice
        try:
            cr = self.create_cursor()
            
            if ins_capture_ticket_data.bln_refund :
                pass
            else :

                if ins_capture_ticket_data.str_agency_auto_invoice_yes_no.upper() not in ('NO','FALSE')\
                    and (ins_general_methods.ins_auto_inv.bln_auto_invoice_all_ticket or (ins_general_methods.ins_auto_inv.bln_auto_invoice and ins_capture_ticket_data.str_agency_auto_invoice_yes_no.upper() in ('T','TRUE','YES','Y'))\
                    or (ins_general_methods.ins_auto_inv.bln_auto_invoice and ins_capture_emd_ticket_data.str_agency_auto_invoice_yes_no.upper() in ('T','TRUE','YES','Y'))): # //refer 21791 
                    # // for enabling auto capturing for emd tickets
                    #refer  19527 auto invoice emd tickets based on reason for issuance
                    lst_emd_tickets_to_invoice = []
                    if   lst_emd_tickets and lst_ticket_emd_capture_details:
                        for ins in lst_ticket_emd_capture_details:
                            if  ins.bln_auto_invoice_emd_tickets:
                                lst_emd_tickets_to_invoice.append(ins)
                            else:
                                lst_emd_tickets.remove(ins.str_ticket_number)
                    
                    lst_all_tickets = lst_tickets + lst_emd_tickets
                    lst_tickets_copy = copy.copy(lst_all_tickets)
                    lst_hv_copy = copy.copy(lst_hotel_vouchers)
                    lst_os_copy = copy.copy(lst_other_service_details)

                    lst_all_ticket_capture_details = lst_ticket_capture_details + lst_emd_tickets_to_invoice
                    

                    lst_auto_inv_details = [(lst_hv_copy,'H'),(lst_os_copy,'O'),(lst_tickets_copy,'T')]
                    
                    for lst_tickets_copy,str_supp_doc_type in lst_auto_inv_details :
                        if lst_tickets_copy :                    
                            
                            bln_void = False
                            if ins_capture_ticket_data.str_void_date :
                                bln_void = True
                            
                                
                            if str_supp_doc_type != 'T' :
                                int_tickets_count_in_inv = len(lst_tickets_copy)
                            else :
                                ins_ticket_base = lst_all_ticket_capture_details[0]
                                if  not ins_ticket_base.int_tickets_count_in_inv :
                                    ins_ticket_base.int_tickets_count_in_inv = len(lst_tickets_copy)
                                int_tickets_count_in_inv = ins_ticket_base.int_tickets_count_in_inv

                            bln_per_ticket = False
                            bln_per_file = False
                            str_inv_grouping_value = ''
                            if int_tickets_count_in_inv :

                                if not bln_void and ins_capture_ticket_data.str_auto_inv_grouping : #refs #26999
                                    
                                    if ins_capture_ticket_data.str_auto_inv_grouping in ('ISSUE DATE END OF THE DAY','LPO END OF THE DAY','PNR END OF THE DAY') :
                                        return 
                                    
                                    elif ins_capture_ticket_data.str_auto_inv_grouping == 'LPO COUNT BASED' :
                                        if not ins_ticket_base.str_lpo_number :
                                            return
                                        str_inv_grouping_value = ins_ticket_base.str_lpo_number+ins_ticket_base.str_account_code_inv
                                        
                                    elif ins_capture_ticket_data.str_auto_inv_grouping == 'PNR COUNT BASED' :
                                        str_inv_grouping_value = ins_ticket_base.str_pnr_no
                                        
                                    elif ins_capture_ticket_data.str_auto_inv_grouping == 'PER GDS FILE' :
                                        str_inv_grouping_value = ''
                                        bln_per_file = True
                                        
                                    elif ins_capture_ticket_data.str_auto_inv_grouping == 'PER TICKET' :
                                        str_inv_grouping_value = ''
                                        bln_per_ticket = True
                                    else :
                                        str_inv_grouping_value = ins_capture_ticket_data.str_pnr_no.strip()
                                        
                                else :
                                    str_inv_grouping_value = ins_capture_ticket_data.str_pnr_no.strip()
    #                            str_all_tickets = ','.join(lst_tickets_copy)
                                str_current_date_time = ins_general_methods.get_current_date_time()

#                                cr = ins_general_methods.create_cursor()
                                
                                if bln_void : #// add a row for deleting the ticket from the invoice


                                    for str_ticket_number in lst_tickets_copy :

                                        cr.execute("""
                                                    SELECT pk_bint_auto_invoice_details_id ,
                                                            vchr_list_of_ticket_numbers ,
                                                            vchr_pnr_number  ,
                                                            int_total_number_of_tickets  ,
                                                            int_type  ,
                                                            bln_approval  ,
                                                            bln_completed  ,
                                                            vchr_remarks , 
                                                            tim_created 
                                                            FROM tbl_auto_invoice_details
                                                        WHERE bln_completed = FALSE
                                                        AND chr_supporting_document_type = %s
                                                        AND string_to_array(%s , '') <@ string_to_array(vchr_list_of_ticket_numbers,',')
                                                        """,(str_supp_doc_type,str_ticket_number,))

                                        rst = cr.fetchone()

                                        if rst :

                                            if rst['int_type'] == 0 : # if row for delete already exist
                                                continue
                                            else :
                                                lst_tickets = rst['vchr_list_of_ticket_numbers'].split(',')
                                                lst_tickets = [ str_ticket_nos for str_ticket_nos in lst_tickets if str_ticket_nos != str_ticket_number]

                                                if lst_tickets :
                                                    str_list_of_tkt_nos = ','.join(lst_tickets)

                                                    cr.execute("""  
                                                                    UPDATE tbl_auto_invoice_details
                                                                    SET vchr_list_of_ticket_numbers = %s ,
                                                                        int_total_number_of_tickets = int_total_number_of_tickets - 1
                                                                    WHERE pk_bint_auto_invoice_details_id = %s
                                                                    """,(str_list_of_tkt_nos,rst['pk_bint_auto_invoice_details_id']))

                                                    pass
                                                else :
                                                    cr.execute("""  
                                                                    DELETE FROM tbl_auto_invoice_details
                                                                    WHERE pk_bint_auto_invoice_details_id = %s
                                                                    """,(rst['pk_bint_auto_invoice_details_id'],))

                                        else : ##insert a row for delete,==>  not needed now, it will work completely based on cronjob
                                            pass
                                else :  

                                    """ 
                                        Add row for invoicing the tickets
                                        If pnr already in the table.. then do following

                                    """

                                    int_type = 1
                                    if bln_per_file or bln_per_ticket :
                                        rst = []
                                    else :
                                        cr.execute("""
                                                    SELECT pk_bint_auto_invoice_details_id ,
                                                            vchr_list_of_ticket_numbers ,
                                                            vchr_pnr_number  ,
                                                            int_total_number_of_tickets  ,
                                                            int_type  ,
                                                            bln_approval  ,
                                                            bln_completed  ,
                                                            vchr_remarks , 
                                                            tim_created 
                                                            FROM tbl_auto_invoice_details
                                                        WHERE vchr_pnr_number = %s
                                                        AND chr_supporting_document_type = %s
                                                        ORDER BY int_type ASC , bln_completed DESC

                                                        """,(str_inv_grouping_value,str_supp_doc_type))

                                        rst = cr.fetchall()

                                    if rst not in [[],None,[None],[[]] ]: 
                                        for record in rst :

                                            if record['bln_completed'] or record['int_type'] != 1 :
                                                lst_added_tickets = record['vchr_list_of_ticket_numbers'].split(',')
                                                for str_ticket_number in lst_tickets_copy :
                                                    if str_ticket_number in lst_added_tickets :
                                                        lst_tickets_copy.remove(str_ticket_number)
                                                    else :
                                                        lst_added_tickets.append(str_ticket_number)


                                            elif record['int_type'] == 1 and not record['bln_completed'] :
                                                if lst_tickets_copy :
                                                    lst_added_tickets = record['vchr_list_of_ticket_numbers'].split(',')

                                                    for str_ticket_number in lst_tickets_copy :
                                                        if str_ticket_number in lst_added_tickets :
                                                            lst_tickets_copy.remove(str_ticket_number)

                                                        else :
                                                            lst_added_tickets.append(str_ticket_number)

                                                    if lst_tickets_copy :
                                                        int_total_tkts = len(lst_added_tickets)

                                                        if int_total_tkts >= record['int_total_number_of_tickets'] :
                                                            bln_approval = True
                                                        else :
                                                            int_total_tkts = record['int_total_number_of_tickets']
                                                            bln_approval = False

                                                        str_list_of_tkt_nos =  ','.join(lst_added_tickets)
                                                        lst_tickets_copy =  []
                                                        cr.execute("""  
                                                                            UPDATE tbl_auto_invoice_details
                                                                            SET vchr_list_of_ticket_numbers = %s ,
                                                                                bln_approval = %s ,
                                                                                int_total_number_of_tickets = %s
                                                                            WHERE pk_bint_auto_invoice_details_id = %s
                                                                            """,(str_list_of_tkt_nos,
                                                                                    bln_approval,
                                                                                    int_total_tkts ,
                                                                                    record['pk_bint_auto_invoice_details_id'],))
                                                                                    
                                    
                                    if rst in [[],None,[None],[[]] ] or lst_tickets_copy:    ## Add a new row for invoice

                                        tpl_insert_value = []
                                        if bln_per_file :
                                            str_list_of_tkt_nos =  ','.join(lst_tickets_copy)
                                            bln_approval = True
                                            int_tickets_count_in_inv = len(lst_tickets_copy)

                                            tpl_insert_value = [(
                                                                    str_list_of_tkt_nos ,
                                                                    str_inv_grouping_value ,
                                                                    int_tickets_count_in_inv,
                                                                    int_type ,
                                                                    bln_approval ,
                                                                    False ,
                                                                    str_current_date_time ,
                                                                    str_supp_doc_type

                                                                        )]
                                            
                                        elif bln_per_ticket :
                                            int_tickets_count_in_inv = 1
                                            bln_approval = True
                                            for str_tkt_no in lst_tickets_copy :
                                                tpl_insert_value.append((
                                                                    str_tkt_no ,
                                                                    str_inv_grouping_value ,
                                                                    int_tickets_count_in_inv,
                                                                    int_type ,
                                                                    bln_approval ,
                                                                    False ,
                                                                    str_current_date_time ,
                                                                    str_supp_doc_type

                                                                        ))
                                            
                                        else :
                                        
                                            str_list_of_tkt_nos =  ','.join(lst_tickets_copy)

                                            if int_tickets_count_in_inv < len(lst_tickets_copy):
                                                int_tickets_count_in_inv =  len(lst_tickets_copy)

                                            if len(lst_tickets_copy) >= int_tickets_count_in_inv :
                                                bln_approval = True
                                            else :
                                                bln_approval = False

                                            tpl_insert_value = [(
                                                                str_list_of_tkt_nos ,
                                                                str_inv_grouping_value ,
                                                                int_tickets_count_in_inv,
                                                                int_type ,
                                                                bln_approval ,
                                                                False ,
                                                                str_current_date_time ,
                                                                str_supp_doc_type
                                                                )]

                                        if tpl_insert_value :
                                            cr.executemany("""
                                                        INSERT INTO tbl_auto_invoice_details
                                                            (
                                                            vchr_list_of_ticket_numbers ,
                                                            vchr_pnr_number ,
                                                            int_total_number_of_tickets ,
                                                            int_type ,
                                                            bln_approval ,
                                                            bln_completed ,
                                                            tim_created ,
                                                            chr_supporting_document_type
                                                            )
                                                            VALUES
                                                            (
                                                            %s, %s, %s,
                                                            %s, %s, %s,
                                                            %s, %s
                                                            )

                                                            """,tpl_insert_value)


                else:
                    pass
                pass

        except Exception as msg:
            cr.close()
            ins_general_methods.ins_db.rollback()

        else :
            cr.close()
            ins_general_methods.ins_db.commit()
    
    def create_json_and_upload(self,ins_ticket_base):
        
        
        
        ins_non_iata_settings = self.get_non_iata_instance(ins_ticket_base)
        if not ins_non_iata_settings :
            return
        
        str_status = 'ISSUE'
        
        flt_vat_in = ins_ticket_base.flt_vat_in_inv #40099
        if ins_ticket_base.bln_refund :
            str_status = 'REFUND'
            flt_vat_in = ins_ticket_base.flt_vat_in_rfd
        elif ins_ticket_base.str_void_date :
            str_status = 'VOID'
            
            
        dct_authentication = {
                                "STR_USER_NAME": ins_non_iata_settings.str_user_name,
                                "STR_PASSWORD": ins_non_iata_settings.str_password,
                                "STR_AUTHENTICATION": "" ,
                                "STR_JSON_TYPE" : 'NON_IATA'
                             }
                   
        dct_json = {

                "str_authentication_key": dct_authentication,
                
                "json_master":
                
                    {
                "STR_AUTO_INVOICE": ins_ticket_base.str_agency_auto_invoice_yes_no,
                "STR_TYPE": "STOCK" ,
                "STR_TICKET_NO" :ins_ticket_base.str_ticket_number,
                "STR_ACTION": "NEW OR UPDATE",
                "STR_STATUS": str_status,
                "STR_COST_CENTRE_CODE": ins_ticket_base.str_cost_centre ,
                "STR_DEPARTMENT_CODE": ins_ticket_base.str_branch_code,
                "STR_FILE_NUMBER": '',
                "STR_TICKET_TYPE": ins_ticket_base.str_ticket_type ,
                "STR_ACCOUNT_CODE": ins_ticket_base.str_customer_code,
                "STR_SUB_CUSTOMER_CODE": ins_ticket_base.str_sub_customer_code,
                "STR_AIRLINE_NUMERIC_CODE":  ins_ticket_base.str_ticketing_airline_numeric_code,
                "STR_AIRLINE_CHARACTER_CODE": ins_ticket_base.str_ticketing_airline_character_code,
                "STR_AIRLINE_NAME": ins_ticket_base.str_ticketing_airline_name,
                "STR_SUPPLIER_CODE": ins_non_iata_settings.str_supplier,
                "STR_CORP_CREDIT_CARD_ACCOUNT_CODE": '', 
                "STR_CUSTOMER_CORP_CREDIT_CARD_ACCOUNT_CODE": '', ## Put credit card num instead of this
                "STR_REPORTING_DATE": ins_ticket_base.str_ticket_issue_date,
                "STR_TICKET_ISSUE_DATE": ins_ticket_base.str_ticket_issue_date,
                "STR_TRAVELER_ID": '',
                "STR_PAX_NAME": ins_ticket_base.str_pax_name,
                "STR_ADDITIONAL_PAX": '',
                "STR_SECTOR": ins_ticket_base.str_sector,
                "CHR_TRAVELER_CLASS": ins_ticket_base.str_class,
                "CHR_RETURN_CLASS": ins_ticket_base.str_return_class,
                "STR_LPO_NO": ins_ticket_base.str_lpo_number,
                "STR_GDS": ins_ticket_base.str_crs_company,
                "STR_BOOKING_STAFF_CODE": ins_ticket_base.str_booking_agent_numeric_code,
                "STR_BOOKING_STAFF_CHARACTER_CODE": ins_ticket_base.str_booking_agent_code,
                "STR_BOOKING_STAFF_EMAIL_ID": '',
                "STR_TICKETING_STAFF_CODE": ins_ticket_base.str_ticketing_agent_numeric_code,
                "STR_TICKETING_STAFF_CHARACTER_CODE": ins_ticket_base.str_ticketing_agent_code,
                "STR_TICKETING_STAFF_EMAIL_ID": '',
                "STR_PNR_NO": ins_ticket_base.str_pnr_no,
                "STR_TOUR_CODE": ins_ticket_base.str_tour_code,
                "STR_FARE_BASIS": ins_ticket_base.str_fare_basis,
                "STR_REGION_CODE": ins_ticket_base.str_region_code,
                "STR_TICKET_REMARK": ins_ticket_base.str_remarks,
                "STR_CUSTOMER_REF_NO": '',
                "STR_CUSTOMER_EMP_NO": ins_ticket_base.str_cust_employee_no,
                "STR_SALES_MAN_CODE": '',
                "STR_TRAVEL_DATE": ins_ticket_base.str_travel_date,
                "STR_RETURN_DATE": ins_ticket_base.str_return_date,
                "STR_PAX_TYPE": ins_ticket_base.str_pax_type,
                "INT_NO_OF_PAX": ins_ticket_base.int_no_of_pax_inv,
                "STR_LAST_CONJ_TICKET": ins_ticket_base.str_last_conjection_ticket_number,
                "STR_ORIGINAL_ISSUE": ins_ticket_base.str_original_issue,
                "STR_BOOKING_AGENCY_IATA_NO": ins_ticket_base.str_booking_agency_iata_no,
                "STR_TICKETING_AGENCY_IATA_NO": ins_ticket_base.str_ticketing_agency_iata_no,
                "STR_BOOKING_AGENCY_OFFICE_ID": ins_ticket_base.str_booking_agency_office_id,
                "STR_TICKETING_AGENCY_OFFICE_ID": ins_ticket_base.str_ticketing_agency_office_id,
                "STR_PNR_FIRST_OWNER_OFFICE_ID": ins_ticket_base.str_pnr_first_owner_office_id,
                "STR_PNR_CURRENT_OWNER_OFFICE_ID": ins_ticket_base.str_pnr_current_owner_office_id,
                "STR_PRODUCT":"FLIGHT",
                "STR_AIRLINE_REF_NO": '',
                "STR_REFUND_DATE": ins_ticket_base.str_refund_date or '',
                "STR_REFUND_STAFF_CODE": ins_ticket_base.str_ticketing_agent_numeric_code,
                "STR_REFUND_STAFF_CHARACTER_CODE": ins_ticket_base.str_ticketing_agent_code,
                "STR_REFUND_STAFF_EMAIL_ID": '',
                "STR_REFUND_STATUS": str_status,
                "DBL_PURCHASE_ROE": ins_ticket_base.flt_supplier_currency_roe,
                "STR_PURCHASE_CUR_CODE": ins_ticket_base.str_defult_currency_code,
                "DBL_SELLING_ROE": ins_ticket_base.flt_supplier_currency_roe,
                "STR_SELLING_CUR_CODE": ins_ticket_base.str_defult_currency_code,
                "DBL_PURCHASE_CUR_PUBLISHED_FARE": ins_ticket_base.flt_published_fare_inv,
                "DBL_PURCHASE_CUR_TOTAL_MARKET_FARE": ins_ticket_base.flt_market_fare_cash_inv,
                "DBL_PURCHASE_CUR_TOTAL_MARKET_FARE_CREDIT_CARD": ins_ticket_base.flt_market_fare_credit_inv,
                "STR_TAX_DETAILS": ins_ticket_base.str_tax_details,
                "DBL_PURCHASE_CUR_TOTAL_TAX": ins_ticket_base.flt_total_tax_cash_inv,
                "DBL_PURCHASE_CUR_TOTAL_TAX_CREDIT_CARD": ins_ticket_base.flt_total_tax_credit_inv,
                "DBL_PURCHASE_CUR_STD_COMMISION": ins_ticket_base.flt_standard_commission,
                "DBL_PURCHASE_CUR_SUPPLIER_FEE": 0,
                "DBL_PURCHASE_CUR_SUPPLIER_CHARGE": ins_ticket_base.flt_supplier_refund_charge,
                "DBL_PURCHASE_CUR_SUPPLIER_AMOUNT": ins_ticket_base.flt_supplier_amount,
                "DBL_PURCHASE_CUR_AGENCY_CHARGE": ins_ticket_base.flt_supplier_refund_charge ,
                "DBL_PURCHASE_CUR_SERVICE_FEE": ins_ticket_base.flt_service_charge,
                "DBL_PURCHASE_CUR_EXTRA_EARNING": ins_ticket_base.flt_extra_earning_inv,
                "DBL_PURCHASE_CUR_PAYBACK_AMOUNT": ins_ticket_base.flt_pay_back_commission_inv,
                "DBL_PURCHASE_CUR_CREDIT_CARD_CHARGES": ins_ticket_base.flt_cc_charge_collected_ext,
                "DBL_PURCHASE_CUR_DISCOUNT": ins_ticket_base.flt_discount_given_ext,
                "DBL_PURCHASE_CUR_PRICE": ins_ticket_base.flt_selling_price,
                "DBL_BASE_CUR_PUBLISHED_FARE": ins_general_methods.convert_amount(ins_ticket_base.flt_published_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_TOTAL_MARKET_FARE": ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_TOTAL_MARKET_FARE_CREDIT_CARD": ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_TOTAL_TAX": ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_TOTAL_TAX_CREDIT_CARD": ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_STD_COMMISION": ins_general_methods.convert_amount(ins_ticket_base.flt_standard_commission,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_SUPPLIER_FEE": 0,
                "DBL_BASE_CUR_SUPPLIER_CHARGE": ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                "DBL_BASE_CUR_SUPPLIER_AMOUNT":ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amount,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_AGENCY_CHARGE": ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                "DBL_BASE_CUR_SERVICE_FEE": ins_general_methods.convert_amount(ins_ticket_base.flt_service_charge,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_BASE_CUR_EXTRA_EARNING":0.0,
                "DBL_BASE_CUR_PAYBACK_AMOUNT":0 ,
                "STR_PAYBACK_ACCOUNT_CODE": '',
                "DBL_BASE_CUR_CREDIT_CARD_CHARGES": '' ,
                "DBL_BASE_CUR_DISCOUNT":0.0,
                "DBL_BASE_CUR_PRICE": ins_general_methods.convert_amount(ins_ticket_base.flt_selling_price,ins_ticket_base.flt_supplier_currency_roe),
                "DBL_SELLING_CUR_PRICE":ins_ticket_base.flt_selling_price,
                "STR_BOOKING_DATE" : ins_ticket_base.str_ticket_booking_date,
                "STR_GDS_FILE_NAME_INV" : ins_ticket_base.str_file_name_inv,
                "STR_GDS_FILE_NAME_RFD" : ins_ticket_base.str_file_name_rfd,
                "STR_FARE_CONSTRUCTION" : ins_ticket_base.str_fare_construction,
                "DBL_PURCHASE_CUR_INPUT_VAT" : flt_vat_in,
                "DBL_BASE_CUR_INPUT_VAT": ins_general_methods.convert_amount(flt_vat_in,ins_ticket_base.flt_supplier_currency_roe)
                
                }}
     
        path_invoice = ins_non_iata_settings.str_url 
        
        
     ############################    
#        import httplib, urllib,urllib2
#        params = urllib.urlencode(dct_json)
#        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
#        conn = urllib2.urlopen(path_invoice)
#        conn.request("POST", "", params, headers)
#        response = conn.getresponse()
#          
                                     
      ##################                               
                
        try :
            
            session = Session()
            session.head(path_invoice)
            
            dct_json = json.dumps(dct_json)
            response = session.post(
                                url=path_invoice,
                                data={'json_data' : dct_json },
                                headers={
                                    'Referer': path_invoice
                                        }
                                    )
        except Exception as msg:
            print('Creating session failed , please check server side working or not !!!')
            
            self.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        'Creating session failed , please check server side working or not !!!' + str(msg),
                                        str_status
                                        
                                        )
            return

    
        if response.status_code == 200 and (response.content.find('"STATUS":1') != -1 or response.content.find('"STATUS":2') != -1):
            try :
                    print((response.content))
            except :
                pass
            print('success')
        else :
            print('Json upload Failed')
            self.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        ('Saving Failed Error Code : ' + str(response.status_code) + '    ' +  str(response.content))[:500],
                                        str_status
                                        
                                        )
    
        try :
            session.close()
        except :
            pass
        
        
    #37364    
    def create_void_json_and_upload(self,ins_ticket_base): 
        ins_non_iata_settings = self.get_non_iata_instance(ins_ticket_base)
        if not ins_non_iata_settings :
            return
        
        str_status = 'VOID'        
            
        dct_authentication = {
                                "STR_USER_NAME": ins_non_iata_settings.str_user_name,
                                "STR_PASSWORD": ins_non_iata_settings.str_password,
                                "STR_AUTHENTICATION": "" ,
                                "STR_JSON_TYPE" : 'NON_IATA'
                             }
                   
        dct_void_json = {

                "str_authentication_key": dct_authentication,
                
                "json_master":
                
                    {
                        "STR_AUTO_INVOICE": ins_ticket_base.str_agency_auto_invoice_yes_no,
                        "STR_VOID_DATE": ins_ticket_base.str_refund_date,####
                        "STR_TICKET_NO": ins_ticket_base.str_ticket_number,
                        "STR_ACTION": "NEW",
                        "STR_STATUS": "VOID",
                        "STR_COST_CENTRE_CODE": ins_ticket_base.str_cost_centre,
                        "STR_DEPARTMENT_CODE": ins_ticket_base.str_branch_code,
                        "STR_SUPPLIER_CODE": ins_non_iata_settings.str_supplier,
                        "STR_PURCHASE_CUR_CODE": ins_ticket_base.str_tran_currency_rfd,
                        "STR_AIRLINE_NUMERIC_CODE": ins_ticket_base.str_ticketing_airline_numeric_code,
                        "STR_AIRLINE_CHARACTER_CODE": ins_ticket_base.str_ticketing_airline_character_code,
                        "STR_AIRLINE_NAME": ins_ticket_base.str_ticketing_airline_name,
                        "STR_REPORTING_DATE": ins_ticket_base.str_ticket_issue_date or '',
                        "STR_TICKET_ISSUE_DATE": ins_ticket_base.str_ticket_issue_date or '',
                        "DBL_PURCHASE_CUR_PRICE": ins_ticket_base.flt_selling_price,
                        "STR_ACCOUNT_CODE": ins_ticket_base.str_customer_code,
                        "STR_SUB_CUSTOMER_CODE": ins_ticket_base.str_sub_customer_code,
                        "STR_LPO_NO": ins_ticket_base.str_lpo_number,
                        "STR_XO_NO": "",####
                        "STR_TICKET_TYPE": ins_ticket_base.str_ticket_type,
                        "STR_PAX_NAME": ins_ticket_base.str_pax_name_rfd,
                        "STR_SECTOR": ins_ticket_base.str_sector,
                        "CHR_TRAVELER_CLASS": ins_ticket_base.str_class,
                        "STR_GDS": ins_ticket_base.str_crs_company,
                        "STR_BOOKING_STAFF_CODE": ins_ticket_base.str_booking_agent_numeric_code,
                        "STR_BOOKING_STAFF_CHARACTER_CODE": ins_ticket_base.str_booking_agent_code,
                        "STR_BOOKING_STAFF_EMAIL_ID": "",####
                        "STR_STAFF_NAME": "",####
                        "STR_TICKETING_STAFF_CODE": ins_ticket_base.str_ticketing_agent_numeric_code_rfd,
                        "STR_TICKETING_STAFF_CHARACTER_CODE": ins_ticket_base.str_ticketing_agent_code_rfd,
                        "STR_TICKETING_STAFF_EMAIL_ID": "",####
                        "STR_TICKET_REMARK": ins_ticket_base.str_remarks_rfd,
                        "STR_REFUND_DATE": ins_ticket_base.str_refund_date or '',
                        "STR_REFUND_STAFF_CODE": ins_ticket_base.str_ticketing_agent_numeric_code_rfd,
                        "STR_REFUND_STAFF_CHARACTER_CODE": ins_ticket_base.str_ticketing_agent_code_rfd,
                        "STR_REFUND_STAFF_EMAIL_ID": ''
                
                }}
        
        path_void = ins_non_iata_settings.str_url 
                
        try :
            session = Session()
            session.head(path_void)
            dct_void_json = json.dumps(dct_void_json)
            response = session.post(
                                url=path_void,
                                data={'json_data' : dct_void_json },
                                headers={
                                    'Referer': path_void
                                        }
                                    )
        except Exception as msg:
            print('Creating session failed , please check server side working or not !!!VOID')
            
            self.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        'Creating session failed , please check server side working or not !!!###' + str(msg),
                                        str_status
                                        
                                        )
            return

        
        if response.status_code == 200 and (response.content.find('"STATUS":1') != -1 or response.content.find('"STATUS":5') != -1):
            try :
                print((response.content))
            except :
                pass
            print('Void Json upload success')
        else :
            print('Void Json upload Failed')
            self.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        ('Saving Failed Error Code : ' + str(response.status_code) + '    ' +  str(response.content))[:500],
                                        str_status
                                        
                                        )
    
        try :
            session.close()
        except :
            pass
        
    def save_airport(self,lst_sector):
        try:
            cr = self.create_cursor()
            lst_sector = list(set(lst_sector))
            lst_add_airport = []
            str_current_date_time = ins_general_methods.get_current_date_time()

            for str_airport in lst_sector :
                if not str_airport :
                    continue

                cr.execute(""" SELECT
                                vchr_airport_code
                                FROM tbl_airport
                                WHERE vchr_airport_code = %s
                                AND chr_document_status = 'N' """,(str_airport,))
                rst = cr.fetchone()

                if not rst :
                    lst_add_airport.append((str_airport,str_airport,'N',2,str_current_date_time))

            if not lst_add_airport :
                cr.close()
                return
        
            cr.executemany("""  INSERT INTO tbl_airport
                                (
                                vchr_airport_code,
                                vchr_airport_name,
                                chr_document_status,
                                fk_bint_created_user_id,
                                tim_created
                                )
                                VALUES
                                (
                                %s,%s,%s,%s,%s
                                )
                                """,lst_add_airport)



            cr.close()
        
        except Exception as msg:
            cr.close()
            print (msg)
            raise
        
    def add_new_airline_into_db(self, str_airline_chr_code,str_airline_numeric_code, str_airline_name ):

        if  not str_airline_numeric_code:
            str_airline_numeric_code = str_airline_chr_code

        if not str_airline_chr_code:
            str_airline_chr_code = str_airline_numeric_code

        if not str_airline_name:
            str_airline_name =  str_airline_chr_code + str_airline_numeric_code

        str_airline_code = str_airline_chr_code + str_airline_numeric_code
        str_current_date_time = ins_general_methods.get_current_date_time()


        while ins_general_methods.get_account_data(str_airline_code)[0] :
            str_airline_code += '0'
        
        while ins_general_methods.get_account_id_by_name(str_airline_name) :
            str_airline_name += '0'
        try:
            cr = self.create_cursor()
            cr.execute("""
                        INSERT INTO tbl_account
                            (
                            vchr_system_mapping_code ,
                            vchr_account_code ,
                            vchr_account_name ,
                            tim_create_date ,
                            vchr_currency_code ,
                            int_account_type
                            )
                            VALUES
                            (
                            %s,%s,%s,
                            %s,%s,%s
                            ) RETURNING pk_bint_account_id
                            """,
                            (str_airline_code,
                            str_airline_code,
                            str_airline_name,
                            str_current_date_time,
                            ins_general_methods.str_base_currency ,
                            17 ))

            int_account_id = cr.fetchone()[0]
            cr.execute("""
            INSERT INTO tbl_airline
                (
                fk_bint_airline_account_id ,
                vchr_airline_numeric_code ,
                vchr_airline_chr_code ,
                vchr_airline_name ,
                fk_bint_created_user_id ,
                tim_created
                )
                VALUES
                (
                %s, %s, %s,
                %s, %s, %s
                )
                """
                ,(
                int_account_id ,
                str_airline_numeric_code ,
                str_airline_chr_code ,
                str_airline_name ,
                2 ,
                str_current_date_time

                ))
            int_airline_id = ins_general_methods.get_max_value('tbl_airline','pk_bint_airline_id')- 1

            cr.close()
            return int_airline_id ,int_account_id
        except Exception as msg:
            cr.close()
            print (msg)
            raise
    
    def save_emd_sector(self, str_reason_for_issuance):
        try:
            cr = self.create_cursor()
            cr.execute("""SELECT vchr_mpd_vmpd_remarks,
                                    bln_allow_auto_invoice
                                FROM tbl_mpd_vmpd_remarks
                                WHERE vchr_mpd_vmpd_remarks = %s
                                AND chr_document_status = 'N'

                            """,(str_reason_for_issuance,))

            rst = cr.fetchone()
            if rst :
                cr.close()
                return rst ['bln_allow_auto_invoice']

            else :
                str_current_date_time = ins_general_methods.get_current_date_time()

                cr.execute("""INSERT INTO tbl_mpd_vmpd_remarks
                                (
                                vchr_mpd_vmpd_remarks,
                                fk_bint_created_user_id,
                                tim_created
                                )

                                VALUES (
                                        %s, %s, %s
                                        )

                                """ ,(str_reason_for_issuance,
                                    2,
                                    str_current_date_time
                                    ))
                cr.close()
                return True
        except Exception as msg:
            cr.close()
            print (msg)
            raise
            pass

    
    def save_hotel(self,lst_hotel_to_insert):
        #refs #17717
        if not lst_hotel_to_insert :
            return

        str_hotel_code = lst_hotel_to_insert[0]
        str_hotel_name = lst_hotel_to_insert[1]
        str_hotel_address = lst_hotel_to_insert[2]
        str_hotel_phone = lst_hotel_to_insert[3]
        str_hotel_fax = lst_hotel_to_insert[4]
        str_hotel_email = lst_hotel_to_insert[5]
        int_city_id = lst_hotel_to_insert[6]
        
        
        cr = self.create_cursor()
        
        int_limit = 5
        rst = [1]
        while rst :
            str_hotel_code = lst_hotel_to_insert[0][:int_limit].strip() + '_CAP'
            
            if int_limit > 9 :
                str_hotel_code += '0'*(int_limit-8)
            cr.execute("""
                            SELECT fk_bint_hotel_city_id
                            FROM tbl_hotel_master
                            WHERE vchr_hotel_code = %s 
                            AND chr_document_status =  'N'
                        """,(str_hotel_code,))
                        
            rst = cr.fetchone()
            int_limit += 1
                        
        
        try :
            cr.execute("""INSERT INTO  tbl_hotel_master
                            (
                            fk_bint_hotel_city_id ,
                            vchr_hotel_code ,
                            vchr_hotel_name ,
                            vchr_hotel_address ,
                            vchr_hotel_phone ,
                            vchr_hotel_email ,
                            int_star_rating ,
                            chr_document_status ,
                            fk_bint_created_user_id ,
                            tim_created
                            )
                            values
                            (%s,%s,%s,%s,
                                %s,%s,%s,%s,
                                    %s,%s)

                             """,(int_city_id,
                                   str_hotel_code,
                                    str_hotel_name,
                                     str_hotel_address,
                                      str_hotel_phone,
                                        str_hotel_email,
                                         1,
                                         'N',
                                         2,
                                         ins_general_methods.get_current_date_time()
                                    ))
        except :
            ins_general_methods.ins_db.rollback()
            
            pass
        else :
            ins_general_methods.ins_db.commit()
        pass
        
        cr.close()
    
    def save_auto_refund_details(self,ins_ticket_base):
        
        
        str_current_date_time = ins_general_methods.get_current_date_time()
        ins_auto = None
        
        if ins_ticket_base.int_counter_staff_id_rfd and ins_ticket_base.int_counter_staff_id_rfd in ins_general_methods.ins_auto_inv.dct_office_id_data_refund:
            ins_auto = ins_general_methods.ins_auto_inv.dct_office_id_data_refund[ins_ticket_base.int_counter_staff_id_rfd]
        
        if ins_auto:
            if ins_auto.int_customer_id:
               ins_ticket_base.int_account_master_id = ins_auto.int_customer_id
            if ins_auto.bln_capture_agency_charge:
                ins_ticket_base.flt_client_refund_charge = ins_auto.flt_capture_agency_charge
#                ins_ticket_base.flt_supplier_refund_charge = ins_auto.flt_capture_agency_charge
#                ins_ticket_base.flt_supplier_refund_charge_cash = ins_auto.flt_capture_agency_charge
                        
            if ins_auto.int_cost_center_id :    
                ins_ticket_base.int_location_id = ins_auto.int_cost_center_id

            if ins_auto.int_department_id :    
               ins_ticket_base.int_branch_id = ins_auto.int_department_id
        elif ins_general_methods.ins_auto_inv.bln_auto_refund_all_ticket:
            pass
        else :
            return ins_ticket_base
                

        try:
            cr = self.create_cursor()
            cr.execute("""
                            INSERT INTO tbl_auto_invoice_details
                                (
                                vchr_list_of_ticket_numbers ,
                                vchr_pnr_number ,
                                int_total_number_of_tickets ,
                                int_type ,
                                bln_approval ,
                                bln_completed ,
                                tim_created ,
                                chr_supporting_document_type
                                )
                                VALUES
                                (
                                %s, %s, %s,
                                %s, %s, %s,
                                %s, %s
                                )

                                """,(ins_ticket_base.str_ticket_number,
                                     ins_ticket_base.str_pnr_no,
                                     1,
                                     3,
                                     True,
                                     False,
                                     str_current_date_time,
                                     'T'))
                                     
            
            cr.close()
        except Exception as msg:
            raise Exception(msg)
        else :
            return ins_ticket_base
        
    def save_credit_card_transaction_data(self, lst_credit_card_transaction_data):
        cr = self.create_cursor()
    
        cr.executemany("""
                                    INSERT INTO tbl_card_type_wise_payment
                                        (vchr_supporting_doc_no ,
                                        vchr_supporting_doc_type ,
                                        int_card_type ,
                                        vchr_approval_code ,
    
                                        vchr_tran_currency ,
                                        dbl_tran_amount
    
                                        )
                                        VALUES
                                        (
                                        %s, %s, %s, %s, %s, %s
                                        )
                                    """,lst_credit_card_transaction_data)
        cr.close()
    
    def create_fare_data_list(self,ins_ticket_base,lst_fare_common_data,int_supplier_roe = 1,int_customer_roe = 1 ,int_service_type = 1):
        lst_fare_data_save = []
        for lst_fare_data in lst_fare_common_data:
            bln_cust_currency = False
            flt_currency_roe = lst_fare_data[5] #roe used for calculation
            flt_roe = 1 # roe set in table 
            if lst_fare_data[1]:
                flt_roe = int_supplier_roe
            
            if lst_fare_data[3]:
                bln_cust_currency = True
                flt_roe = int_customer_roe
                
            if lst_fare_data[0] or lst_fare_data[2]:
                flt_roe = 1
                
            if not lst_fare_data[6] : #not bln refund and not bln void
                int_pay_back_account_id_1 = ins_ticket_base.int_pay_back_account_id_inv_1
                int_pay_back_account_id_2 = ins_ticket_base.int_pay_back_account_id_inv_2
                int_pay_back_account_id_3 = ins_ticket_base.int_pay_back_account_id_inv_3
                flt_published_fare = ins_ticket_base.flt_published_fare_inv 
                flt_special_fare = ins_ticket_base.flt_special_fare_inv 
                flt_market_fare  = ins_ticket_base.flt_market_fare_inv  
                flt_market_fare_cash = ins_ticket_base.flt_market_fare_cash_inv 
                flt_market_fare_credit  = ins_ticket_base.flt_market_fare_credit_inv  
                flt_market_fare_card_amount = ins_ticket_base.flt_market_fare_card_amount_inv 
                flt_market_fare_uccf_amount = ins_ticket_base.flt_market_fare_uccf_amount_inv 
                flt_total_tax = ins_ticket_base.flt_total_tax_inv 
                flt_total_tax_cash  = ins_ticket_base.flt_total_tax_cash_inv  
                flt_total_tax_credit = ins_ticket_base.flt_total_tax_credit_inv 
                flt_tax_card_amount = ins_ticket_base.flt_tax_card_amount_inv 
                flt_tax_uccf_amount = ins_ticket_base.flt_tax_uccf_amount_inv 
                flt_vat_in = ins_ticket_base.flt_vat_in_inv 
        #        payback commmission1 percentage  = payback commmission1 percentage  
                flt_pay_back_commission_1 = ins_ticket_base.flt_pay_back_commission_inv
                flt_income_amount_percentage_1 = ins_ticket_base.flt_service_charge_percentage_inv
                flt_income_amount_1 = ins_ticket_base.flt_service_charge
                flt_income_amount_percentage_2 = ins_ticket_base.flt_extra_earninig_percentage_inv
                flt_income_amount_2 = ins_ticket_base.flt_extra_earning_inv
                flt_income_amount_percentage_3 = ins_ticket_base.flt_cc_charge_collected_percentage
                flt_income_amount_3 = ins_ticket_base.flt_cc_charge_collected_ext
                flt_expense_amount_percentage_1 = ins_ticket_base.flt_discount_given_percentage_inv
                flt_expense_amount_1 = ins_ticket_base.flt_discount_given_inv
                flt_standard_commn_percentage = ins_ticket_base.flt_std_commn_percentage_inv
                flt_standard_commission = ins_ticket_base.flt_standard_commission
                flt_client_amount  = ins_ticket_base.flt_selling_price  
                flt_client_amount_posting = ins_ticket_base.flt_debited_amount_inv 
                flt_supplier_amount = ins_ticket_base.flt_supplier_amount 
                flt_supplier_amount_posting = ins_ticket_base.flt_net_payable_inv  
                flt_profit  = ins_ticket_base.flt_profit_inv  
                flt_supplier_amt_as_per_bsp  = ins_ticket_base.flt_supplier_amt_as_per_bsp_issue

            else: #bln refund or bln void
                int_pay_back_account_id_1 = ins_ticket_base.int_pay_back_account_id_rfd_1
                int_pay_back_account_id_2 = ins_ticket_base.int_pay_back_account_id_rfd_2
                int_pay_back_account_id_3 = ins_ticket_base.int_pay_back_account_id_rfd_3
                flt_published_fare = ins_ticket_base.flt_published_fare_rfd 
                flt_special_fare = ins_ticket_base.flt_special_fare_rfd 
                flt_market_fare  = ins_ticket_base.flt_market_fare_rfd  
                flt_market_fare_cash = ins_ticket_base.flt_market_fare_cash_rfd 
                flt_market_fare_credit  = ins_ticket_base.flt_market_fare_credit_rfd  
                flt_market_fare_card_amount = ins_ticket_base.flt_market_fare_card_amount_rfd 
                flt_market_fare_uccf_amount = ins_ticket_base.flt_market_fare_uccf_amount_rfd 
                flt_total_tax = ins_ticket_base.flt_total_tax_rfd
                flt_total_tax_cash  = ins_ticket_base.flt_total_tax_cash_rfd
                flt_total_tax_credit = ins_ticket_base.flt_total_tax_credit_rfd
                flt_tax_card_amount = ins_ticket_base.flt_tax_card_amount_rfd
                flt_tax_uccf_amount = ins_ticket_base.flt_tax_uccf_amount_rfd 
                flt_vat_in = ins_ticket_base.flt_vat_in_rfd 
        #        payback commmission1 percentage  = payback commmission1 percentage
                flt_pay_back_commission_1 = ins_ticket_base.flt_pay_back_commission_rfd
                flt_income_amount_percentage_1 = ins_ticket_base.flt_service_charge_percentage_inv
                flt_income_amount_1 = ins_ticket_base.flt_extra_earning_rfd
                flt_income_amount_percentage_2 = ins_ticket_base.flt_extra_earninig_percentage_inv
                flt_income_amount_2 = ins_ticket_base.flt_service_charge_rfd
                flt_income_amount_percentage_3 = 0.0 #ins_ticket_base.flt_cc_charge_collected_percentage
                flt_income_amount_3 = 0.0 #cc charge collected
                flt_expense_amount_percentage_1 = ins_ticket_base.flt_discount_given_percentage_inv
                flt_expense_amount_1 = ins_ticket_base.flt_discount_given_rfd
                flt_standard_commn_percentage = ins_ticket_base.flt_std_commn_percentage_rfd
                flt_standard_commission = ins_ticket_base.flt_standard_commission_rfd
                flt_client_amount  =  ins_ticket_base.flt_client_refund_net
                flt_client_amount_posting = ins_ticket_base.flt_credited_amount_rfd
                flt_supplier_amount = ins_ticket_base.flt_supplier_refund_net 
                flt_supplier_amount_posting = ins_ticket_base.flt_debited_amount_rfd #ins_ticket_base.flt_net_payable_rfd
                flt_profit  = ins_ticket_base.flt_profit_rfd
                flt_supplier_amt_as_per_bsp  = ins_ticket_base.flt_supplier_amt_as_per_bsp_refund
                
            lst_fare_data_save.append([
                    ins_ticket_base.str_ticket_number, # ticket number
                    ins_ticket_base.int_ticket_id, # tbl_ticket id
                    int_service_type , #service type -ticket/service
                    lst_fare_data[0],#bln_base_currency
                    lst_fare_data[1],#bln_tran_currency
                    lst_fare_data[2],#bln_cust_base_currency
                    lst_fare_data[3],#bln_cust_currency
                    lst_fare_data[4],#currency code
                    flt_roe,#roe
                    lst_fare_data[6], #bln refund
                    int_pay_back_account_id_1,
                    int_pay_back_account_id_2,
                    int_pay_back_account_id_3,

                    ins_general_methods.convert_amount_data(flt_published_fare,flt_currency_roe,bln_cust_currency,int_customer_roe), #published fare
                    ins_general_methods.convert_amount_data(flt_special_fare,flt_currency_roe,bln_cust_currency,int_customer_roe), #special fare
                    ins_general_methods.convert_amount_data(flt_market_fare,flt_currency_roe,bln_cust_currency,int_customer_roe), #printing_fare

                    ins_general_methods.convert_amount_data(flt_market_fare,flt_currency_roe,bln_cust_currency,int_customer_roe), #MF
                    ins_general_methods.convert_amount_data(flt_market_fare_cash,flt_currency_roe,bln_cust_currency,int_customer_roe), #MF cash
                    ins_general_methods.convert_amount_data(flt_market_fare_credit,flt_currency_roe,bln_cust_currency,int_customer_roe), #mf credit
                    ins_general_methods.convert_amount_data(flt_market_fare_card_amount,flt_currency_roe,bln_cust_currency,int_customer_roe), #MFcredit-MFuccf
                    ins_general_methods.convert_amount_data(flt_market_fare_uccf_amount,flt_currency_roe,bln_cust_currency,int_customer_roe), #MF uccf

                    ins_general_methods.convert_amount_data(flt_total_tax,flt_currency_roe,bln_cust_currency,int_customer_roe), #tax
                    ins_general_methods.convert_amount_data(flt_total_tax_cash,flt_currency_roe,bln_cust_currency,int_customer_roe), #tax cash
                    ins_general_methods.convert_amount_data(flt_total_tax_credit,flt_currency_roe,bln_cust_currency,int_customer_roe), #tax credit
                    ins_general_methods.convert_amount_data(flt_tax_card_amount,flt_currency_roe,bln_cust_currency,int_customer_roe), #taxCredit - taxUccf
                    ins_general_methods.convert_amount_data(flt_tax_uccf_amount,flt_currency_roe,bln_cust_currency,int_customer_roe), #tax uccf

                    ins_general_methods.convert_amount_data(flt_vat_in,flt_currency_roe,bln_cust_currency,int_customer_roe), #VAT

                    0.0, #payback commmission1 percentage 
                    ins_general_methods.convert_amount_data(flt_pay_back_commission_1,flt_currency_roe,bln_cust_currency,int_customer_roe), #payback service fee1
                    flt_income_amount_percentage_1, #Income amount 1 percentage
                    ins_general_methods.convert_amount_data(flt_income_amount_1,flt_currency_roe,bln_cust_currency,int_customer_roe), #Income amount 1
                    flt_income_amount_percentage_2, #Income amount 2 percentage 
                    ins_general_methods.convert_amount_data(flt_income_amount_2,flt_currency_roe,bln_cust_currency,int_customer_roe), #Income amount 2
                    flt_income_amount_percentage_3, #Income amount 3 percentage 
                    ins_general_methods.convert_amount_data(flt_income_amount_3,flt_currency_roe,bln_cust_currency,int_customer_roe), #Income amount 3
                    flt_expense_amount_percentage_1, #Expense amount 1 percentage 
                    ins_general_methods.convert_amount_data(flt_expense_amount_1,flt_currency_roe,bln_cust_currency,int_customer_roe), #Expense amount 1
                    flt_standard_commn_percentage, #standard commission percentage
                    ins_general_methods.convert_amount_data(flt_standard_commission,flt_currency_roe,bln_cust_currency,int_customer_roe),#standard commission
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_adm_expect,flt_currency_roe,bln_cust_currency,int_customer_roe), #adm expect
                    ins_general_methods.convert_amount_data(flt_client_amount,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_client_amount= selling price
                    ins_general_methods.convert_amount_data(flt_client_amount_posting,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_client_amount_posting = debited amount
                    ins_general_methods.convert_amount_data(flt_supplier_amount,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_supplier_amount
                    ins_general_methods.convert_amount_data(flt_supplier_amount_posting,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_supplier_amount_posting = net payable
                    ins_general_methods.convert_amount_data(flt_profit,flt_currency_roe,bln_cust_currency,int_customer_roe), #profit amount
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_fare_differece,flt_currency_roe,bln_cust_currency,int_customer_roe), #fare difference
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_client_refund_charge,flt_currency_roe,bln_cust_currency,int_customer_roe), #client refund charge
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_supplier_refund_charge,flt_currency_roe,bln_cust_currency,int_customer_roe), #supplier refund charge
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_supplier_refund_charge_cash,flt_currency_roe,bln_cust_currency,int_customer_roe), #supplier charge cash
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_market_fare_card_amount_rfd,flt_currency_roe,bln_cust_currency,int_customer_roe), #supplier charge credit
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_supplier_refund_charge_credit,flt_currency_roe,bln_cust_currency,int_customer_roe), #supplier charge credit card
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_market_fare_uccf_amount_rfd,flt_currency_roe,bln_cust_currency,int_customer_roe), #supplier charge uccf 
                    
                    ])
        return lst_fare_data_save        
        """ins_general_methods.convert_amount_data(flt_supplier_amt_as_per_bsp,flt_currency_roe), #sup amount as per bsp
                    
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_client_refund_net,flt_currency_roe), #client refund net amount
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_supplier_refund_net,flt_currency_roe), #supplier refund net amount
                    ins_general_methods.convert_amount_data(ins_ticket_base.flt_credited_amount_rfd,flt_currency_roe) #credited amount refund """
    
    def save_fare_details_data(self,lst_fare_data):
        cr = self.create_cursor()
        cr.executemany("""INSERT INTO tbl_fare_details
                                        (   vchr_sup_doc_number,
                                            fk_bint_sup_doc_id,
                                            int_service_type,
                                            bln_sup_base_currency_data,
                                            bln_sup_currency_data,
                                            bln_cust_base_currency_data,
                                            bln_cust_currency_data,
                                            vchr_currency,
                                            dbl_roe,
                                            bln_refund_side ,
                                            fk_bint_payback_account_id_1,
                                            fk_bint_payback_account_id_2,
                                            fk_bint_payback_account_id_3,

                                            dbl_published_fare,
                                            dbl_special_fare,
                                            dbl_printing_fare,
                                            dbl_market_fare,
                                            dbl_market_fare_credit,
                                            dbl_market_fare_credit_card,
                                            dbl_market_fare_card_amount,
                                            dbl_market_fare_uccf_amount,
                                            dbl_tax,
                                            dbl_tax_credit,
                                            dbl_tax_credit_card,
                                            dbl_tax_card_amount,
                                            dbl_tax_uccf_amount,

                                            dbl_input_tax,
                                            dbl_payback_amount_percentage_1,
                                            dbl_payback_amount_1,
                                            dbl_income_amount_percentage_1,
                                            dbl_income_amount_1,
                                            dbl_income_amount_percentage_2,
                                            dbl_income_amount_2,
                                            dbl_income_amount_percentage_3,
                                            dbl_income_amount_3,
                                            dbl_expense_amount_percentage_1,
                                            dbl_expense_amount_1,
                                            dbl_std_commission_percentage,
                                            dbl_std_commission,
                                            dbl_adm_expect,
                                            dbl_client_amount,
                                            dbl_client_amount_posting,
                                            dbl_supplier_amount,
                                            dbl_supplier_amount_posting,
                                            dbl_profit,
                                            dbl_fare_difference,
                                            --dbl_supplier_bsp_file_amount,
                                            dbl_agency_charge_rfd,
                                            dbl_sup_charge_rfd,
                                            dbl_sup_charge_credit_rfd,
                                            dbl_sup_charge_card_amount_rfd,
                                            dbl_sup_charge_credit_card_rfd,
                                            dbl_sup_charge_uccf_amount_rfd
                                            --dbl_client_net_rfd,
                                            --dbl_supplier_net_rfd,
                                            --dbl_credited_amount_rfd
                                            )
                                        VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s)""" , lst_fare_data)
        cr.close()
        
    def create_fare_data_list_for_voucher(self,ins_service_base):
        lst_temp_fare_common_data = []
        bln_cust_base = False
        if not ins_service_base.str_cust_currency :
            bln_cust_base = True
        if ins_service_base.str_tran_currency == ins_general_methods.str_base_currency :
            if ins_service_base.str_tran_currency == ins_service_base.str_cust_currency:
#                    print ('all are same')
                lst_temp_fare_common_data = [[True, #bln_sup_base_currency
                                            True, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_service_base.str_tran_currency, #currency -all are same
                                            1,#roe
                                            False]] #bln_refund
            else:
#                    print ('Tran and base are same, Cust different')
                lst_temp_fare_common_data = [[True, #bln_sup_base_currency
                                            True, #bln_tran_currency
                                            bln_cust_base, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_service_base.str_tran_currency, #currency -Tran and base are same
                                            1,#roe
                                            False]] #bln_refund
                if ins_service_base.str_cust_currency:
                    lst_temp_fare_common_data.extend([[False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_service_base.str_cust_currency, #currency - Cust different
                                            ins_service_base.flt_supplier_currency_roe, #roe
                                            False],#bln_refund
                                            [False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_general_methods.str_base_currency, #currency - Cust different
                                            ins_service_base.flt_supplier_currency_roe, #roe
                                            False]
                                            ]) 
        elif ins_service_base.str_tran_currency == ins_service_base.str_cust_currency:
#                print ('Tran and cust are same, Base different')
            lst_temp_fare_common_data = [[False, #bln_sup_base_currency
                                            True, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_service_base.str_tran_currency, #currency -Tran and Cust are same
                                            1, #roe 
                                            False ],#bln_refund
                                            [True, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_general_methods.str_base_currency, #currency - base different
                                            ins_service_base.flt_supplier_currency_roe,#roe
                                            False] #bln_refund
                                            ] 
        elif ins_service_base.str_cust_currency == ins_general_methods.str_base_currency:
#                print ('Cust and base are same, Tran different')
            lst_temp_fare_common_data = [[False, #bln_sup_base_currency
                                            True, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_service_base.str_tran_currency, #currency - Tran different
                                            1, #roe
                                            False],#bln_refund
                                            [True, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_general_methods.str_base_currency, #currency - Tran different
                                            ins_service_base.flt_supplier_currency_roe, #roe
                                            False],
                                            [False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_general_methods.str_base_currency, #currency -Cust and base are same
                                            ins_service_base.flt_supplier_currency_roe,#roe
                                            False] #bln_refund
                                        ] 
        else:
#                print ('all are different')
            lst_temp_fare_common_data = [[False, #bln_sup_base_currency
                                            True, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_service_base.str_tran_currency, #currency -Tran 
                                            1,#roe
                                            False],#bln_refund
                                            [True, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            bln_cust_base, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_general_methods.str_base_currency, #currency - base
                                            ins_service_base.flt_supplier_currency_roe,#roe
                                            False] #bln_refund
                                        ] 
            if ins_service_base.str_cust_currency:
                lst_temp_fare_common_data.extend([[False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            False, #bln_cust_base_currency
                                            True, #bln_cust_currency
                                            ins_service_base.str_cust_currency, #currency - Cust
                                            ins_service_base.flt_supplier_currency_roe, #roe
                                            False],#bln_refund
                                            [False, #bln_sup_base_currency
                                            False, #bln_tran_currency
                                            True, #bln_cust_base_currency
                                            False, #bln_cust_currency
                                            ins_general_methods.str_base_currency, #currency - Cust
                                            ins_service_base.flt_supplier_currency_roe, #roe
                                            False]] #bln_refund
                                            ) 

        int_customer_roe = ins_service_base.flt_cust_currency_roe

        lst_fare_data_save = []
        for lst_fare_data in lst_temp_fare_common_data:
            bln_cust_currency = False
            flt_currency_roe = lst_fare_data[5] #roe used for calculation
            flt_roe = 1 # roe set in table 
            if lst_fare_data[1]:
                flt_roe = ins_service_base.flt_supplier_currency_roe
            
            if lst_fare_data[3]:
                bln_cust_currency = True
                flt_roe = int_customer_roe
                
            if lst_fare_data[0] or lst_fare_data[2]:
                flt_roe = 1
            
            int_voucher_table_id = None
            int_service_type = None
            if ins_service_base.str_voucher_type == 'H' :
                int_voucher_table_id = ins_general_methods.get_hotel_voucher_details(ins_service_base.str_voucher_number)
                int_service_type = 2
            elif ins_service_base.str_voucher_type == 'C' :
                int_voucher_table_id = ins_general_methods.get_car_voucher_details(ins_service_base.str_voucher_number)
                int_service_type = 3
            elif ins_service_base.str_voucher_type == 'O' :
                int_voucher_table_id = ins_general_methods.get_other_voucher_details(ins_service_base.str_voucher_number)
                int_service_type = 4
                
            lst_fare_data_save.append([
                    ins_service_base.str_voucher_number, # ticket number
                    int_voucher_table_id, # tbl_ticket id
                    int_service_type , #service type -ticket/service
                    lst_fare_data[0],#bln_base_currency
                    lst_fare_data[1],#bln_tran_currency
                    lst_fare_data[2],#bln_cust_base_currency
                    lst_fare_data[3],#bln_cust_currency
                    lst_fare_data[4],#currency code
                    flt_roe,#roe
                    lst_fare_data[6], #bln refund
                    None,
                    None,
                    None,

                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #published fare
                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #special fare
                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #printing_fare

                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #MF
                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_credit_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #MF cash
                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_credit_card_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #mf credit
                    ins_general_methods.convert_amount_data(ins_service_base.flt_fare_credit_card_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #MFcredit-MFuccf
                    0.0, #MF uccf

                    ins_general_methods.convert_amount_data(ins_service_base.flt_total_tax_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #tax
                    ins_general_methods.convert_amount_data(ins_service_base.flt_total_tax_credit_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #tax cash
                    ins_general_methods.convert_amount_data(ins_service_base.flt_total_tax_credit_card_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #tax credit
                    ins_general_methods.convert_amount_data(ins_service_base.flt_total_tax_credit_card_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #taxCredit - taxUccf
                    0.0, #tax uccf

                    0.0, #VAT

                    0.0, #payback commmission1 percentage 
                    0.0, #payback service fee1
                    ins_service_base.flt_service_fee_percentage, #Income amount 1 percentage
                    ins_general_methods.convert_amount_data(ins_service_base.flt_service_fee,flt_currency_roe,bln_cust_currency,int_customer_roe), #Income amount 1
                    0.0, #Income amount 2 percentage 
                    0.0, #Income amount 2
                    0.0, #Income amount 3 percentage 
                    0.0, #Income amount 3
                    ins_service_base.flt_discount_percentage, #Expense amount 1 percentage 
                    ins_general_methods.convert_amount_data(ins_service_base.flt_discount,flt_currency_roe,bln_cust_currency,int_customer_roe), #Expense amount 1
                    0.0, #standard commission percentage
                    0.0,#standard commission
                    0.0, #adm expect
                    ins_general_methods.convert_amount_data(ins_service_base.flt_selling_price_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_client_amount= selling price
                    ins_general_methods.convert_amount_data(ins_service_base.flt_debited_amount_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_client_amount_posting = debited amount
                    ins_general_methods.convert_amount_data(ins_service_base.flt_gross_payable_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_supplier_amount
                    ins_general_methods.convert_amount_data(ins_service_base.flt_net_payable_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #dbl_supplier_amount_posting = net payable
                    ins_general_methods.convert_amount_data(ins_service_base.flt_profit_inv,flt_currency_roe,bln_cust_currency,int_customer_roe), #profit amount
                    0.0, #fare difference
                    0.0, #client refund charge
                    0.0, #supplier refund charge
                    0.0, #supplier charge cash
                    0.0, #supplier charge credit
                    0.0, #supplier charge credit card
                    0.0 #supplier charge uccf 
                    
                    ])
        return lst_fare_data_save
        
    def save_ticket_log_data(self,str_ticket_number,str_current_date_time,str_transaction_type = 'SALE'):
        try:
            dct_ticket_log = {}
            bln_refund = 'FALSE'
            if str_transaction_type != 'SALE' : #either SALE OR REFUND
                bln_refund = 'TRUE'
            
            cr = self.create_cursor()
            
            cr.execute("""SELECT json_agg(row_to_json(tbl_ticket)) AS json_ticket_details,
                                pk_bint_ticket_id
                            FROM tbl_ticket 
                            WHERE vchr_ticket_number = %s 
                            GROUP BY pk_bint_ticket_id """,(str_ticket_number,))
            rst_ticket_table = cr.fetchone()
            
            dct_ticket_log['tbl_ticket'] = rst_ticket_table['json_ticket_details'][0]
            
            cr.execute("""SELECT json_agg(row_to_json(tbl_fare_details)) AS json_fare_details
                            FROM tbl_fare_details 
                            WHERE vchr_sup_doc_number = %s
                                  AND bln_refund_side = %s """,(str_ticket_number,bln_refund))
            rst_fare_table = cr.fetchone()
            dct_ticket_log['tbl_fare_details'] = rst_fare_table['json_fare_details']
            
            cr.execute("""SELECT json_agg(row_to_json(tbl_sector_details)) AS json_sector_details
                            FROM tbl_sector_details 
                            WHERE vchr_ticket_number = %s """,(str_ticket_number,))
            rst_sector_table = cr.fetchone()
            
            dct_ticket_log['tbl_sector_details'] = rst_sector_table['json_sector_details']
            
            cr.execute("""INSERT INTO tbl_ticket_logs
                                        (   vchr_ticket_number,
                                            json_log_item,
                                            time_created,
                                            fk_bint_created_user_id,
                                            fk_bint_ticket_id,
                                            vchr_ticket_action,
                                            vchr_transaction_type
                                            )
                                        VALUES( %s, %s, %s, %s, %s, %s, %s)""" ,
                                        (   str_ticket_number,
                                            json.dumps(dct_ticket_log),
                                            str_current_date_time,
                                            2,
                                            rst_ticket_table['pk_bint_ticket_id'],
                                            'CAPTURE_CREATE',
                                            str_transaction_type
                                        ))
            cr.close()
        except Exception as msg:
            cr.close()
            print ('Saving log failed..'+str(msg))
            raise Exception('Saving log failed..'+str(msg))
        
    def save_other_voucher_log_data(self,str_voucher_number,str_current_date_time,str_service_type):
        try:
            dct_other_voucher_log = {}
            bln_refund = 'FALSE'
            str_transaction_type = 'SALE'
            str_table_name = ''
            str_table_log_name = ''
            str_pk_bint_name = ''
            str_fk_bint_log_name = ''
            
            if str_service_type == 'H' :
                str_table_name = 'tbl_hotel_voucher'
                str_table_log_name = 'tbl_hotel_voucher_logs'
                str_pk_bint_name = 'pk_bint_hotel_voucher_id'
                str_fk_bint_log_name = 'fk_bint_hotel_voucher_id'
            elif str_service_type == 'C' :
                str_table_name = 'tbl_car_voucher'
                str_table_log_name = 'tbl_car_voucher_logs'
                str_pk_bint_name = 'pk_bint_car_voucher_id'
                str_fk_bint_log_name = 'fk_bint_car_voucher_id'
            elif str_service_type == 'O' :
                str_table_name = 'tbl_other_voucher'
                str_table_log_name = 'tbl_other_voucher_logs'
                str_pk_bint_name = 'pk_bint_other_voucher_id'
                str_fk_bint_log_name = 'fk_bint_other_voucher_id'
            
            cr = self.create_cursor()
            
            str_voucher_query = """SELECT json_agg(row_to_json(%s)) AS json_voucher_details,
                                %s AS pk_bint_voucher_id
                            FROM %s 
                            WHERE vchr_voucher_number = '%s'
                            GROUP BY %s """%(str_table_name,str_pk_bint_name,str_table_name,str_voucher_number,str_pk_bint_name)
            cr.execute(str_voucher_query)
            rst_voucher_table = cr.fetchone()
            
            dct_other_voucher_log[str_table_name] = rst_voucher_table['json_voucher_details'][0]
            
            cr.execute("""SELECT json_agg(row_to_json(tbl_fare_details)) AS json_fare_details
                            FROM tbl_fare_details 
                            WHERE vchr_sup_doc_number = %s
                                  AND bln_refund_side  = %s""",(str_voucher_number,bln_refund))
            rst_fare_table = cr.fetchone()
            dct_other_voucher_log['tbl_fare_details'] = rst_fare_table['json_fare_details']
            
            cr.execute("""INSERT INTO %s
                                        (   vchr_voucher_number,
                                            json_log_item,
                                            time_created,
                                            fk_bint_created_user_id,
                                            %s,
                                            vchr_voucher_action,
                                            vchr_transaction_type
                                            )
                                        VALUES( '%s','%s','%s',%s, %s,'%s','%s')""" %
                                        (   str_table_log_name,
                                            str_fk_bint_log_name,
                                            str_voucher_number,
                                            json.dumps(dct_other_voucher_log),
                                            str_current_date_time,
                                            2,
                                            rst_voucher_table['pk_bint_voucher_id'],
                                            'CAPTURE_CREATE',
                                            str_transaction_type
                                        ))
            cr.close()
        except Exception as msg:
            cr.close()
            print ('Saving log failed..'+str(msg))
            raise Exception('Saving log failed..'+str(msg))