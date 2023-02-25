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
import datetime
import json
import copy
from requests import Session

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
        ins_general_methods.connect_db()
        ins_general_methods.reload_data()
#    else:
#        ins_general_methods.reload_data()
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
        lst_extra_capturing_fields = []
        lst_ticket_sale_details = []
        lst_ticket_refund_details = []
        lst_credit_card_transaction_data = []
        lst_original_ticket_details = []
        lst_setcor_data = []
        lst_tkt_no = []
        lst_setcor_data = []
        dct_tkts = {}
        lst_tax_details = []   #45552
        lst_tax_details_rfd = []

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

            #// Changed the position of code for finding supplier id
            cr.execute("""SELECT ts.fk_bint_supplier_id,
                                    ts.fk_bint_ticket_type,
                                    ta.fk_bint_cost_center_id
                                
                            FROM tbl_ticket_stock_allocation ta
                             INNER JOIN tbl_ticket_stock ts
                                 ON ts.pk_bint_ticket_stock_id = ta.fk_bint_ticket_stock_id
                                    AND ta.chr_document_status = 'N'
                                    AND ts.chr_document_status = 'N'
                                    
                             INNER JOIN tbl_account tac
                                 ON fk_bint_supplier_id = tac.pk_bint_account_id
                                     AND tac.chr_document_status = 'N'
                                     AND tac.vchr_currency_code = %s
                                     
                                    WHERE ta.vchr_starting_ticket_number :: BIGINT <= %s
                                    AND ta.vchr_ending_ticket_number :: BIGINT >= %s""",
                                      ( ins_ticket_base.str_defult_currency_code ,
                                      ins_ticket_base.str_ticket_number,
                                       ins_ticket_base.str_ticket_number)
                )
#                Refere 14796
            rst = cr.fetchone()
            if rst:
                ins_ticket_base.int_ticket_type_id = rst['fk_bint_ticket_type']
                ins_ticket_base.int_supplier_id = rst['fk_bint_supplier_id']
                ins_ticket_base.int_location_id =  rst['fk_bint_cost_center_id']
                ins_ticket_base.chr_supplier_type = 'S'
            elif ins_ticket_base.str_ticketing_agency_iata_no:
                cr.execute(""" SELECT tc.fk_bint_creditor_account_id,
                                    ta.vchr_currency_code
                       FROM tbl_creditor tc
                       INNER JOIN tbl_account ta
                        ON tc.fk_bint_creditor_account_id = ta.pk_bint_account_id
                        AND ta.chr_document_status = 'N'
                        AND tc.chr_document_status = 'N'
                        AND ta.vchr_currency_code = %s
                       WHERE substring(tc.vchr_iata_number,1,7) = %s
                       AND tc.chr_document_status = 'N' """,
                         
                         ( ins_ticket_base.str_defult_currency_code,
                            ins_ticket_base.str_ticketing_agency_iata_no[0:7]))

                rst = cr.fetchone()
                if rst:
                    if rst['vchr_currency_code'] == ins_ticket_base.str_defult_currency_code :
                        ins_ticket_base.int_supplier_id = rst['fk_bint_creditor_account_id']
                        ins_ticket_base.chr_supplier_type = 'S'
                        pass
                    else :
                        ins_ticket_base.int_supplier_id = None
                        ins_ticket_base.chr_supplier_type = ''

            else:
                if  ins_ticket_base.str_ticketing_agency_iata_no == '' and ins_ticket_base.str_pnr_current_owner_iata_no != '':
                    cr.execute("""SELECT tc.fk_bint_creditor_account_id,
                                    ta.vchr_currency_code
                       FROM tbl_creditor tc
                       INNER JOIN tbl_account ta
                        ON tc.fk_bint_creditor_account_id = ta.pk_bint_account_id
                        AND ta.chr_document_status = 'N'
                        AND tc.chr_document_status = 'N'
                        AND ta.vchr_currency_code = %s
                        
                       WHERE substring(tc.vchr_iata_number,1,7) = %s
                       AND tc.chr_document_status = 'N'""",
                             (ins_ticket_base.str_defult_currency_code, 
                                ins_ticket_base.str_pnr_current_owner_iata_no[0:7]))
                    rst = cr.fetchone()
                    if rst:
                        if rst['vchr_currency_code'] == ins_ticket_base.str_defult_currency_code :
                            ins_ticket_base.int_supplier_id = rst['fk_bint_creditor_account_id']
                            ins_ticket_base.chr_supplier_type = 'S'
                            pass
                        else :
                            ins_ticket_base.int_supplier_id = None
                            ins_ticket_base.chr_supplier_type = ''
                pass

            lst_tkt_no.append(ins_ticket_base.str_ticket_number)
            rst = ins_general_methods.get_ticket_details(ins_ticket_base.str_ticket_number)
            
            if rst:
                
                if rst[0]['bln_conjuction_ticket'] :
                    str_main_ticket = ins_general_methods.get_conjuntion_ticket_ticket(ins_ticket_base.str_ticket_number)
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
                   
                    if rst[0]['dat_ticket_issue'] > dat_refund_date:#// refer 22125
                        dat_refund_date = rst[0]['dat_ticket_issue']
                        
                    dat_refund_date = dat_refund_date.strftime("%d/%m/%Y")
                    ins_ticket_base.str_refund_date = dat_refund_date

                    if ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                        ins_ticket_base.flt_supplier_currency_roe_rfd = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_refund_date)

                    #40225
                    if ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                        ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.str_cust_currency_rfd = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,\
                                ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
                               
                    ins_ticket_base.int_ticket_id = rst[0]['pk_bint_ticket_id']
                    ins_ticket_base.int_ticket_type_id = rst[0]['fk_bint_ticket_type_id']
                    ins_ticket_base.int_region_id = rst[0]['fk_bint_region_id']
                    ins_ticket_base.int_class_id = rst[0]['fk_bint_booking_class_id']
                    ins_ticket_base.int_airline_id = rst[0]['fk_bint_airline_id']
                    ins_ticket_base.int_airline_account_id = rst[0]['fk_bint_airline_account_id']
                    ins_ticket_base.str_class_group = rst[0]['vchr_class']
                    ins_ticket_base.str_class = rst[0]['vchr_class_chr']
                    ins_ticket_base.str_return_class = rst[0]['vchr_return_class_chr']
                    ins_ticket_base.int_return_class_id = rst[0]['fk_bint_return_booking_class_id']
                    ins_ticket_base.str_return_class_group = rst[0]['vchr_return_class']
                    ins_ticket_base.str_last_conjection_ticket_number = rst[0]['vchr_last_conjection_ticket_number']
                    ins_ticket_base.str_booking_agent_code = rst[0]['vchr_booking_agent_code']
                    ins_ticket_base.str_booking_agent_numeric_code = rst[0]['vchr_booking_agent_numeric_code']
                    if not ins_ticket_base.int_booking_agent_counter_staff_id:
                        ins_ticket_base.int_booking_agent_counter_staff_id = rst[0]['fk_bint_booking_agent_counter_staff_id']
                    ins_ticket_base.int_card_payment_type_rfd = rst[0]['int_card_payment_type']
                    ins_ticket_base.str_original_issue = rst[0]['vchr_original_issue']

                    if not ins_ticket_base.str_ticketing_agent_code :
                        ins_ticket_base.str_ticketing_agent_code = rst[0]['vchr_ticketing_agent_code_inv']
                        ins_ticket_base.str_ticketing_agent_numeric_code = rst[0]['vchr_ticketing_agent_numeric_code_inv']
                        ##Refer #14726
                        ins_ticket_base.int_counter_staff_id_inv = rst[0]['fk_bint_counter_staff_id_inv']
                    
                    if not ins_ticket_base.str_pax_name :
                        ins_ticket_base.str_pax_name = rst[0]['vchr_pax_name_inv'] or ''
                    if not ins_ticket_base.str_sector or ins_ticket_base.str_sector == '/':
                        ins_ticket_base.str_sector = rst[0]['vchr_sector_inv'] or ''
                    if not ins_ticket_base.int_no_of_pax_inv :
                        ins_ticket_base.int_no_of_pax_inv = rst[0]['int_no_of_pax_inv'] or 0
                    if not ins_ticket_base.int_no_of_segments :
                        ins_ticket_base.int_no_of_segments = rst[0]['int_no_of_segments_inv'] or 0
                    ins_ticket_base.int_location_id_rfd = rst[0]['fk_bint_cost_center_id_inv']  #44032					    
                    ins_ticket_base.int_branch_id_rfd = rst[0]['fk_bint_department_id_inv']
                        
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
                    
                    if not ins_ticket_base.int_account_master_id and rst[0]['fk_bint_customer_account_id_inv'] :
                        ins_ticket_base.int_account_master_id = rst[0]['fk_bint_customer_account_id_inv']
                            
                    ins_ticket_base.int_counter_staff_id_rfd = ins_ticket_base.int_counter_staff_id_inv or rst[0]['fk_bint_counter_staff_id_inv']
                    
                    ins_ticket_base.flt_market_fare_card_amount_rfd = ins_ticket_base.flt_market_fare_card_amount_inv
                    ins_ticket_base.flt_market_fare_uccf_amount_rfd = ins_ticket_base.flt_market_fare_uccf_amount_inv
                    ins_ticket_base.flt_tax_card_amount_rfd = ins_ticket_base.flt_tax_card_amount_inv	
                    ins_ticket_base.flt_tax_uccf_amount_rfd = ins_ticket_base.flt_tax_uccf_amount_inv
                    
                    ins_ticket_base.flt_market_fare_card_amount_inv = 0
                    ins_ticket_base.flt_market_fare_uccf_amount_inv = 0
                    ins_ticket_base.flt_tax_card_amount_inv = 0
                    ins_ticket_base.flt_tax_uccf_amount_inv = 0
                    ins_ticket_base.str_pnr_no = rst[0]['vchr_pnr_number']
                    ins_ticket_base.str_fare_basis = rst[0]['vchr_fare_basis']
#                    ins_ticket_base.flt_sup_charge_card_amount_rfd = 0 	
#                    ins_ticket_base.flt_sup_charge_uccf_amount_rfd = 0


                    self.save_refund_tickets(ins_ticket_base)
                    if lst_original_ticket_details:
                        #36508
                        self.save_refund_void_data_of_original_tickets(lst_original_ticket_details , 
                                            ins_ticket_base.str_refund_date , 
                                            'R',
                                            ins_ticket_base.str_file_name_rfd ,
                                            ins_ticket_base.str_base_currency_rfd ,
                                            ins_ticket_base.str_tran_currency_rfd ,
                                            ins_ticket_base.int_account_master_id ,
                                            ins_ticket_base.int_supplier_id_rfd ,
                                            ins_ticket_base.int_location_id_rfd ,  #44032
                                            ins_ticket_base.int_branch_id_rfd ,
                                            ins_ticket_base.str_ticketing_agent_code_rfd ,
                                            ins_ticket_base.str_ticketing_agent_numeric_code_rfd ,
                                            ins_ticket_base.int_counter_staff_id_rfd
                                            
                                            )  # refer bug no:19895
                    return
                else:
                    if rst[0]['dat_refund'] is not None:
                        continue
                    if rst[0]['dat_ticket_issue']:
                        dat_ticket_issue = rst[0]['dat_ticket_issue'].strftime("%d/%m/%Y")
                        if dat_ticket_issue is not None:
                            if ins_ticket_base.str_void_date: #void date format should be a string as '%d/%m/%Y'
                                dat_void_date = datetime.datetime.strptime(ins_ticket_base.str_void_date,"%d/%m/%Y").date()
                                if rst[0]['dat_ticket_issue'] > dat_void_date:#// refer 22125
                                    dat_void_date = rst[0]['dat_ticket_issue']
                                    
                                dat_void_date = dat_void_date.strftime("%d/%m/%Y")
                                ins_ticket_base.str_refund_date = dat_void_date
#                                ins_ticket_base.str_ticket_issue_date = dat_void_date
                                ins_ticket_base.chr_ticket_status = 'V'
                                
                            if ins_general_methods.ins_capture_base.bln_multi_currency and ins_ticket_base.str_defult_currency_code != ins_general_methods.str_base_currency:
                                ins_ticket_base.flt_supplier_currency_roe_rfd = ins_general_methods.get_roe_of_currency_for_a_date(ins_ticket_base.str_defult_currency_code,
                                                                                                ins_ticket_base.str_refund_date)
                            #40225
                            if ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                                ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.str_cust_currency_rfd = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,
                                                    ins_ticket_base.str_refund_date, ins_ticket_base.str_base_currency)

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
                            ins_ticket_base.int_region_id = rst[0]['fk_bint_region_id']
                            ins_ticket_base.int_class_id = rst[0]['fk_bint_booking_class_id']
                            ins_ticket_base.int_airline_id = rst[0]['fk_bint_airline_id']
                            ins_ticket_base.int_airline_account_id = rst[0]['fk_bint_airline_account_id']
                            ins_ticket_base.str_class_group = rst[0]['vchr_class'] or ''
                            ins_ticket_base.str_class = rst[0]['vchr_class_chr'] or ''
                            ins_ticket_base.str_return_class = rst[0]['vchr_return_class_chr'] or ''
                            ins_ticket_base.int_return_class_id = rst[0]['fk_bint_return_booking_class_id']
                            ins_ticket_base.str_return_class_group = rst[0]['vchr_return_class'] or ''
                            ins_ticket_base.str_last_conjection_ticket_number = rst[0]['vchr_last_conjection_ticket_number']
                            ins_ticket_base.str_booking_agent_code = rst[0]['vchr_booking_agent_code']
                            ins_ticket_base.str_original_issue = ''

                            ins_ticket_base.str_booking_agent_numeric_code = rst[0]['vchr_booking_agent_numeric_code']
                            if not ins_ticket_base.int_booking_agent_counter_staff_id:
                                ins_ticket_base.int_booking_agent_counter_staff_id = rst[0]['fk_bint_booking_agent_counter_staff_id']
                            if not ins_ticket_base.str_ticketing_agent_code :
                                ins_ticket_base.str_ticketing_agent_code = rst[0]['vchr_ticketing_agent_code_inv']
                                ins_ticket_base.str_ticketing_agent_numeric_code = rst[0]['vchr_ticketing_agent_numeric_code_inv']
                                ins_ticket_base.int_counter_staff_id_inv = rst[0]['fk_bint_counter_staff_id_inv']

                            if not ins_ticket_base.str_pax_name :
                                ins_ticket_base.str_pax_name = rst[0]['vchr_pax_name_inv'] or ''

                            if not ins_ticket_base.str_sector or ins_ticket_base.str_sector == '/':
                                ins_ticket_base.str_sector = rst[0]['vchr_sector_inv'] or ''
                            if not ins_ticket_base.int_no_of_pax_inv :
                                ins_ticket_base.int_no_of_pax_inv = rst[0]['int_no_of_pax_inv'] or 0
                            if not ins_ticket_base.int_no_of_segments :
                                ins_ticket_base.int_no_of_segments = rst[0]['int_no_of_segments_inv'] or 0

                            ins_ticket_base.str_base_currency_rfd = ins_ticket_base.str_base_currency
                            ins_ticket_base.str_tran_currency_rfd = rst[0]['vchr_tran_currency_inv'] #36583
                            ins_ticket_base.str_pax_name_rfd = ins_ticket_base.str_pax_name
                            ins_ticket_base.int_number_of_segments_rfd = ins_ticket_base.int_no_of_segments
                            ins_ticket_base.str_ticketing_agent_code_rfd = ins_ticket_base.str_ticketing_agent_code
                            ins_ticket_base.str_ticketing_agent_numeric_code_rfd = ins_ticket_base.str_ticketing_agent_numeric_code
#                            ins_ticket_base.flt_supplier_currency_roe_rfd = ins_ticket_base.flt_supplier_currency_roe
                            ins_ticket_base.flt_supplier_currency_roe = 1
                            ins_ticket_base.flt_cust_currency_roe = 1
                            ins_ticket_base.flt_service_charge_rfd = ins_ticket_base.flt_service_charge
                            ins_ticket_base.flt_service_charge = 0.00
                            ins_ticket_base.flt_service_charge_percentage_rfd = ins_ticket_base.flt_service_charge_percentage_inv
                            ins_ticket_base.flt_service_charge_percentage_inv = 0.00
                            ins_ticket_base.int_company_id_rfd = ins_ticket_base.int_company_id or 1
                            ins_ticket_base.int_company_id = None
                            ins_ticket_base.int_no_of_pax_rfd = ins_ticket_base.int_no_of_pax_inv
                            ins_ticket_base.int_location_id_rfd = rst[0]['fk_bint_cost_center_id_inv']  #44032					    
                            ins_ticket_base.int_branch_id_rfd = rst[0]['fk_bint_department_id_inv']

                            ins_ticket_base.int_supplier_id_rfd = ins_ticket_base.int_supplier_id
                            ins_ticket_base.int_supplier_id = None
                            if not ins_ticket_base.int_account_master_id: #37364
                                ins_ticket_base.int_account_master_id = rst[0]['fk_bint_customer_account_id_inv'] 
                            
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
                            
                            ins_ticket_base.int_counter_staff_id_rfd = ins_ticket_base.int_counter_staff_id_inv
                            
                            ins_ticket_base.flt_market_fare_card_amount_rfd = ins_ticket_base.flt_market_fare_card_amount_inv
                            ins_ticket_base.flt_market_fare_uccf_amount_rfd = ins_ticket_base.flt_market_fare_uccf_amount_inv
                            ins_ticket_base.flt_tax_card_amount_rfd = ins_ticket_base.flt_tax_card_amount_inv	
                            ins_ticket_base.flt_tax_uccf_amount_rfd = ins_ticket_base.flt_tax_uccf_amount_inv

                            ins_ticket_base.flt_market_fare_card_amount_inv = 0
                            ins_ticket_base.flt_market_fare_uccf_amount_inv = 0
                            ins_ticket_base.flt_tax_card_amount_inv = 0
                            ins_ticket_base.flt_tax_uccf_amount_inv = 0
                            
                            self.save_void_ticket(ins_ticket_base)

                            self.update_refund_sides_when_ticket_void(ins_ticket_base.str_ticket_number)
                            
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
                    
                    #40225
                    if ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                        ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.str_cust_currency_rfd = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,
                                            ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
                               
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
                    ins_ticket_base.flt_cust_currency_roe = 1
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
#                    ins_ticket_base.int_counter_staff_id_inv = None
                    
                    ins_ticket_base.int_location_id_rfd = ins_ticket_base.int_location_id
                    ins_ticket_base.int_location_id = None
                    
                    ins_ticket_base.int_branch_id_rfd = ins_ticket_base.int_branch_id
                    ins_ticket_base.int_branch_id = None
                    
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
                                            ins_ticket_base.str_base_currency_rfd ,
                                            ins_ticket_base.str_tran_currency_rfd ,
                                            ins_ticket_base.int_account_master_id ,
                                            ins_ticket_base.int_supplier_id_rfd ,
                                            ins_ticket_base.int_location_id_rfd ,  #44032
                                            ins_ticket_base.int_branch_id_rfd ,
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
                    
                    #40225
                    if ins_ticket_base.str_ticket_issue_date  or ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                        ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.str_cust_currency = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,
                               ins_ticket_base.str_ticket_issue_date or ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
                          
                    if ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                        ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.str_cust_currency_rfd = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,
                                        ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
                               
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
                    ins_ticket_base.flt_cust_currency_roe = 1
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
#                    ins_ticket_base.int_counter_staff_id_inv = None
                    
                    ins_ticket_base.int_location_id_rfd = ins_ticket_base.int_location_id
                    ins_ticket_base.int_location_id = None
                    
                    ins_ticket_base.int_branch_id_rfd = ins_ticket_base.int_branch_id
                    ins_ticket_base.int_branch_id = None
                    
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
                                            ins_ticket_base.str_base_currency_rfd ,
                                            ins_ticket_base.str_tran_currency_rfd ,
                                            ins_ticket_base.int_account_master_id ,
                                            ins_ticket_base.int_supplier_id_rfd ,
                                            ins_ticket_base.int_location_id_rfd ,  #44032
                                            ins_ticket_base.int_branch_id_rfd ,
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

            #40225
            if ins_ticket_base.str_ticket_issue_date  or ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.str_cust_currency = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,\
                       ins_ticket_base.str_ticket_issue_date or ins_ticket_base.str_refund_date, ins_ticket_base.str_base_currency)
            
            if ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                    ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.str_cust_currency_rfd = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,\
                                            ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
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
            
            str_current_date_time = ins_general_methods.get_current_date_time()

            lst_ticket_details.append([ins_ticket_base.int_location_id,
                                ins_ticket_base.str_pnr_no,
                                ins_ticket_base.str_ticket_number,
                                ins_ticket_base.str_last_conjection_ticket_number,
                                ins_ticket_base.str_original_issue,
                                ins_ticket_base.str_lpo_number,
                                ins_ticket_base.chr_ticket_status,
                                ins_ticket_base.chr_bsp_ticket_status,
                                ins_ticket_base.chr_system_sale_part_action,
                                ins_ticket_base.chr_ticket_category,
                                ins_ticket_base.int_dc_card,
                                ins_ticket_base.bln_glpo,
                                ins_ticket_base.int_supplier_id,
                                ins_ticket_base.str_ticket_issue_date,
                                ins_ticket_base.int_ticket_type_id,
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
                                ins_ticket_base.str_remarks_rfd,
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
                                ins_general_methods.convert_amount(ins_ticket_base.flt_adm_expect,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_adm_expect,
                                ins_ticket_base.str_refund_date,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_published_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_published_fare_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_market_fare_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_special_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_special_fare_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_total_tax_rfd,
                                ins_ticket_base.flt_std_commn_percentage_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_discount_given_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_discount_given_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_supplier_refund_charge,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_client_refund_charge,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_supplier_refund_net,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_client_refund_net,
                                ins_ticket_base.str_inv_document_number,
                                ins_ticket_base.str_rfd_document_number,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amt_as_per_bsp_issue,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_supplier_amt_as_per_bsp_issue,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amt_as_per_bsp_refund,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_supplier_amt_as_per_bsp_refund,
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
                                ins_ticket_base.str_ticketing_agent_code_rfd,
                                ins_ticket_base.str_ticketing_agent_numeric_code_rfd,
                                ins_ticket_base.str_ticket_booking_date,
                                ins_ticket_base.str_file_name_inv,
                                ins_ticket_base.str_file_name_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_market_fare_cash_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_market_fare_credit_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_market_fare_cash_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_market_fare_credit_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_total_tax_cash_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_total_tax_credit_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_total_tax_cash_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_total_tax_credit_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_credit,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_supplier_refund_charge_credit,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_cash,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_supplier_refund_charge_cash,
                                ins_ticket_base.str_base_currency,
                                ins_ticket_base.str_defult_currency_code,
                                ins_ticket_base.str_base_currency_rfd,
                                ins_ticket_base.str_tran_currency_rfd,
                                ins_ticket_base.flt_supplier_currency_roe,
                                ins_ticket_base.int_company_id,
                                2 , #fk_bint_created_user_id
                                str_current_date_time,
                                ins_ticket_base.str_destination,
                                ins_ticket_base.str_rm_opt1 ,
                                ins_ticket_base.str_rm_opt2 ,
                                ins_ticket_base.str_rm_opt3 ,
                                ins_ticket_base.str_rm_opt4 ,
                                ins_ticket_base.str_rm_opt5 ,
                                ins_ticket_base.str_rm_opt6 ,
                                ins_ticket_base.str_rm_opt7 ,
                                ins_ticket_base.str_rm_opt8 ,
                                ins_ticket_base.str_rm_opt9 ,
                                ins_ticket_base.str_rm_opt10 ,
                                ins_ticket_base.str_cust_cost_centre,
                                ins_ticket_base.int_number_of_segments_rfd,
                                ins_ticket_base.chr_system_refund_part_action,
                                ins_ticket_base.int_no_of_pax_rfd,
                                ins_ticket_base.int_no_of_pax_inv,
                                ins_ticket_base.str_employee_number,
                                ins_ticket_base.str_pax_name_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_standard_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_standard_commission_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_net_payable_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_net_payable_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_net_payable_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_net_payable_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_actual_cost_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_actual_cost_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_actual_cost_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_actual_cost_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_selling_price_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_selling_price_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_service_charge_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_service_charge_rfd,
                                ins_ticket_base.int_company_id_rfd,
                                ins_ticket_base.str_card_approval_code,
                                ins_ticket_base.int_card_payment_type,


                                ins_general_methods.convert_amount(ins_ticket_base.flt_debited_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_debited_amount_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_debited_amount_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_inv_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_inv_debited_amount_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_credited_amount_rfd,

                                ins_ticket_base.int_supplier_id_rfd ,

                                ins_general_methods.convert_amount(ins_ticket_base.flt_profit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_profit_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_profit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_profit_rfd ,
                                ins_ticket_base.str_connection_ticket ,

                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_supplier_amount_rfd ,
                                ins_ticket_base.int_profile_id ,
                                ins_ticket_base.str_agency_adv_receipt_no ,
                                
                                ins_ticket_base.int_pay_back_account_id_inv ,
                                ins_ticket_base.int_pay_back_account_id_rfd ,
                                ins_ticket_base.flt_pay_back_commission_percentage_inv ,
                                ins_ticket_base.flt_pay_back_commission_percentage_rfd ,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_pay_back_commission_inv ,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_pay_back_commission_rfd ,
                                
                                ins_ticket_base.int_discount_account_id_inv ,
                                ins_ticket_base.flt_discount_given_percentage_inv ,
                                ins_ticket_base.int_discount_account_id_rfd ,
                                ins_ticket_base.flt_discount_given_percentage_rfd ,
                                
                                ins_ticket_base.int_extra_earning_account_id_inv ,
                                ins_ticket_base.flt_extra_earninig_percentage_inv ,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_extra_earning_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_extra_earning_inv ,
                                
                                ins_ticket_base.int_extra_earning_account_id_rfd ,
                                ins_ticket_base.flt_extra_earninig_percentage_rfd ,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_extra_earning_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_extra_earning_rfd ,
                                
                                ins_ticket_base.int_counter_staff_id_inv,
                                ins_ticket_base.int_counter_staff_id_rfd ,
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_market_fare_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_market_fare_rfd ,
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_cc_charge_collected_ext,
                                ins_ticket_base.int_booking_agent_counter_staff_id ,
                                ins_ticket_base.int_product_master_id_inv ,
                                ins_ticket_base.int_product_master_id_rfd, 
                                
                                ins_ticket_base.int_credit_card_pos_id, # refer 20879
                                ins_ticket_base.str_cc_number ,# refer 20879
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_card_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_market_fare_card_amount_inv ,
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_uccf_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_market_fare_uccf_amount_inv ,
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_tax_card_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_tax_card_amount_inv ,
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_tax_uccf_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_tax_uccf_amount_inv , 
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_market_fare_card_amount_rfd , 
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_market_fare_uccf_amount_rfd  , 
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_tax_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_tax_card_amount_rfd ,	
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_tax_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_tax_uccf_amount_rfd ,	
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_sup_charge_card_amount_rfd , 
                                
                                ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_sup_charge_uccf_amount_rfd ,
                                
                                ins_ticket_base.int_card_payment_type_rfd,
                                ins_ticket_base.int_corporate_card_id_rfd,
                                
                                ins_ticket_base.str_crm_reference, # Refer 23236,
                                ins_ticket_base.flt_supplier_currency_roe_rfd,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_vat_in_inv,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.flt_vat_in_inv,
                                ins_general_methods.convert_amount(ins_ticket_base.flt_vat_in_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_ticket_base.flt_vat_in_rfd,
                                ins_ticket_base.dat_rm_lpo_date,
                                ins_ticket_base.flt_service_charge_percentage_inv,
                                ins_ticket_base.flt_service_charge_percentage_rfd,
                                ins_ticket_base.str_tax_details_rfd, #42728
                                ins_ticket_base.str_cust_currency,  #40225
                                ins_ticket_base.flt_cust_currency_roe,
                                ins_ticket_base.str_cust_currency_rfd,
                                ins_ticket_base.flt_cust_currency_roe_rfd,
                                ins_ticket_base.int_location_id_rfd,  #44032
                                ins_ticket_base.int_branch_id_rfd,
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_selling_price,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_debited_amount_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_service_charge,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_extra_earning_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),    #40899
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_pay_back_commission_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_discount_given_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                ins_ticket_base.int_cust_traveller_id, #45745
                                ins_ticket_base.int_distribution_type_inv, #46542
                                ins_ticket_base.int_distribution_type_rfd
                                
                                 ])

            
            if ins_ticket_base.str_cc_type_rfd.upper() == 'TP' or ins_ticket_base.str_cc_type_inv.upper() == 'TP'  \
                or ins_ticket_base.str_airplus_card_type.strip().upper() == 'AIRPLUS' :
                    int_exteranl_type = 4
            else :
                    int_exteranl_type = 0

            lst_extra_capturing_fields.append([
                                ins_ticket_base.int_ticket_id ,
                                ins_ticket_base.str_ticket_number ,
                                'T',
                                ins_ticket_base.int_cost_centre_id_ext ,
                                ins_ticket_base.int_department_id_ext ,
                                ins_ticket_base.str_agency_sales_man ,
                                ins_ticket_base.str_agency_ticketing_staff ,
                                ins_ticket_base.str_agency_traacs_user ,
                                ins_ticket_base.str_agency_adv_receipt_no ,
                                ins_ticket_base.str_agency_internal_remarks ,
                                ins_ticket_base.str_agency_product_code ,
                                ins_ticket_base.str_agency_sub_product_code ,
                                ins_ticket_base.str_agency_auto_invoice_yes_no ,

                                ins_ticket_base.str_party_file_job_card_no ,
                                ins_ticket_base.str_party_lpo_no ,
                                ins_ticket_base.int_party_maximum_tickets ,
                                ins_ticket_base.str_party_multiple_fop_yes_no ,
                                ins_ticket_base.str_cust_approver_name ,
                                ins_ticket_base.str_cust_approver_email ,
                                ins_ticket_base.str_cust_employee_no ,
                                ins_ticket_base.str_cust_employee_grade ,
                                ins_ticket_base.str_cust_cost_centre ,
                                ins_ticket_base.str_cust_department ,
                                ins_ticket_base.str_cust_accounting_unit ,
                                ins_ticket_base.str_cust_internal_ac_no ,
                                ins_ticket_base.str_cust_project_code ,
                                ins_ticket_base.str_cust_action_no ,
                                ins_ticket_base.str_cust_job_code ,
                                ins_ticket_base.str_cust_resource_code ,
                                ins_ticket_base.str_cust_commitment_no ,
                                ins_ticket_base.str_cust_purpose_of_travel ,
                                ins_ticket_base.str_cust_pax_mobile ,
                                ins_ticket_base.str_cust_pax_email ,
                                ins_ticket_base.str_cust_engagement_code ,
                                ins_ticket_base.flt_published_fare_ext ,
                                ins_ticket_base.flt_original_fare_ext ,
                                ins_ticket_base.flt_printing_fare_ext ,
                                ins_ticket_base.flt_service_fee_ext ,
                                ins_ticket_base.flt_extra_earning_inv ,
                                ins_ticket_base.flt_payback_commission_ext ,
                                ins_ticket_base.flt_cc_charge_collected_ext  ,
                                ins_ticket_base.flt_discount_given_ext ,
                                ins_ticket_base.flt_selling_price_ext ,
                                ins_ticket_base.flt_lowest_offered_ext ,
                                ins_ticket_base.str_reason_for_choose_higher_ext ,
                                ins_ticket_base.flt_fare_accepted_or_paid_ext ,
                                ins_ticket_base.flt_fare_lost_amount_ext ,
                                ins_ticket_base.str_rm_opt1 ,
                                ins_ticket_base.str_rm_opt2 ,
                                ins_ticket_base.str_rm_opt3 ,
                                ins_ticket_base.str_rm_opt4 ,
                                ins_ticket_base.str_rm_opt5 ,
                                ins_ticket_base.str_rm_opt6 ,
                                ins_ticket_base.str_rm_opt7 ,
                                ins_ticket_base.str_rm_opt8 ,
                                ins_ticket_base.str_rm_opt9 ,
                                ins_ticket_base.str_rm_opt10 ,
                                ins_ticket_base.str_rm_opt11 , #40299
                                ins_ticket_base.str_rm_opt12 ,
                                ins_ticket_base.str_rm_opt13 ,
                                ins_ticket_base.str_rm_opt14 ,
                                ins_ticket_base.str_rm_opt15 ,
                                ins_ticket_base.str_rm_opt16 ,
                                ins_ticket_base.str_rm_opt17 ,
                                ins_ticket_base.str_rm_opt18 ,
                                ins_ticket_base.str_rm_opt19 ,
                                ins_ticket_base.str_rm_opt20 ,
                                ins_ticket_base.str_against_doc_ext ,
                                ins_ticket_base.str_corp_card_code_ext ,
                                ins_ticket_base.str_compliance_ext ,
                                ins_ticket_base.int_account_master_id ,
                                ins_ticket_base.str_pnr_type_ext ,
                                int_exteranl_type ,
                                ins_ticket_base.int_rm_account_id_1 ,
                                ins_ticket_base.int_rm_account_id_2 ,
                                ins_ticket_base.int_rm_account_id_3 ,
                                ins_ticket_base.flt_rm_amount_1 ,
                                ins_ticket_base.flt_rm_amount_2 ,
                                ins_ticket_base.flt_rm_amount_3 ,
                                ins_ticket_base.str_quot_option_1,
                                ins_ticket_base.str_quot_option_2,
                                ins_ticket_base.str_master_reference,
                                ins_ticket_base.str_master_narration,
                                ins_ticket_base.str_rm_field_data,
                                ins_ticket_base.str_airline_pnr_number,
                                ins_ticket_base.str_passport_no,#38905
                                ins_ticket_base.str_nationality,
                                ins_ticket_base.str_cc_number_ext,
                                ins_ticket_base.str_cc_type_inv,
                                ins_ticket_base.str_co2_emission,   #41852
                                ins_ticket_base.json_user_defined_remark
                                

            ]
            )


            #40225
            ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.str_cust_currency = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,\
                   ins_ticket_base.str_ticket_issue_date or ins_ticket_base.str_refund_date ,ins_ticket_base.str_base_currency)
                   
            if not ins_ticket_base.str_refund_date :
                #// Appending into list for inserting data into the ticket sale table
                lst_ticket_sale_details.append([
                                                ins_ticket_base.int_ticket_id,
                                                ins_ticket_base.str_ticket_number,
                                                ins_ticket_base.str_original_issue,
                                                ins_ticket_base.str_last_conjection_ticket_number,
                                                False,
                                                ins_ticket_base.chr_ticket_status,
                                                ins_ticket_base.chr_system_sale_part_action,
                                                ins_ticket_base.chr_ticket_category,
                                                ins_ticket_base.str_crs_company,
                                                ins_ticket_base.str_file_name_inv,
                                                ins_ticket_base.str_pnr_no,
                                                ins_ticket_base.str_ticket_booking_date,
                                                ins_ticket_base.str_ticket_issue_date,
                                                ins_ticket_base.str_booking_agent_code,
                                                ins_ticket_base.str_booking_agent_numeric_code,
                                                ins_ticket_base.str_ticketing_agent_code,
                                                ins_ticket_base.str_ticketing_agent_numeric_code,
                                                ins_ticket_base.str_booking_agency_iata_no,
                                                ins_ticket_base.str_ticketing_agency_iata_no,
                                                ins_ticket_base.str_booking_agency_office_id,
                                                ins_ticket_base.str_pnr_first_owner_office_id,
                                                ins_ticket_base.str_pnr_current_owner_office_id,
                                                ins_ticket_base.str_ticketing_agency_office_id,
                                                ins_ticket_base.int_dc_card,
                                                ins_ticket_base.str_travel_date,
                                                ins_ticket_base.str_return_date,
                                                ins_ticket_base.int_ticket_type_id,
                                                ins_ticket_base.int_airline_id,
                                                ins_ticket_base.int_region_id,
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
                                                ins_ticket_base.str_lpo_number,
                                                ins_ticket_base.str_cust_cost_centre,
                                                ins_ticket_base.str_employee_number,
                                                ins_ticket_base.int_company_id,
                                                ins_ticket_base.int_location_id,
                                                ins_ticket_base.int_branch_id,
                                                ins_ticket_base.int_account_master_id,
                                                ins_ticket_base.int_supplier_id,
                                                ins_ticket_base.str_sector,
                                                ins_ticket_base.int_no_of_segments,
                                                ins_ticket_base.str_pax_name,
                                                ins_ticket_base.int_no_of_pax_inv,
                                                ins_ticket_base.str_tax_details,
                                                ins_ticket_base.str_remarks,
                                                'N',
                                                2,
                                                str_current_date_time,
                                                ins_ticket_base.int_airline_account_id,
                                                ins_general_methods.str_base_currency,
                                                ins_ticket_base.str_defult_currency_code,
                                                ins_ticket_base.str_destination,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_published_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_published_fare_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_market_fare_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_market_fare_cash_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_market_fare_credit_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_special_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_special_fare_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_total_tax_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_total_tax_cash_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_total_tax_credit_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_standard_commission,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_standard_commission,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amount,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_supplier_amount,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_service_charge,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_service_charge,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_discount_given_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_discount_given_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_selling_price,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_selling_price,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_net_payable_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_net_payable_inv,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_actual_cost_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_actual_cost_inv,
                                                ins_ticket_base.str_card_approval_code,

                                                ins_general_methods.convert_amount(ins_ticket_base.flt_debited_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_debited_amount_inv ,
                                                ins_ticket_base.str_pax_type ,

                                                ins_general_methods.convert_amount(ins_ticket_base.flt_profit_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_profit_inv ,
                                                ins_ticket_base.int_profile_id ,
                                                ins_ticket_base.str_agency_adv_receipt_no ,
                                                
                                                ins_ticket_base.int_pay_back_account_id_inv ,
                                                ins_ticket_base.flt_pay_back_commission_percentage_inv ,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_pay_back_commission_inv ,
                                                
                                                ins_ticket_base.int_discount_account_id_inv ,
                                                ins_ticket_base.flt_discount_given_percentage_inv ,
                                                ins_ticket_base.int_extra_earning_account_id_inv ,
                                                ins_ticket_base.flt_extra_earninig_percentage_inv ,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_extra_earning_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_extra_earning_inv ,
                                                ins_ticket_base.int_counter_staff_id_inv ,
                                                
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_market_fare_inv,
                                                ins_ticket_base.int_booking_agent_counter_staff_id ,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_cc_charge_collected_ext,
                                                ins_ticket_base.flt_supplier_currency_roe,

                                                ins_ticket_base.int_credit_card_pos_id, # refer 20879
                                                ins_ticket_base.str_cc_number, # refer 20879
                                                
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_card_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_market_fare_card_amount_inv ,

                                                ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_uccf_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_market_fare_uccf_amount_inv ,

                                                ins_general_methods.convert_amount(ins_ticket_base.flt_tax_card_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_tax_card_amount_inv ,

                                                ins_general_methods.convert_amount(ins_ticket_base.flt_tax_uccf_amount_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_tax_uccf_amount_inv,

                                                ins_ticket_base.str_crm_reference, # Refer 23236
                                                ins_ticket_base.int_card_payment_type,
                                                ins_general_methods.convert_amount(ins_ticket_base.flt_vat_in_inv,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.flt_vat_in_inv,
                                                ins_ticket_base.dat_rm_lpo_date,
                                                ins_ticket_base.flt_service_charge_percentage_inv,
                                                ins_ticket_base.str_cust_currency,  #40225
                                                ins_ticket_base.flt_cust_currency_roe,
                                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_selling_price,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_debited_amount_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_service_charge,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_extra_earning_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_pay_back_commission_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_discount_given_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                                ins_ticket_base.int_cust_traveller_id, #45745
                                                ins_ticket_base.int_distribution_type_inv  #46542

                                                ])
                #45552
                lst_tax_details.append([ins_ticket_base.flt_market_fare_inv,
                                     ins_ticket_base.flt_total_tax_inv,
                                     ins_ticket_base.flt_standard_commission,
                                     ins_ticket_base.flt_supplier_amount,
                                     ins_ticket_base.flt_vat_in_inv,
                                     0.0,#agency_charge
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),#airline_charge
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_service_charge,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_extra_earning_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_pay_back_commission_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_discount_given_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_selling_price,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),
                                     ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_debited_amount_inv,ins_ticket_base.flt_cust_currency_roe,ins_ticket_base.flt_supplier_currency_roe),#client_net
                                     0.0, #vat_out
                                     ins_ticket_base.str_ticket_issue_date,
                                     'T',
                                     ins_ticket_base.str_sector,
                                     ins_ticket_base.str_tax_details,
                                     ins_ticket_base.str_ticket_type,
                                     ins_ticket_base.int_supplier_id,
                                     ins_ticket_base.int_account_master_id,
                                     None, #country_id
                                     None, #ticket, hotel - service id
                                     ins_ticket_base.flt_cust_currency_roe,
                                     ins_ticket_base.flt_supplier_currency_roe,
                                     ins_ticket_base.chr_ticket_status, #issue,rfd
                                     ins_ticket_base.int_ticket_id,
                                     ins_ticket_base.str_ticket_number,
                                     ins_ticket_base.str_base_currency,
                                     ins_ticket_base.str_defult_currency_code,
                                     ins_ticket_base.str_cust_currency,
                                     False,
                                     False
                                            ])
#                int_ticket_sale_id = int_ticket_sale_id + 1                                
            else :

                lst_ticket_refund_details.append([

                                            ins_ticket_base.int_ticket_id,
                                            ins_ticket_base.str_ticket_number,
                                            ins_ticket_base.str_original_issue,
                                            ins_ticket_base.str_last_conjection_ticket_number,
                                            ins_ticket_base.chr_ticket_status,
                                            ins_ticket_base.chr_system_refund_part_action,
                                            ins_ticket_base.chr_ticket_category,
                                            ins_ticket_base.str_crs_company,
                                            ins_ticket_base.str_file_name_rfd,
                                            ins_ticket_base.str_pnr_no,
                                            ins_ticket_base.str_refund_date,
                                            ins_ticket_base.str_booking_agent_code,
                                            ins_ticket_base.str_booking_agent_numeric_code,
                                            ins_ticket_base.str_ticketing_agent_code,
                                            ins_ticket_base.str_ticketing_agent_numeric_code,
                                            ins_ticket_base.int_ticket_type_id,
                                            ins_ticket_base.int_airline_id,
                                            ins_ticket_base.int_airline_account_id,
                                            ins_ticket_base.int_region_id,
                                            ins_ticket_base.str_class,
                                            ins_ticket_base.int_class_id,
                                            ins_ticket_base.str_class_group,
                                            ins_ticket_base.str_return_class,
                                            ins_ticket_base.int_return_class_id,
                                            ins_ticket_base.str_return_class_group,
                                            ins_ticket_base.str_tour_code,
                                            ins_ticket_base.str_fare_basis,
                                            ins_ticket_base.str_lpo_number,
                                            ins_ticket_base.str_cust_cost_centre,
                                            ins_ticket_base.str_employee_number,
                                            ins_ticket_base.str_pax_type,
                                            ins_ticket_base.int_company_id_rfd,
                                            ins_ticket_base.int_location_id_rfd,  #44032
                                            ins_ticket_base.int_branch_id_rfd,
                                            ins_ticket_base.int_account_master_id,
                                            ins_ticket_base.int_supplier_id_rfd,
                                            ins_ticket_base.str_sector,
                                            ins_ticket_base.int_no_of_segments,
                                            ins_ticket_base.str_pax_name,
                                            ins_ticket_base.int_no_of_pax_rfd,
                                            ins_ticket_base.str_remarks_rfd,
                                            ins_general_methods.str_base_currency, #vchr_base_currency_rfd
                                            ins_ticket_base.str_defult_currency_code, #vchr_tran_currency_rfd
                                            ins_ticket_base.flt_supplier_currency_roe_rfd,
                                            'N',
                                            2,
                                            str_current_date_time,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_published_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_published_fare_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_market_fare_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_market_fare_credit_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_market_fare_cash_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_special_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_special_fare_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_total_tax_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_total_tax_credit_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_total_tax_cash_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_supplier_amount_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_supplier_refund_charge,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_credit,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_supplier_refund_charge_credit,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_cash,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_supplier_refund_charge_cash,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_supplier_refund_net,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_selling_price_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_selling_price_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_client_refund_charge,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_client_refund_net,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_net_payable_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_net_payable_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_actual_cost_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_actual_cost_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_service_charge_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_service_charge_rfd,
                                            ins_ticket_base.str_card_approval_code,

                                            ins_general_methods.convert_amount(ins_ticket_base.flt_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_debited_amount_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_inv_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_inv_debited_amount_rfd,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_credited_amount_rfd ,

                                            ins_general_methods.convert_amount(ins_ticket_base.flt_profit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_profit_rfd ,
                                            ins_ticket_base.str_agency_adv_receipt_no ,
                                            
                                            ins_ticket_base.int_pay_back_account_id_rfd ,
                                            ins_ticket_base.flt_pay_back_commission_percentage_rfd ,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_pay_back_commission_rfd ,
                                            
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_discount_given_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_discount_given_rfd,
                                            ins_ticket_base.int_discount_account_id_rfd ,
                                            ins_ticket_base.flt_discount_given_percentage_rfd ,
                                            ins_ticket_base.int_extra_earning_account_id_rfd ,
                                            ins_ticket_base.flt_extra_earninig_percentage_rfd ,
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_extra_earning_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_extra_earning_rfd ,
                                            ins_ticket_base.int_counter_staff_id_rfd ,
                                            
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_market_fare_rfd,
                                            ins_ticket_base.int_booking_agent_counter_staff_id ,
                                            
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_market_fare_card_amount_rfd , 

                                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_market_fare_uccf_amount_rfd  , 

                                            ins_general_methods.convert_amount(ins_ticket_base.flt_tax_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_tax_card_amount_rfd ,	

                                            ins_general_methods.convert_amount(ins_ticket_base.flt_tax_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_tax_uccf_amount_rfd ,	

                                            ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_sup_charge_card_amount_rfd , 

                                            ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_sup_charge_uccf_amount_rfd,
                                            
                                            ins_ticket_base.int_card_payment_type_rfd,
                                            ins_ticket_base.int_corporate_card_id_rfd,
                                            ins_ticket_base.str_crm_reference, # Refer 23236
                                            ins_general_methods.convert_amount(ins_ticket_base.flt_vat_in_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.flt_vat_in_rfd,
                                            ins_ticket_base.flt_service_charge_percentage_rfd,
                                            ins_ticket_base.int_credit_card_pos_id,     #40837
                                            ins_ticket_base.str_cc_number,
                                            ins_ticket_base.str_tax_details_rfd,
                                            ins_ticket_base.str_cust_currency_rfd,  #40225
                                            ins_ticket_base.flt_cust_currency_roe_rfd,
                                            ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                            ins_ticket_base.int_distribution_type_rfd #46542
                                            
                                                    ])
                #45552
                lst_tax_details_rfd.append([ins_ticket_base.flt_market_fare_rfd,
                                         ins_ticket_base.flt_total_tax_rfd,
                                         ins_ticket_base.flt_standard_commission_rfd,
                                         ins_ticket_base.flt_supplier_amount_rfd,
                                         ins_ticket_base.flt_vat_in_rfd,
                                         0.0,#agency_charge
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),#airline_charge
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_service_charge_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_extra_earning_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_discount_given_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_selling_price_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                         ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                                         0.0, #vat_out
                                         ins_ticket_base.str_refund_date,
                                         'T',
                                         ins_ticket_base.str_sector,
                                         ins_ticket_base.str_tax_details_rfd,
                                         ins_ticket_base.str_ticket_type,
                                         ins_ticket_base.int_supplier_id_rfd,
                                         ins_ticket_base.int_account_master_id,
                                         None, #country_id
                                         None, #ticket, hotel - service id
                                         ins_ticket_base.flt_cust_currency_roe_rfd,
                                         ins_ticket_base.flt_supplier_currency_roe_rfd,
                                         'IR', #issue,rfd, #47520 - status to IR for refund captured
                                         ins_ticket_base.int_ticket_id,
                                         ins_ticket_base.str_ticket_number,
                                         ins_ticket_base.str_base_currency_rfd,
                                         ins_ticket_base.str_defult_currency_code,
                                         ins_ticket_base.str_cust_currency_rfd,
                                         False,
                                         False
                                                ])


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
                                            str_departure_time,
                                            str_arrival_time,
                                            bln_open_segment
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

                


        for lst_data in lst_ticket_details:
            lst_data[94] = ins_ticket_base.str_return_date
            lst_data[27] = ins_ticket_base.str_return_class
            lst_data[28] = ins_ticket_base.int_return_class_id
            lst_data[29] = ins_ticket_base.str_return_class_group
            
        for lst_data in lst_ticket_sale_details:
            lst_data[25] = ins_ticket_base.str_return_date
            lst_data[32] = ins_ticket_base.str_return_class
            lst_data[33] = ins_ticket_base.int_return_class_id
            lst_data[34] = ins_ticket_base.str_return_class_group


        # Calculating servive fee from Collection amount
        flt_individual_ticket_service_fee = 0
        if ins_ticket_base.flt_coll_amount  > 0 and not ins_ticket_base.str_refund_date:
            flt_total_service_fee = ins_ticket_base.flt_coll_amount - (flt_total_tickets_fare + flt_total_tickets_tax)
            flt_individual_ticket_service_fee = flt_total_service_fee / len(lst_ticket_capture_details)

            for lst_data in lst_ticket_details:
                if  flt_individual_ticket_service_fee:
                    if flt_individual_ticket_service_fee:
#                        lst_data[47] = flt_individual_ticket_service_fee
                        pass
        try:
            if lst_ticket_details:
                cr.executemany("""INSERT INTO tbl_ticket
                                    (   fk_bint_cost_center_id_inv,
                                        vchr_pnr_number,
                                        vchr_ticket_number,
                                        vchr_last_conjection_ticket_number,
                                        vchr_original_issue,
                                        vchr_customer_lpo_or_to_number,
                                        chr_ticket_status,
                                        chr_supplier_ticket_status,
                                        chr_system_sale_part_action,
                                        chr_ticket_category,
                                        fk_bint_corporate_card_id,
                                        bln_glpo,
                                        fk_bint_supplier_account_id_inv,
                                        dat_ticket_issue,
                                        fk_bint_ticket_type_id,
                                        vchr_booking_agent_code,
                                        vchr_booking_agent_numeric_code,
                                        vchr_ticketing_agent_code_inv,
                                        vchr_ticketing_agent_numeric_code_inv,
                                        fk_bint_airline_id,
                                        fk_bint_airline_account_id,
                                        fk_bint_region_id,
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
                                        vchr_remarks_rfd,
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
                                        dbl_base_currency_adm_expect_inv,
                                        dbl_tran_currency_adm_expect_inv,
                                        dat_refund,
                                        dbl_base_currency_published_fare_rfd,
                                        dbl_tran_currency_published_fare_rfd,
                                        dbl_base_currency_market_fare_rfd,
                                        dbl_tran_currency_market_fare_rfd,
                                        dbl_base_currency_special_fare_rfd,
                                        dbl_tran_currency_special_fare_rfd,
                                        dbl_base_currency_tax_rfd,
                                        dbl_tran_currency_tax_rfd,
                                        dbl_std_commission_percentage_rfd,
                                        dbl_base_currency_discount_given_rfd,
                                        dbl_tran_currency_discount_given_rfd,
                                        dbl_base_currency_supplier_charge_rfd,
                                        dbl_tran_currency_supplier_charge_rfd,
                                        dbl_base_currency_client_charge_rfd,
                                        dbl_tran_currency_client_charge_rfd,
                                        dbl_base_currency_supplier_net_rfd,
                                        dbl_tran_currency_supplier_net_rfd,
                                        dbl_base_currency_client_net_rfd,
                                        dbl_tran_currency_client_net_rfd,
                                        vchr_document_no_inv,
                                        vchr_document_no_rfd,
                                        dbl_base_currency_supplier_bsp_file_amount_inv,
                                        dbl_tran_currency_supplier_bsp_file_amount_inv,
                                        dbl_base_currency_supplier_bsp_file_amount_rfd,
                                        dbl_tran_currency_supplier_bsp_file_amount_rfd,
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
                                        vchr_ticketing_agent_code_rfd,
                                        vchr_ticketing_agent_numeric_code_rfd,
                                        dat_booking,
                                        vchr_gds_file_name_inv,
                                        vchr_gds_file_name_rfd,
                                        dbl_base_currency_market_fare_credit_inv,
                                        dbl_tran_currency_market_fare_credit_inv,
                                        dbl_base_currency_market_fare_credit_card_inv,
                                        dbl_tran_currency_market_fare_credit_card_inv,
                                        dbl_base_currency_market_fare_credit_rfd,
                                        dbl_tran_currency_market_fare_credit_rfd,
                                        dbl_base_currency_market_fare_credit_card_rfd,
                                        dbl_tran_currency_market_fare_credit_card_rfd,
                                        dbl_base_currency_tax_credit_inv,
                                        dbl_tran_currency_tax_credit_inv,
                                        dbl_base_currency_tax_credit_card_inv,
                                        dbl_tran_currency_tax_credit_card_inv,
                                        dbl_base_currency_tax_credit_rfd,
                                        dbl_tran_currency_tax_credit_rfd,
                                        dbl_base_currency_tax_credit_card_rfd,
                                        dbl_tran_currency_tax_credit_card_rfd,
                                        dbl_base_currency_sup_charge_credit_card_rfd,
                                        dbl_tran_currency_sup_charge_credit_card_rfd,
                                        dbl_base_currency_sup_charge_credit_rfd,
                                        dbl_tran_currency_sup_charge_credit_rfd,
                                        vchr_base_currency_inv,
                                        vchr_tran_currency_inv,
                                        vchr_base_currency_rfd,
                                        vchr_tran_currency_rfd,
                                        dbl_tran_currency_roe_inv,
                                        fk_bint_company_id_inv,
                                        fk_bint_created_user_id,
                                        tim_created,
                                        vchr_destination_airport,
                                        vchr_field_1 ,
                                        vchr_field_2 ,
                                        vchr_field_3 ,
                                        vchr_field_4 ,
                                        vchr_field_5 ,
                                        vchr_field_6 ,
                                        vchr_field_7 ,
                                        vchr_field_8 ,
                                        vchr_field_9 ,
                                        vchr_field_10,
                                        vchr_customer_cost_centre,
                                        int_no_of_segments_rfd,
                                        chr_system_refund_part_action,
                                        int_no_of_pax_rfd,
                                        int_no_of_pax_inv,
                                        vchr_customer_employee_number,
                                        vchr_pax_name_rfd ,
                                        dbl_base_currency_std_commission_amount_rfd,
                                        dbl_tran_currency_std_commission_amount_rfd,
                                        dbl_base_currency_net_payable_inv,
                                        dbl_tran_currency_net_payable_inv,
                                        dbl_base_currency_inv_net_payable_rfd,
                                        dbl_tran_currency_inv_net_payable_rfd,
                                        dbl_base_currency_actual_cost_inv,
                                        dbl_tran_currency_actual_cost_inv,
                                        dbl_base_currency_actual_cost_rfd,
                                        dbl_tran_currency_actual_cost_rfd,
                                        dbl_base_currency_selling_price_rfd,
                                        dbl_tran_currency_selling_price_rfd,
                                        dbl_base_currency_service_fee_rfd,
                                        dbl_tran_currency_service_fee_rfd,
                                        fk_bint_company_id_rfd,
                                        vchr_card_approval_code,
                                        int_card_payment_type,

                                        dbl_base_currency_debited_amount_inv,
                                        dbl_tran_currency_debited_amount_inv,
                                        dbl_base_currency_debited_amount_rfd,
                                        dbl_tran_currency_debited_amount_rfd,
                                        dbl_base_currency_inv_debited_amount_rfd,
                                        dbl_tran_currency_inv_debited_amount_rfd,
                                        dbl_base_currency_credited_amount_rfd,
                                        dbl_tran_currency_credited_amount_rfd,

                                        fk_bint_supplier_account_id_rfd ,

                                        dbl_base_currency_profit_inv ,
                                        dbl_tran_currency_profit_inv ,
                                        dbl_base_currency_profit_rfd ,
                                        dbl_tran_currency_profit_rfd ,

                                        vchr_connection_ticket_number ,

                                        dbl_base_currency_gross_payable_rfd,
                                        dbl_tran_currency_gross_payable_rfd ,
                                        fk_bint_profile_id ,
                                        vchr_advance_receipt_number ,
                                        
                                        fk_bint_payback_account_id_inv ,
                                        fk_bint_payback_account_id_rfd ,
                                        dbl_payback_service_percentage_inv ,
                                        dbl_payback_service_percentage_rfd ,
                                        dbl_base_currency_payback_service_fee_inv ,
                                        dbl_tran_currency_payback_service_fee_inv ,
                                        dbl_base_currency_payback_service_fee_rfd ,
                                        dbl_tran_currency_payback_service_fee_rfd ,
                                        
                                        
                                        fk_bint_discount_account_id_inv ,
                                        dbl_discount_given_percentage_inv ,
                                        fk_bint_discount_account_id_rfd ,
                                        dbl_discount_given_percentage_rfd ,
                                        
                                        fk_bint_extra_earning_account_id_inv ,
                                        dbl_extra_earning_percentage_inv ,
                                        dbl_base_currency_extra_earning_inv ,
                                        dbl_tran_currency_extra_earning_inv ,
                                        
                                        fk_bint_extra_earning_account_id_rfd ,
                                        dbl_extra_earning_percentage_rfd ,
                                        dbl_base_currency_extra_earning_rfd ,
                                        dbl_trancurrency_extra_earning_rfd ,
                                        fk_bint_counter_staff_id_inv ,
                                        fk_bint_counter_staff_id_rfd ,
                                        
                                        dbl_base_currency_printing_fare_inv ,
                                        dbl_tran_currency_printing_fare_inv ,
                                        dbl_base_currency_printing_fare_rfd ,
                                        dbl_tran_currency_printing_fare_rfd ,
                                        
                                        dbl_base_currency_cc_charge_collected_inv ,
                                        dbl_tran_currency_cc_charge_collected_inv,
                                        fk_bint_booking_agent_counter_staff_id,
                                        fk_bint_product_master_id_inv 	,
                                        fk_bint_product_master_id_rfd,
                                        
                                        fk_bint_credit_card_pos_id,
                                        vchr_cc_number ,
                                        
                                        dbl_base_currency_market_fare_card_amount_inv, 	
                                        dbl_tran_currency_market_fare_card_amount_inv,
                                        dbl_base_currency_market_fare_uccf_amount_inv, 
                                        dbl_tran_currency_market_fare_uccf_amount_inv, 	
                                        
                                        dbl_base_currency_tax_card_amount_inv, 	
                                        dbl_tran_currency_tax_card_amount_inv,
                                        dbl_base_currency_tax_uccf_amount_inv, 	
                                        dbl_tran_currency_tax_uccf_amount_inv, 	
                                        
                                        dbl_base_currency_market_fare_card_amount_rfd, 	
                                        dbl_tran_currency_market_fare_card_amount_rfd,
                                        dbl_base_currency_market_fare_uccf_amount_rfd, 	
                                        dbl_tran_currency_market_fare_uccf_amount_rfd, 	
                                        
                                        dbl_base_currency_tax_card_amount_rfd, 	
                                        dbl_tran_currency_tax_card_amount_rfd,
                                        dbl_base_currency_tax_uccf_amount_rfd, 	
                                        dbl_tran_currency_tax_uccf_amount_rfd,
                                        
                                        dbl_base_currency_sup_charge_card_amount_rfd, 	
                                        dbl_tran_currency_sup_charge_card_amount_rfd,
                                        dbl_base_currency_sup_charge_uccf_amount_rfd, 
                                        dbl_tran_currency_sup_charge_uccf_amount_rfd,
                                        int_card_payment_type_rfd,
                                        fk_bint_corporate_card_id_rfd,
                                        vchr_crm_reference,
                                        dbl_tran_currency_roe_rfd,
                                        dbl_base_currency_vat_in_inv,
                                        dbl_tran_currency_vat_in_inv,
                                        dbl_base_currency_vat_in_rfd,
                                        dbl_tran_currency_vat_in_rfd,
                                        dat_customer_lpo_date,
                                        dbl_service_fee_percentage_inv,
                                        dbl_service_fee_percentage_rfd,
                                        vchr_tax_details_rfd,
                                        vchr_cust_currency_inv,
                                        dbl_cust_currency_roe_inv,
                                        vchr_cust_currency_rfd,
                                        dbl_cust_currency_roe_rfd,
                                        fk_bint_cost_center_id_rfd,
                                        fk_bint_department_id_rfd,
                                        dbl_cust_currency_selling_price_inv,
                                        dbl_cust_currency_debited_amount_inv,
                                        dbl_cust_currency_client_net_rfd,
                                        dbl_cust_currency_credited_amount_rfd,
                                        dbl_cust_currency_service_fee_inv,
                                        dbl_cust_currency_extra_earning_inv,
                                        dbl_cust_currency_payback_service_fee_inv,
                                        dbl_cust_currency_cc_charge_collected_inv,
                                        dbl_cust_currency_discount_given_inv,
                                        fk_bint_passenger_profile_id,
                                        int_distribution_type_inv,
                                        int_distribution_type_rfd
                                       
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
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s)""" , lst_ticket_details)

                
                dct_tkts = ins_general_methods.get_ticket_table_id(lst_tkt_no)
                
                if ins_general_methods.ins_global.lst_process_list :
                    for ins_ticket_base1 in ins_general_methods.ins_global.lst_process_list : #38619
                        if ins_ticket_base1.str_ticket_number in dct_tkts :
                            ins_ticket_base1.int_ticket_id = dct_tkts[ins_ticket_base1.str_ticket_number]
                        else :
                            raise Exception("Unable to update non iata list..")
                        
                if lst_ticket_sale_details :
                    for lst_data_tmp in lst_ticket_sale_details :
                        if lst_data_tmp[1] in dct_tkts :
                            lst_data_tmp[0] = dct_tkts[lst_data_tmp[1]]
                        else :
                            raise Exception("Unable to save sales..")

                    cr.executemany("""
                        INSERT INTO tbl_ticket_sales
                        (
                            fk_bint_ticket_id,
                            vchr_ticket_number,
                            vchr_original_issue,
                            vchr_last_conjection_ticket_number,
                            bln_conjuction_ticket,
                            chr_ticket_status,
                            chr_system_sale_part_action,
                            chr_ticket_category,
                            vchr_gds_company,
                            vchr_gds_file_name_inv,
                            vchr_pnr_number,
                            dat_booking,
                            dat_ticket_issue,
                            vchr_booking_agent_code,
                            vchr_booking_agent_numeric_code,
                            vchr_ticketing_agent_code_inv,
                            vchr_ticketing_agent_numeric_code_inv,
                            vchr_booking_agency_iata_no,
                            vchr_ticketing_agency_iata_no,
                            vchr_booking_agency_office_id,
                            vchr_pnr_first_owner_office_id,
                            vchr_pnr_current_owner_office_id,
                            vchr_ticketing_agency_office_id,
                            fk_bint_corporate_card_id,
                            dat_travel_date,
                            dat_of_return,
                            fk_bint_ticket_type_id,
                            fk_bint_airline_id,
                            fk_bint_region_id,
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
                            vchr_customer_lpo_or_to_number,
                            vchr_customer_cost_centre,
                            vchr_customer_employee_number,
                            fk_bint_company_id_inv,
                            fk_bint_cost_center_id_inv,
                            fk_bint_department_id_inv,
                            fk_bint_customer_account_id_inv,
                            fk_bint_supplier_account_id_inv,
                            vchr_sector_inv,
                            int_no_of_segments_inv,
                            vchr_pax_name_inv,
                            int_no_of_pax_inv,
                            vchr_tax_details_inv,
                            vchr_remarks_inv,
                            chr_document_status,
                            fk_bint_created_user_id,
                            tim_created,
                            fk_bint_airline_account_id,
                            vchr_base_currency_inv,
                            vchr_tran_currency_inv,
                            vchr_destination_airport,
                            dbl_base_currency_published_fare_inv ,
                            dbl_tran_currency_published_fare_inv,
                            dbl_base_currency_market_fare_inv ,
                            dbl_tran_currency_market_fare_inv ,
                            dbl_base_currency_market_fare_credit_inv,
                            dbl_tran_currency_market_fare_credit_inv,
                            dbl_base_currency_market_fare_credit_card_inv,
                            dbl_tran_currency_market_fare_credit_card_inv,
                            dbl_base_currency_special_fare_inv,
                            dbl_tran_currency_special_fare_inv,
                            dbl_base_currency_tax_inv,
                            dbl_tran_currency_tax_inv,
                            dbl_base_currency_tax_credit_inv,
                            dbl_tran_currency_tax_credit_inv,
                            dbl_base_currency_tax_credit_card_inv,
                            dbl_tran_currency_tax_credit_card_inv,
                            dbl_base_currency_std_commission_amount_inv,
                            dbl_tran_currency_std_commission_amount_inv,
                            dbl_base_currency_gross_payable_inv,
                            dbl_tran_currency_gross_payable_inv,
                            dbl_base_currency_service_fee_inv,
                            dbl_tran_currency_service_fee_inv,
                            dbl_base_currency_discount_given_inv,
                            dbl_tran_currency_discount_given_inv,
                            dbl_base_currency_selling_price_inv,
                            dbl_tran_currency_selling_price_inv,
                            dbl_base_currency_net_payable_inv,
                            dbl_tran_currency_net_payable_inv,
                            dbl_base_currency_actual_cost_inv,
                            dbl_tran_currency_actual_cost_inv,
                            vchr_card_approval_code,

                            dbl_base_currency_debited_amount_inv,
                            dbl_tran_currency_debited_amount_inv,
                            vchr_pax_type ,

                            dbl_base_currency_profit_inv ,
                            dbl_tran_currency_profit_inv ,
                            fk_bint_profile_id ,
                            vchr_advance_receipt_number ,
                            fk_bint_payback_account_id_inv ,
                            dbl_payback_service_percentage_inv ,
                            dbl_base_currency_payback_service_fee_inv ,
                            dbl_tran_currency_payback_service_fee_inv ,
                            
                            fk_bint_discount_account_id_inv ,
                            dbl_discount_given_percentage_inv ,

                            fk_bint_extra_earning_account_id_inv ,
                            dbl_extra_earning_percentage_inv ,
                            dbl_base_currency_extra_earning_inv ,
                            dbl_tran_currency_extra_earning_inv ,
                            fk_bint_counter_staff_id ,
                            dbl_base_currency_printing_fare_inv ,
                            dbl_tran_currency_printing_fare_inv ,
                            fk_bint_booking_agent_counter_staff_id,
                            dbl_base_currency_cc_charge_collected ,
                            dbl_tran_currency_cc_charge_collected ,
                            dbl_tran_currency_roe_inv,
                            
                            fk_bint_credit_card_pos_id,
                            vchr_cc_number ,
                            
                            dbl_base_currency_market_fare_card_amount_inv, 	
                            dbl_tran_currency_market_fare_card_amount_inv,
                            dbl_base_currency_market_fare_uccf_amount_inv, 
                            dbl_tran_currency_market_fare_uccf_amount_inv, 	

                            dbl_base_currency_tax_card_amount_inv, 	
                            dbl_tran_currency_tax_card_amount_inv,
                            dbl_base_currency_tax_uccf_amount_inv, 	
                            dbl_tran_currency_tax_uccf_amount_inv,
                            
                            vchr_crm_reference,
                            int_card_payment_type,
                            dbl_base_currency_vat_in_inv,
                            dbl_tran_currency_vat_in_inv,
                            dat_customer_lpo_date,
                            dbl_service_fee_percentage_inv,
                            vchr_cust_currency_inv,
                            dbl_cust_currency_roe_inv,
                            dbl_cust_currency_selling_price_inv,
                            dbl_cust_currency_debited_amount_inv,
                            dbl_cust_currency_service_fee_inv,
                            
                            dbl_cust_currency_extra_earning_inv,
                            dbl_cust_currency_payback_service_fee_inv,
                            dbl_cust_currency_cc_charge_collected_inv,
                            dbl_cust_currency_discount_given_inv,
                            fk_bint_passenger_profile_id,
                            int_distribution_type_inv
                            
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",lst_ticket_sale_details)                            
                                    
                if lst_ticket_refund_details :
                    
                    for lst_data_tmp in lst_ticket_refund_details :
                        if lst_data_tmp[1] in dct_tkts :
                            lst_data_tmp[0] = dct_tkts[lst_data_tmp[1]]
                        else :
                            raise Exception("Unable to save refund..")
                        
                    cr.executemany(""" INSERT INTO tbl_ticket_refund
                            (
                            fk_bint_ticket_id,
                            vchr_ticket_number,
                            vchr_original_issue,
                            vchr_last_conjection_ticket_number,
                            chr_ticket_status,
                            chr_system_refund_part_action,
                            chr_ticket_category,
                            vchr_gds_company,
                            vchr_gds_file_name_rfd,
                            vchr_pnr_number,
                            dat_refund,
                            vchr_booking_agent_code,
                            vchr_booking_agent_numeric_code,
                            vchr_ticketing_agent_code_rfd,
                            vchr_ticketing_agent_numeric_code_rfd,
                            fk_bint_ticket_type_id,
                            fk_bint_airline_id,
                            fk_bint_airline_account_id,
                            fk_bint_region_id,
                            vchr_class_chr,
                            fk_bint_booking_class_id,
                            vchr_class,
                            vchr_return_class_chr,
                            fk_bint_return_booking_class_id,
                            vchr_return_class,
                            vchr_tour_code,
                            vchr_fare_basis,
                            vchr_customer_lpo_or_to_number,
                            vchr_customer_cost_centre,
                            vchr_customer_employee_number,
                            vchr_pax_type,
                            fk_bint_company_id_rfd,
                            fk_bint_cost_center_id_rfd,
                            fk_bint_department_id_rfd,
                            fk_bint_customer_account_id_rfd,
                            fk_bint_supplier_account_id_rfd,
                            vchr_sector_rfd,
                            int_no_of_segments_rfd,
                            vchr_pax_name_rfd,
                            int_no_of_pax_rfd,
                            vchr_remarks_rfd,
                            vchr_base_currency_rfd,
                            vchr_tran_currency_rfd,
                            dbl_tran_currency_roe_rfd,
                            chr_document_status,
                            fk_bint_created_user_id,
                            tim_created ,
                            dbl_base_currency_published_fare_rfd,
                            dbl_tran_currency_published_fare_rfd,
                            dbl_base_currency_market_fare_rfd,
                            dbl_tran_currency_market_fare_rfd,
                            dbl_base_currency_market_fare_credit_card_rfd,
                            dbl_tran_currency_market_fare_credit_card_rfd,
                            dbl_base_currency_market_fare_credit_rfd,
                            dbl_tran_currency_market_fare_credit_rfd,
                            dbl_base_currency_special_fare_rfd,
                            dbl_tran_currency_special_fare_rfd,
                            dbl_base_currency_tax_rfd,
                            dbl_tran_currency_tax_rfd,
                            dbl_base_currency_tax_credit_card_rfd,
                            dbl_tran_currency_tax_credit_card_rfd,
                            dbl_base_currency_tax_credit_rfd,
                            dbl_tran_currency_tax_credit_rfd,
                            dbl_base_currency_gross_payable_rfd,
                            dbl_tran_currency_gross_payable_rfd,
                            dbl_base_currency_supplier_charge_rfd,
                            dbl_tran_currency_supplier_charge_rfd,
                            dbl_base_currency_sup_charge_credit_card_rfd,
                            dbl_tran_currency_sup_charge_credit_card_rfd,
                            dbl_base_currency_sup_charge_credit_rfd,
                            dbl_tran_currency_sup_charge_credit_rfd,
                            dbl_base_currency_supplier_net_rfd,
                            dbl_tran_currency_supplier_net_rfd,
                            dbl_base_currency_selling_price_rfd,
                            dbl_tran_currency_selling_price_rfd,
                            dbl_base_currency_client_charge_rfd,
                            dbl_tran_currency_client_charge_rfd,
                            dbl_base_currency_client_net_rfd,
                            dbl_tran_currency_client_net_rfd,
                            dbl_base_currency_inv_net_payable_rfd,
                            dbl_tran_currency_inv_net_payable_rfd,
                            dbl_base_currency_actual_cost_rfd,
                            dbl_tran_currency_actual_cost_rfd,
                            dbl_base_currency_service_fee_rfd,
                            dbl_tran_currency_service_fee_rfd,
                            vchr_card_approval_code,

                            dbl_base_currency_debited_amount_rfd,
                            dbl_tran_currency_debited_amount_rfd,
                            dbl_base_currency_inv_debited_amount_rfd,
                            dbl_tran_currency_inv_debited_amount_rfd,
                            dbl_base_currency_credited_amount_rfd,
                            dbl_tran_currency_credited_amount_rfd ,

                            dbl_base_currency_profit_rfd ,
                            dbl_tran_currency_profit_rfd ,
                            vchr_advance_receipt_number_rfd ,
                            fk_bint_payback_account_id_rfd ,
                            dbl_payback_service_percentage_rfd ,
                            dbl_base_currency_payback_service_fee_rfd ,
                            dbl_tran_currency_payback_service_fee_rfd ,
                    
                            dbl_base_currency_discount_given_rfd,
                            dbl_tran_currency_discount_given_rfd,
                            fk_bint_discount_account_id_rfd ,
                            dbl_discount_given_percentage_rfd ,

                            fk_bint_extra_earning_account_id_rfd ,
                            dbl_extra_earning_percentage_rfd ,
                            dbl_base_currency_extra_earning_rfd ,
                            dbl_trancurrency_extra_earning_rfd ,
                            fk_bint_counter_staff_id ,
                            dbl_base_currency_printing_fare_rfd ,
                            dbl_tran_currency_printing_fare_rfd ,
                            fk_bint_booking_agent_counter_staff_id,
                            
                            dbl_base_currency_market_fare_card_amount_rfd, 	
                            dbl_tran_currency_market_fare_card_amount_rfd,
                            dbl_base_currency_market_fare_uccf_amount_rfd, 	
                            dbl_tran_currency_market_fare_uccf_amount_rfd, 	

                            dbl_base_currency_tax_card_amount_rfd, 	
                            dbl_tran_currency_tax_card_amount_rfd,
                            dbl_base_currency_tax_uccf_amount_rfd, 	
                            dbl_tran_currency_tax_uccf_amount_rfd,

                            dbl_base_currency_sup_charge_card_amount_rfd, 	
                            dbl_tran_currency_sup_charge_card_amount_rfd,
                            dbl_base_currency_sup_charge_uccf_amount_rfd, 
                            dbl_tran_currency_sup_charge_uccf_amount_rfd,
                            
                            int_card_payment_type,
                            fk_bint_corporate_card_id,
                            vchr_crm_reference,
                            dbl_base_currency_vat_in_rfd,
                            dbl_tran_currency_vat_in_rfd,
                            dbl_service_fee_percentage_rfd,
                            fk_bint_credit_card_pos_id,
                            vchr_cc_number,
                            vchr_tax_details_rfd,
                            vchr_cust_currency_rfd,
                            dbl_cust_currency_roe_rfd,
                            dbl_cust_currency_client_net_rfd,
                            dbl_cust_currency_credited_amount_rfd,
                            int_distribution_type_rfd
                            
                            )

                            VALUES

                            (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s)


                        """,lst_ticket_refund_details)

                if lst_extra_capturing_fields :
                    
                    for lst_data_tmp in lst_extra_capturing_fields :
                        if lst_data_tmp[1] in dct_tkts :
                            lst_data_tmp[0] = dct_tkts[lst_data_tmp[1]]
                        else :
                            raise Exception("Unable to save extra capturing..")
                        
                    cr.executemany("""
                                    INSERT INTO tbl_extra_capturing_data
                                        (
                                            fk_bint_supporting_document_id  ,
                                            vchr_supporting_document_no ,
                                            chr_supporting_document_type ,
                                            fk_bint_cost_center_id ,
                                            fk_bint_department_id ,
                                            vchr_agency_sales_man ,
                                            vchr_agency_ticketing_staff ,
                                            vchr_agency_traacs_user ,
                                            vchr_agency_adv_receipt_no ,
                                            vchr_agency_internal_remarks  ,
                                            vchr_agency_product_code ,
                                            vchr_agency_sub_product_code ,
                                            vchr_agency_auto_invoice_yes_no  ,

                                            vchr_party_file_job_card_no ,
                                            vchr_party_lpo_no ,
                                            int_party_maximum_tickets ,
                                            vchr_party_multiple_fop_yes_no ,
                                            vchr_cust_approver_name ,
                                            vchr_cust_approver_email ,
                                            vchr_cust_employee_no ,
                                            vchr_cust_employee_grade ,
                                            vchr_cust_cost_centre ,
                                            vchr_cust_department ,
                                            vchr_cust_accounting_unit ,
                                            vchr_cust_internal_ac_no ,
                                            vchr_cust_project_code ,
                                            vchr_cust_action_no ,
                                            vchr_cust_job_code ,
                                            vchr_cust_resource_code ,
                                            vchr_cust_commitment_no ,
                                            vchr_cust_purpose_of_travel ,
                                            vchr_cust_pax_mobile ,
                                            vchr_cust_pax_email ,
                                            vchr_cust_engagement_code ,
                                            dbl_published_fare ,
                                            dbl_original_fare ,
                                            dbl_printing_fare ,
                                            dbl_service_fee ,
                                            dbl_extra_earning ,
                                            dbl_payback_commission ,
                                            dbl_cc_charge_collected ,
                                            dbl_discount_given ,
                                            dbl_selling_price ,
                                            dbl_lowest_offered ,
                                            vchr_reason_for_choose_higher ,
                                            dbl_fare_accepted_or_paid ,
                                            dbl_fare_lost_amount ,
                                            vchr_field_1 ,
                                            vchr_field_2 ,
                                            vchr_field_3 ,
                                            vchr_field_4 ,
                                            vchr_field_5 ,
                                            vchr_field_6 ,
                                            vchr_field_7 ,
                                            vchr_field_8 ,
                                            vchr_field_9 ,
                                            vchr_field_10 ,
                                            vchr_field_11 ,
                                            vchr_field_12 ,
                                            vchr_field_13 ,
                                            vchr_field_14 ,
                                            vchr_field_15 ,
                                            vchr_field_16 ,
                                            vchr_field_17 ,
                                            vchr_field_18 ,
                                            vchr_field_19 ,
                                            vchr_field_20 ,
                                            vchr_against_doc_no ,
                                            vchr_corp_card_code ,
                                            vchr_compliance ,
                                            fk_bint_customer_account_id ,
                                            vchr_pnr_type ,
                                            int_external_type ,
                                            fk_bint_ar_account_id_1 ,
                                            fk_bint_ar_account_id_2 ,
                                            fk_bint_ar_account_id_3 ,
                                            dbl_ar_amount_1 ,
                                            dbl_ar_amount_2 ,
                                            dbl_ar_amount_3 ,
                                            vchr_quot_option_1,
                                            vchr_quot_option_2,
                                            vchr_master_reference,
                                            vchr_master_narration,
                                            vchr_rm_field,
                                            vchr_airline_pnr_number,
                                            vchr_passport_no,
                                            vchr_nationality,
                                            vchr_card_no,
                                            vchr_card_type,
                                            vchr_co2_emission,
                                            json_extra_userdefined

                                        )

                                        VALUES

                                        (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s

                                        )""",lst_extra_capturing_fields)
                
                    pass
                if lst_tax_details: #45552
                    self.save_tax_details(lst_tax_details)
                elif lst_tax_details_rfd:
                    self.save_tax_details(lst_tax_details_rfd)
                    
                if lst_credit_card_transaction_data :

                    self.save_credit_card_transaction_data(lst_credit_card_transaction_data)
                    
                for str_ticket_number in lst_tkt_no: 
                    if ins_general_methods.bln_enable_multiple_discount_details:#45196
                        ins_general_methods.save_discount_data_to_tbl_multi_discount_details(ins_ticket_base,'I', str_ticket_number)
                    
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
                                      bln_open_segment 
                                      )
                                    VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s)""" ,lst_setcor_data)


        #cr.execute("""SELECT setval('tbl_ticket_pk_bint_ticket_id_seq'::regclass, %s)""",(int_ticket_id,))
        except:
            cr.close()
            print (lst_ticket_details,'\n',lst_setcor_data,'\n',lst_extra_capturing_fields)
            raise
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
        lst_htl_tax_details = [] #45552
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
                    'I' ,
                    'I' ,
                    'P' ,
                    'P' ,
                    ins_service_base.str_crs_company ,
                    ins_service_base.str_file_name_inv ,
                    ins_service_base.str_pnr_no ,
                    ins_service_base.str_voucher_issue_date ,
                    ins_service_base.int_service_id ,
                    ins_service_base.int_vendor_id ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_city_id ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_counter_staff_id ,
                    ins_service_base.str_booking_details ,
                    ins_service_base.str_hotel_confirm_number ,
                    ins_service_base.str_supplier_confirm_number ,
                    ins_service_base.str_hotel_check_in_date ,
                    ins_service_base.str_hotel_check_in_time ,
                    ins_service_base.str_hotel_check_out_date ,
                    ins_service_base.str_hotel_check_out_time ,
                    ins_service_base.int_meals_plan_id ,
                    ins_service_base.int_corp_card_id ,
                    ins_service_base.int_card_payment_type ,
                    ins_service_base.str_customer_lpo_number ,
                    ins_service_base.str_customer_cost_centre ,
                    ins_service_base.str_customer_emp_no ,
                    ins_service_base.int_company_id ,
                    ins_service_base.int_company_id_rfd ,
                    ins_service_base.int_location_id ,
                    ins_service_base.int_location_id_rfd ,
                    ins_service_base.int_department_id ,
                    ins_service_base.int_department_id_rfd ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.int_supplier_id ,
                    ins_service_base.int_no_of_adults_inv ,
                    ins_service_base.int_no_of_child_inv ,
                    ins_service_base.int_no_of_guest_inv ,
                    ins_service_base.str_pax_name ,
                    ins_service_base.int_no_of_nights ,
                    ins_service_base.int_no_of_nights_rfd ,
                    ins_service_base.int_no_of_rooms ,
                    ins_service_base.int_no_of_room_nights ,
                    ins_service_base.str_remarks ,
                    ins_service_base.flt_service_fee_percentage ,
                    ins_service_base.str_base_currency ,
                    ins_service_base.str_base_currency_rfd ,
                    ins_service_base.str_tran_currency ,
                    ins_service_base.str_tran_currency_rfd ,
                    ins_service_base.flt_supplier_currency_roe ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_gross_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_gross_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_net_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_actual_cost_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_actual_cost_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_service_fee ,
                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_selling_price_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_debited_amount_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_profit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_profit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_discount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_discount ,
                    2,
                    str_tim_created,
                    ins_service_base.int_product_master_id_inv,
                    ins_service_base.int_product_master_id_rfd,
                    ins_service_base.int_room_type_id ,
                    
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_card_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_card_amount ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_uccf_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_uccf_amount ,
                    ins_general_methods.convert_amount(ins_service_base.flt_tax_card_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_tax_card_amount ,
                    ins_general_methods.convert_amount(ins_service_base.flt_tax_uccf_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_tax_uccf_amount,
                    ins_service_base.int_booking_staff_id,
                    ins_service_base.dat_rm_lpo_date,
                    
                    ins_service_base.str_cust_currency,
                    ins_service_base.flt_cust_currency_roe,
                    ins_service_base.str_cust_currency_rfd,
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    json.dumps(ins_service_base.json_check_in_check_out_details),
                    ins_service_base.int_cust_traveller_id #45745
                    
                    ))
        
        
        
        
        
        
        lst_hotel_vouchers = [tpl_data[0] for tpl_data in lst_hv]
        
        cr.executemany("""

            INSERT INTO tbl_hotel_voucher
            
            (
            vchr_voucher_number ,
            chr_voucher_status ,
            chr_capture_voucher_status ,
            chr_system_sale_part_action ,
            chr_voucher_category ,
            vchr_gds_company ,
            vchr_gds_file_name_inv ,
            vchr_pnr_number ,
            dat_voucher_issue ,
            fk_bint_service_id ,
            fk_bint_hotel_master_id ,
            fk_bint_country_id ,
            fk_bint_hotel_city_id ,
            fk_bint_origin_country_id ,
            fk_bint_counter_staff_id ,
            vchr_booking_details ,
            vchr_hotel_confirm_no ,
            vchr_supplier_confirm_no ,
            dat_check_in ,
            tim_check_in ,
            dat_check_out ,
            tim_check_out ,
            fk_bint_hotel_meals_plan_id ,
            fk_bint_corporate_card_id ,
            int_card_payment_type ,
            vchr_customer_lpo_or_to_number ,
            vchr_customer_cost_centre ,
            vchr_customer_employee_number ,
            fk_bint_company_id_inv ,
            fk_bint_company_id_rfd ,
            fk_bint_cost_center_id_inv ,
            fk_bint_cost_center_id_rfd ,
            fk_bint_department_id_inv ,
            fk_bint_department_id_rfd ,
            fk_bint_customer_account_id_inv ,
            fk_bint_supplier_account_id_inv ,
            int_no_of_adults_inv ,
            int_no_of_child_inv ,
            int_no_of_guests_inv ,
            vchr_guest_name_inv ,
            int_no_of_nights_inv ,
            int_no_of_nights_rfd ,
            int_no_of_rooms_inv ,
            int_no_of_room_nights_inv ,
            vchr_remarks_inv ,
            
            dbl_service_fee_percentage_inv ,
            vchr_base_currency_inv ,
            vchr_base_currency_rfd ,
            vchr_tran_currency_inv ,
            vchr_tran_currency_rfd ,
            dbl_tran_currency_roe_inv ,
            
            dbl_base_currency_fare_inv ,
            dbl_tran_currency_fare_inv ,
            dbl_base_currency_fare_credit_card_inv ,
            dbl_tran_currency_fare_credit_card_inv ,
            dbl_base_currency_fare_credit_inv ,
            dbl_tran_currency_fare_credit_inv ,
            
            dbl_base_currency_printing_fare_inv ,
            dbl_tran_currency_printing_fare_inv ,
            dbl_base_currency_tax_inv ,
            dbl_tran_currency_tax_inv ,
            dbl_base_currency_tax_credit_card_inv ,
            dbl_tran_currency_tax_credit_card_inv ,
            dbl_base_currency_tax_credit_inv ,
            dbl_tran_currency_tax_credit_inv ,
            
            dbl_base_currency_gross_payable_inv ,
            dbl_tran_currency_gross_payable_inv ,
            dbl_base_currency_net_payable_inv ,
            dbl_tran_currency_net_payable_inv ,
            dbl_base_currency_actual_cost_inv ,
            dbl_tran_currency_actual_cost_inv ,
            dbl_base_currency_service_fee_inv ,
            dbl_tran_currency_service_fee_inv ,
            dbl_base_currency_selling_price_inv ,
            dbl_tran_currency_selling_price_inv ,
            dbl_base_currency_debited_amount_inv ,
            dbl_tran_currency_debited_amount_inv ,
            dbl_base_currency_profit_inv ,
            dbl_tran_currency_profit_inv ,
            dbl_base_currency_discount_given_inv,
            dbl_tran_currency_discount_given_inv,
            fk_bint_created_id ,
            tim_created ,
            fk_bint_product_master_id_inv,
            fk_bint_product_master_id_rfd,
            fk_bint_hotel_room_type_id ,
            
            dbl_base_currency_market_fare_card_amount_inv,
            dbl_tran_currency_market_fare_card_amount_inv,
            dbl_base_currency_market_fare_uccf_amount_inv,
            dbl_tran_currency_market_fare_uccf_amount_inv,
            dbl_base_currency_tax_card_amount_inv,
            dbl_tran_currency_tax_card_amount_inv,
            dbl_base_currency_tax_uccf_amount_inv,
            dbl_tran_currency_tax_uccf_amount_inv,
            fk_bint_booking_staff_id_inv,
            dat_customer_lpo_date_inv,
            
            vchr_cust_currency_inv,
            dbl_cust_currency_roe_inv,
            vchr_cust_currency_rfd,
            dbl_cust_currency_selling_price_inv,
            dbl_cust_currency_debited_amount_inv,
            dbl_cust_currency_service_fee_inv,
            dbl_cust_currency_discount_given_inv,
            json_check_in_check_out_details,
            fk_bint_passenger_profile_id
            
            )
            VALUES
            (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s
                            
            )
            
            
            """,lst_hv)
           
        
        lst_hv_sales_data = []
        for ins_service_base in lst_ins_hv :
            ins_service_base.int_voucher_id = ins_general_methods.get_hotel_voucher_details(ins_service_base.str_voucher_number)

            lst_hv_sales_data.append((
                    ins_service_base.str_voucher_number ,
                    'I' ,
                    'P' ,
                    'P' ,
                    ins_service_base.str_crs_company ,
                    ins_service_base.str_file_name_inv ,
                    ins_service_base.str_pnr_no ,
                    ins_service_base.str_voucher_issue_date ,
                    ins_service_base.int_service_id ,
                    ins_service_base.int_vendor_id ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_city_id ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_counter_staff_id ,
                    ins_service_base.str_booking_details ,
                    ins_service_base.str_hotel_confirm_number ,
                    ins_service_base.str_supplier_confirm_number ,
                    ins_service_base.str_hotel_check_in_date ,
                    ins_service_base.str_hotel_check_in_time ,
                    ins_service_base.str_hotel_check_out_date ,
                    ins_service_base.str_hotel_check_out_time ,
                    ins_service_base.int_meals_plan_id ,
                    ins_service_base.int_corp_card_id ,
                    ins_service_base.int_card_payment_type ,
                    ins_service_base.str_customer_lpo_number ,
                    ins_service_base.str_customer_cost_centre ,
                    ins_service_base.str_customer_emp_no ,
                    ins_service_base.int_company_id ,
                    ins_service_base.int_location_id ,
                    ins_service_base.int_department_id ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.int_supplier_id ,
                    ins_service_base.int_no_of_adults_inv ,
                    ins_service_base.int_no_of_child_inv ,
                    ins_service_base.int_no_of_guest_inv ,
                    ins_service_base.str_pax_name ,
                    ins_service_base.int_no_of_nights ,
                    ins_service_base.int_no_of_rooms ,
                    ins_service_base.int_no_of_room_nights ,
                    ins_service_base.str_remarks ,
                    ins_service_base.flt_service_fee_percentage ,
                    ins_service_base.str_base_currency ,
                    ins_service_base.str_tran_currency ,
                    ins_service_base.flt_supplier_currency_roe ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_gross_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_gross_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_net_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_actual_cost_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_actual_cost_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_service_fee ,
                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_selling_price_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_debited_amount_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_profit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_profit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_discount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_discount ,
                    2,
                    str_tim_created ,
                    ins_service_base.int_voucher_id,
                    ins_service_base.int_room_type_id,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_card_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_card_amount ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_uccf_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_uccf_amount ,
                    ins_general_methods.convert_amount(ins_service_base.flt_tax_card_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_tax_card_amount ,
                    ins_general_methods.convert_amount(ins_service_base.flt_tax_uccf_amount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_tax_uccf_amount,
                    ins_service_base.int_booking_staff_id,
                    ins_service_base.dat_rm_lpo_date,
                    ins_service_base.str_cust_currency,
                    ins_service_base.flt_cust_currency_roe,
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    json.dumps(ins_service_base.json_check_in_check_out_details),
                    ins_service_base.int_cust_traveller_id #45745
                    ))
            
        
        cr.executemany("""

            INSERT INTO tbl_hotel_voucher_sales
            
            (
            vchr_voucher_number ,
            chr_voucher_status ,
            chr_system_sale_part_action ,
            chr_voucher_category ,
            vchr_gds_company ,
            vchr_gds_file_name_inv ,
            vchr_pnr_number ,
            dat_voucher_issue ,
            fk_bint_service_id ,
            fk_bint_hotel_master_id ,
            fk_bint_country_id ,
            fk_bint_hotel_city_id ,
            fk_bint_origin_country_id ,
            fk_bint_counter_staff_id ,
            vchr_booking_details ,
            vchr_hotel_confirm_no ,
            vchr_supplier_confirm_no ,
            dat_check_in ,
            tim_check_in ,
            dat_check_out ,
            tim_check_out ,
            fk_bint_hotel_meals_plan_id ,
            fk_bint_corporate_card_id ,
            int_card_payment_type ,
            vchr_customer_lpo_or_to_number ,
            vchr_customer_cost_centre ,
            vchr_customer_employee_number ,
            fk_bint_company_id_inv ,
            fk_bint_cost_center_id_inv ,
            fk_bint_department_id_inv ,
            fk_bint_customer_account_id_inv ,
            fk_bint_supplier_account_id_inv ,
            int_no_of_adults_inv ,
            int_no_of_child_inv ,
            int_no_of_guests_inv ,
            vchr_guest_name_inv ,
            int_no_of_nights_inv ,
            int_no_of_rooms_inv ,
            int_no_of_room_nights_inv ,
            vchr_remarks_inv ,
            dbl_service_fee_percentage_inv ,
            vchr_base_currency_inv ,
            vchr_tran_currency_inv ,
            dbl_tran_currency_roe_inv ,
            dbl_base_currency_fare_inv ,
            dbl_tran_currency_fare_inv ,
            dbl_base_currency_fare_credit_card_inv ,
            dbl_tran_currency_fare_credit_card_inv ,
            dbl_base_currency_fare_credit_inv ,
            dbl_tran_currency_fare_credit_inv ,
            dbl_base_currency_printing_fare_inv ,
            dbl_tran_currency_printing_fare_inv ,
            dbl_base_currency_tax_inv ,
            dbl_tran_currency_tax_inv ,
            dbl_base_currency_tax_credit_card_inv ,
            dbl_tran_currency_tax_credit_card_inv ,
            dbl_base_currency_tax_credit_inv ,
            dbl_tran_currency_tax_credit_inv ,
            dbl_base_currency_gross_payable_inv ,
            dbl_tran_currency_gross_payable_inv ,
            dbl_base_currency_net_payable_inv ,
            dbl_tran_currency_net_payable_inv ,
            dbl_base_currency_actual_cost_inv ,
            dbl_tran_currency_actual_cost_inv ,
            dbl_base_currency_service_fee_inv ,
            dbl_tran_currency_service_fee_inv ,
            dbl_base_currency_selling_price_inv ,
            dbl_tran_currency_selling_price_inv ,
            dbl_base_currency_debited_amount_inv ,
            dbl_tran_currency_debited_amount_inv ,
            dbl_base_currency_profit_inv ,
            dbl_tran_currency_profit_inv ,
            dbl_base_currency_discount_given_inv,
            dbl_tran_currency_discount_given_inv,
            fk_bint_created_id ,
            tim_created ,
            fk_bint_hotel_voucher_id,
            fk_bint_hotel_room_type_id,
            
            dbl_base_currency_market_fare_card_amount_inv,
            dbl_tran_currency_market_fare_card_amount_inv,
            dbl_base_currency_market_fare_uccf_amount_inv,
            dbl_tran_currency_market_fare_uccf_amount_inv,
            dbl_base_currency_tax_card_amount_inv,
            dbl_tran_currency_tax_card_amount_inv,
            dbl_base_currency_tax_uccf_amount_inv,
            dbl_tran_currency_tax_uccf_amount_inv,
            fk_bint_booking_staff_id_inv,
            dat_customer_lpo_date_inv,
            
            vchr_cust_currency_inv,
            dbl_cust_currency_roe_inv,
            dbl_cust_currency_selling_price_inv,            
            dbl_cust_currency_debited_amount_inv,
            dbl_cust_currency_service_fee_inv,
            dbl_cust_currency_discount_given_inv,
            json_check_in_check_out_details,
            fk_bint_passenger_profile_id
            
            
            )
            VALUES
            (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s
            )
            """, lst_hv_sales_data)
            
        
            
            
        lst_hv_details_data = []
        for ins_service_base in lst_ins_hv :
          
            
            lst_hv_details_data.append((ins_service_base.str_voucher_number ,
                    ins_service_base.str_hotel_check_in_date ,
                    ins_service_base.str_hotel_check_out_date ,
                    ins_service_base.int_meals_plan_id ,
                    ins_service_base.int_no_of_adults_inv ,
                    ins_service_base.int_no_of_child_inv ,
                    ins_service_base.int_no_of_guest_inv ,
                    ins_service_base.str_pax_name ,
                    ins_service_base.int_no_of_nights ,
                    ins_service_base.int_no_of_rooms ,
                    ins_service_base.int_no_of_room_nights ,
                    ins_service_base.flt_service_fee_percentage ,
                    ins_service_base.str_base_currency ,
                    ins_service_base.str_tran_currency ,
                    ins_service_base.flt_supplier_currency_roe ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv/ins_service_base.int_no_of_nights,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv/ins_service_base.int_no_of_nights ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_inv/ins_service_base.int_no_of_nights,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_inv/ins_service_base.int_no_of_nights ,
                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_net_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_service_fee ,
                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_selling_price_inv ,
                    ins_service_base.int_voucher_id,
                    ins_service_base.int_room_type_id,
                    ins_general_methods.convert_amount(ins_service_base.flt_discount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_discount,
                    ins_service_base.int_country_id,
                    
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.int_cust_traveller_id #45745 
                    
                                        ))
            

        cr.executemany("""

            INSERT INTO tbl_hotel_voucher_details
            
            (
            vchr_voucher_number ,
            dat_from ,
            dat_to ,
            fk_bint_hotel_meals_plan_id ,
            int_no_of_adults_inv ,
            int_no_of_child_inv ,
            int_no_of_guests_inv ,
            vchr_guest_name ,
            int_no_of_nights ,
            int_no_of_rooms ,
            int_no_of_room_nights ,
            dbl_service_fee_percentage ,
            vchr_base_currency ,
            vchr_tran_currency ,
            dbl_tran_currency_roe ,
            dbl_base_currency_fare_per_night ,
            dbl_tran_currency_fare_per_night ,
            dbl_base_currency_tax_per_night ,
            dbl_tran_currency_tax_per_night ,
            dbl_base_currency_net_payable ,
            dbl_tran_currency_net_payable ,
            dbl_base_currency_service_fee ,
            dbl_tran_currency_service_fee ,
            dbl_base_currency_selling_price ,
            dbl_tran_currency_selling_price ,
            fk_bint_hotel_voucher_id ,
            fk_bint_hotel_room_type_id,
            dbl_base_currency_discount_given	,
            dbl_tran_currency_discount_given	,
            fk_bint_origin_country_id,
            
            dbl_cust_currency_service_fee,
            dbl_cust_currency_discount_given,
            fk_bint_passenger_profile_id
            
            
            )
            VALUES
            (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """, lst_hv_details_data)
            
            
        lst_hotel_sale_details_data = []
        
        for tpl_data in lst_hv_details_data :
            
            lst_temp_data = list(tpl_data)
            int_voucher_sale_id = ins_general_methods.get_hotel_voucher_sales_id(lst_temp_data[0])
            lst_temp_data.extend([ins_service_base.str_cust_currency,#37862
                    ins_service_base.flt_cust_currency_roe,
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    int_voucher_sale_id])
            lst_hotel_sale_details_data.append(tuple(lst_temp_data))
            
            pass
        
        cr.executemany("""

            INSERT INTO tbl_hotel_voucher_sales_details
            
            (
            vchr_voucher_number ,
            dat_from ,
            dat_to ,
            fk_bint_hotel_meals_plan_id ,
            int_no_of_adults_inv ,
            int_no_of_child_inv ,
            int_no_of_guests_inv ,
            vchr_guest_name ,
            int_no_of_nights ,
            int_no_of_rooms ,
            int_no_of_room_nights ,
            dbl_service_fee_percentage ,
            vchr_base_currency ,
            vchr_tran_currency ,
            dbl_tran_currency_roe ,
            dbl_base_currency_fare_per_night ,
            dbl_tran_currency_fare_per_night ,
            dbl_base_currency_tax_per_night ,
            dbl_tran_currency_tax_per_night ,
            dbl_base_currency_net_payable ,
            dbl_tran_currency_net_payable ,
            dbl_base_currency_service_fee ,
            dbl_tran_currency_service_fee ,
            dbl_base_currency_selling_price ,
            dbl_tran_currency_selling_price ,
            fk_bint_hotel_voucher_id ,
            fk_bint_hotel_room_type_id,
            dbl_base_currency_discount_given,
            dbl_tran_currency_discount_given,
            fk_bint_origin_country_id,
            dbl_cust_currency_service_fee,
            dbl_cust_currency_discount_given,
            fk_bint_passenger_profile_id,
            vchr_cust_currency_inv,
            dbl_cust_currency_roe_inv,
            dbl_cust_currency_selling_price_inv,
            fk_bint_hotel_voucher_sales_id
            
            
            )
            VALUES
            (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s
            )
            """, lst_hotel_sale_details_data)
            
        #refer #40299
        for ins_service_base in lst_ins_hv :
            lst_hotel_extra_capturing_fields.append((
                    ins_service_base.int_voucher_id,
                    ins_service_base.str_voucher_number ,
                    'H',
                    ins_service_base.int_location_id ,
                    ins_service_base.int_department_id, 
                    ins_service_base.str_agency_sales_man ,
                    ins_service_base.str_agency_ticketing_staff,
                    ins_service_base.str_agency_traacs_user,
                    ins_service_base.str_agency_adv_receipt_no,
                    ins_service_base.str_agency_internal_remarks,
                    ins_service_base.str_agency_product_code,
                    ins_service_base.str_agency_sub_product_code,
                    ins_service_base.str_agency_auto_invoice_yes_no,
                    ins_service_base.str_party_file_job_card_no,
                    ins_service_base.str_party_lpo_no,
                    ins_service_base.int_party_maximum_tickets,
                    ins_service_base.str_party_multiple_fop_yes_no,
                    ins_service_base.str_cust_approver_name,
                    ins_service_base.str_cust_approver_email,
                    ins_service_base.str_customer_emp_no,
                    ins_service_base.str_cust_employee_grade,
                    ins_service_base.str_customer_cost_centre,
                    ins_service_base.str_cust_department,
                    ins_service_base.str_cust_accounting_unit,
                    ins_service_base.str_cust_internal_ac_no,
                    ins_service_base.str_cust_project_code,
                    ins_service_base.str_cust_action_no,
                    ins_service_base.str_cust_job_code,
                    ins_service_base.str_cust_resource_code,
                    ins_service_base.str_cust_commitment_no,
                    ins_service_base.str_cust_purpose_of_travel,
                    ins_service_base.str_cust_pax_mobile,
                    ins_service_base.str_cust_pax_email,
                    ins_service_base.str_cust_engagement_code,
                    ins_service_base.str_rm_opt1,
                    ins_service_base.str_rm_opt2,
                    ins_service_base.str_rm_opt3,
                    ins_service_base.str_rm_opt4 ,
                    ins_service_base.str_rm_opt5,
                    ins_service_base.str_rm_opt6,
                    ins_service_base.str_rm_opt7,
                    ins_service_base.str_rm_opt8 ,
                    ins_service_base.str_rm_opt9,
                    ins_service_base.str_rm_opt10,
                    ins_service_base.str_rm_opt11, 
                    ins_service_base.str_rm_opt12 ,
                    ins_service_base.str_rm_opt13 ,
                    ins_service_base.str_rm_opt14 ,
                    ins_service_base.str_rm_opt15 ,
                    ins_service_base.str_rm_opt16,
                    ins_service_base.str_rm_opt17,
                    ins_service_base.str_rm_opt18,
                    ins_service_base.str_rm_opt19 ,
                    ins_service_base.str_rm_opt20 ,
                    ins_service_base.str_against_doc_ext ,
                    ins_service_base.str_corp_card_code_ext ,
                    ins_service_base.str_compliance_ext ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.str_pnr_type_ext ,
                    ins_service_base.str_quot_option_1 ,
                    ins_service_base.str_quot_option_2 ,
                    ins_service_base.str_master_reference , 
                    ins_service_base.str_master_narration ,
                    ins_service_base.str_rm_field_data,
                    ins_service_base.str_rate_type, #40299
                    ins_service_base.str_book_rate,
                    ins_service_base.str_cc_number_ext,#39948
                    ins_service_base.str_cc_type,
                    ins_service_base.str_hotel_chain_code,
                    ins_service_base.json_user_defined_remark
                    
            ))
            #45552
            lst_htl_tax_details.append([
                    ins_service_base.flt_fare_inv,
                    ins_service_base.flt_total_tax_inv,
                    0.0, #std_commission
                    ins_service_base.flt_fare_inv+ins_service_base.flt_total_tax_inv, #supplier_amount
                    0.0, #vat_in
                    0.0, #agency_charge
                    0.0, #airline_charge
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    0.0, #extra_earning
                    0.0, #payback_amount
                    0.0, #cc_charge
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe), #client_net
                    0.0, #vat_out
                    ins_service_base.str_voucher_issue_date,
                    'H',
                    '', #str_sector
                    '', #tax_details
                    '', #ticket_type
                    ins_service_base.int_supplier_id,
                    ins_service_base.int_customer_account_id_inv,
                    ins_service_base.int_country_id,
                    None ,
                    ins_service_base.flt_cust_currency_roe,
                    ins_service_base.flt_supplier_currency_roe,
                    'I', 
                    ins_service_base.int_voucher_id,
                    ins_service_base.str_voucher_number,
                    ins_service_base.str_base_currency,
                    ins_service_base.str_tran_currency,
                    ins_service_base.str_cust_currency,
                    False,
                    False
                    
            ])
            
        if lst_htl_tax_details:  #45552
            self.save_tax_details(lst_htl_tax_details)
            
        if lst_hotel_extra_capturing_fields:
            self.save_voucher_details_to_tbl_extra_capturing_data(lst_hotel_extra_capturing_fields)
               
        cr.close()
        return lst_hotel_vouchers
    
    def save_captured_car_voucher_data(self,lst_ins_cv): #40359
        cr = self.create_cursor()
        lst_cv = []
        lst_car_extra_capturing_fields = []
        lst_car_tax_details = []  #45552
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
                    'I' ,
                    'I' ,
                    'P' ,
                    'P' ,
                    ins_service_base.str_crs_company ,
                    ins_service_base.str_file_name_inv ,
                    ins_service_base.str_pnr_no ,
                    ins_service_base.str_voucher_issue_date ,
                    ins_service_base.int_service_id ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_counter_staff_id ,
                    ins_service_base.int_car_rental_company_id,
                    ins_service_base.str_booking_details ,
                    ins_service_base.str_supplier_confirm_number ,
                    ins_service_base.str_car_details,
                    ins_service_base.str_pick_up_date ,
                    ins_service_base.str_pick_up_time ,
                    ins_service_base.str_drop_off_date ,
                    ins_service_base.str_drop_off_time ,
                    ins_service_base.str_pick_up_location ,
                    ins_service_base.str_drop_off_location ,
                    ins_service_base.str_customer_lpo_number ,
                    ins_service_base.str_customer_cost_centre ,
                    ins_service_base.str_customer_emp_no ,
                    ins_service_base.int_corp_card_id ,
                    ins_service_base.int_card_payment_type ,
                    ins_service_base.str_pax_name ,
                    ins_service_base.int_company_id ,
                    ins_service_base.int_company_id ,
                    ins_service_base.int_location_id ,
                    ins_service_base.int_department_id ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.int_supplier_id ,
                    ins_service_base.int_no_of_days ,
                    ins_service_base.int_no_of_days_rfd ,
                    ins_service_base.int_no_of_car ,
                    ins_service_base.str_remarks ,
                    ins_service_base.dat_rm_lpo_date,
                    
                    
                    ins_service_base.str_base_currency ,
                    ins_service_base.str_base_currency ,
                    ins_service_base.str_tran_currency ,
                    ins_service_base.flt_supplier_currency_roe ,
                    ins_service_base.flt_service_fee_percentage ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_inv ,
                    
                    ins_general_methods.convert_amount(ins_service_base.flt_gross_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_gross_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_net_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_actual_cost_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_actual_cost_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_service_fee ,
                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_selling_price_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_debited_amount_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_profit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_profit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_discount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_discount ,
                    ins_service_base.str_cust_currency,
                    ins_service_base.flt_cust_currency_roe,
                    ins_service_base.str_cust_currency_rfd,
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.int_cust_traveller_id, #45745
                    
                    2,
                    str_tim_created
                    
                    
                    ))
        
        
        lst_car_vouchers = [tpl_data[0] for tpl_data in lst_cv]
        
        cr.executemany("""

            INSERT INTO tbl_car_voucher
            
            (
            vchr_voucher_number ,
            chr_voucher_status ,
            chr_capture_voucher_status ,
            chr_system_sale_part_action ,
            chr_voucher_category ,
            vchr_gds_company ,
            vchr_gds_file_name_inv ,
            vchr_pnr_number ,
            dat_voucher_issue ,
            fk_bint_service_id ,
            fk_bint_country_id ,
            fk_bint_counter_staff_id ,
            fk_bint_car_rental_company_id,
            vchr_booking_details ,
            vchr_supplier_confirm_no ,
            vchr_car_vehicle,
            dat_car_start ,
            tim_car_start ,
            dat_car_end ,
            tim_car_end ,
            vchr_car_renting_station,
            vchr_car_drop_station,
            vchr_customer_lpo_or_to_number,
            vchr_customer_cost_centre ,
            vchr_customer_employee_number ,
            fk_bint_corporate_card_id ,
            int_card_payment_type ,
            vchr_pax_name,
            fk_bint_company_id_inv,
            fk_bint_company_id_rfd,
            fk_bint_cost_center_id_inv,
            fk_bint_department_id_inv,
            fk_bint_customer_account_id_inv,
            fk_bint_supplier_account_id_inv,
            int_no_of_days_inv,
            int_no_of_days_rfd,
            int_no_of_cars_inv,
            vchr_remarks_inv,
            dat_customer_lpo_date_inv,
            
            vchr_base_currency_inv ,
            vchr_base_currency_rfd ,
            vchr_tran_currency_inv ,
            dbl_tran_currency_roe_inv ,
            dbl_service_fee_percentage_inv ,
            
            dbl_base_currency_fare_inv ,
            dbl_tran_currency_fare_inv ,
            dbl_base_currency_fare_credit_card_inv ,
            dbl_tran_currency_fare_credit_card_inv ,
            dbl_base_currency_fare_credit_inv ,
            dbl_tran_currency_fare_credit_inv ,
            
            dbl_base_currency_printing_fare_inv ,
            dbl_tran_currency_printing_fare_inv ,
            
            dbl_base_currency_tax_inv ,
            dbl_tran_currency_tax_inv ,
            dbl_base_currency_tax_credit_card_inv ,
            dbl_tran_currency_tax_credit_card_inv ,
            dbl_base_currency_tax_credit_inv ,
            dbl_tran_currency_tax_credit_inv ,
            
            dbl_base_currency_gross_payable_inv ,
            dbl_tran_currency_gross_payable_inv ,
            dbl_base_currency_net_payable_inv ,
            dbl_tran_currency_net_payable_inv ,
            dbl_base_currency_actual_cost_inv ,
            dbl_tran_currency_actual_cost_inv ,
            dbl_base_currency_service_fee_inv ,
            dbl_tran_currency_service_fee_inv ,
            dbl_base_currency_selling_price_inv ,
            dbl_tran_currency_selling_price_inv ,
            dbl_base_currency_debited_amount_inv ,
            dbl_tran_currency_debited_amount_inv ,
            dbl_base_currency_profit_inv ,
            dbl_tran_currency_profit_inv ,
            dbl_base_currency_discount_given_inv,
            dbl_tran_currency_discount_given_inv,
            vchr_cust_currency_inv,
            dbl_cust_currency_roe_inv,
            vchr_cust_currency_rfd,
            dbl_cust_currency_selling_price_inv,
            dbl_cust_currency_debited_amount_inv,
            dbl_cust_currency_service_fee_inv,
            dbl_cust_currency_discount_given_inv,
            fk_bint_passenger_profile_id,
            fk_bint_created_id ,
            tim_created 
            
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s
            )
            
            
            """,lst_cv)
           
        lst_cv_sales_data = []
        for ins_service_base in lst_ins_cv :
            ins_service_base.int_voucher_id = ins_general_methods.get_car_voucher_details(ins_service_base.str_voucher_number)

            lst_cv_sales_data.append((
                    ins_service_base.str_voucher_number ,
                    ins_service_base.int_voucher_id,
                    'I' ,
                    'P' ,
                    'P' ,
                    ins_service_base.str_crs_company ,
                    ins_service_base.str_file_name_inv ,
                    ins_service_base.str_pnr_no ,
                    ins_service_base.str_voucher_issue_date ,
                    ins_service_base.int_service_id ,
                    ins_service_base.int_country_id ,
                    ins_service_base.int_counter_staff_id ,
                    ins_service_base.int_car_rental_company_id,
                    
                    ins_service_base.str_booking_details ,
                    ins_service_base.str_supplier_confirm_number ,
                    ins_service_base.str_car_details,
                    ins_service_base.str_pick_up_date ,
                    ins_service_base.str_pick_up_time ,
                    ins_service_base.str_drop_off_date ,
                    ins_service_base.str_drop_off_time ,
                    ins_service_base.str_pick_up_location ,
                    ins_service_base.str_drop_off_location ,
                    ins_service_base.bln_with_pickup ,
                    ins_service_base.bln_with_dropup ,
                    ins_service_base.str_customer_lpo_number ,
                    ins_service_base.str_customer_cost_centre ,
                    ins_service_base.str_customer_emp_no ,
                    ins_service_base.int_corp_card_id ,
                    ins_service_base.int_card_payment_type ,
                    ins_service_base.str_pax_name ,
                    ins_service_base.int_company_id ,
                    ins_service_base.int_location_id ,
                    ins_service_base.int_department_id ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.int_supplier_id ,
                    ins_service_base.int_no_of_days ,
                    ins_service_base.int_no_of_car ,
                    ins_service_base.str_remarks ,
                    ins_service_base.dat_rm_lpo_date,
                    
                    ins_service_base.str_base_currency ,
                    ins_service_base.str_tran_currency ,
                    ins_service_base.flt_supplier_currency_roe ,
                    ins_service_base.flt_service_fee_percentage ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_credit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_card_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_credit_inv ,
                    
                    ins_general_methods.convert_amount(ins_service_base.flt_gross_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_gross_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_net_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_actual_cost_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_actual_cost_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_service_fee ,
                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_selling_price_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_debited_amount_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_profit_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_profit_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_discount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_discount ,
                    ins_service_base.str_cust_currency,
                    ins_service_base.flt_cust_currency_roe,
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.int_cust_traveller_id, #45745
                    2,
                    str_tim_created,
                    
                    
                    ))
            
        cr.executemany("""

            INSERT INTO tbl_car_voucher_sales
            
            (
            vchr_voucher_number ,
            fk_bint_car_voucher_id,
            chr_voucher_status ,
            chr_system_sale_part_action ,
            chr_voucher_category ,
            vchr_gds_company ,
            vchr_gds_file_name_inv ,
            vchr_pnr_number ,
            dat_voucher_issue ,
            fk_bint_service_id ,
            fk_bint_country_id,
            fk_bint_counter_staff_id ,
            fk_bint_car_rental_company_id,
            
            vchr_booking_details ,
            vchr_supplier_confirm_no ,
            vchr_car_vehicle,
            dat_car_start ,
            tim_car_start ,
            dat_car_end ,
            tim_car_end ,
            vchr_car_renting_station,
            vchr_car_drop_station,
            bln_with_pickup,
            bln_with_dropup,
            vchr_customer_lpo_or_to_number,
            vchr_customer_cost_centre ,
            vchr_customer_employee_number ,
            fk_bint_corporate_card_id ,
            int_card_payment_type ,
            vchr_pax_name,
            fk_bint_company_id_inv,
            fk_bint_cost_center_id_inv,
            fk_bint_department_id_inv,
            fk_bint_customer_account_id_inv,
            fk_bint_supplier_account_id_inv,
            int_no_of_days_inv,
            int_no_of_cars_inv,
            vchr_remarks_inv,
            dat_customer_lpo_date_inv,
            
            vchr_base_currency_inv ,
            vchr_tran_currency_inv ,
            dbl_tran_currency_roe_inv ,
            dbl_service_fee_percentage_inv ,
            
            dbl_base_currency_fare_inv ,
            dbl_tran_currency_fare_inv ,
            dbl_base_currency_fare_credit_card_inv ,
            dbl_tran_currency_fare_credit_card_inv ,
            dbl_base_currency_fare_credit_inv ,
            dbl_tran_currency_fare_credit_inv ,
            
            dbl_base_currency_printing_fare_inv ,
            dbl_tran_currency_printing_fare_inv ,
            
            dbl_base_currency_tax_inv ,
            dbl_tran_currency_tax_inv ,
            dbl_base_currency_tax_credit_card_inv ,
            dbl_tran_currency_tax_credit_card_inv ,
            dbl_base_currency_tax_credit_inv ,
            dbl_tran_currency_tax_credit_inv ,
            
            dbl_base_currency_gross_payable_inv ,
            dbl_tran_currency_gross_payable_inv ,
            dbl_base_currency_net_payable_inv ,
            dbl_tran_currency_net_payable_inv ,
            dbl_base_currency_actual_cost_inv ,
            dbl_tran_currency_actual_cost_inv ,
            dbl_base_currency_service_fee_inv ,
            dbl_tran_currency_service_fee_inv ,
            dbl_base_currency_selling_price_inv ,
            dbl_tran_currency_selling_price_inv ,
            dbl_base_currency_debited_amount_inv ,
            dbl_tran_currency_debited_amount_inv ,
            dbl_base_currency_profit_inv ,
            dbl_tran_currency_profit_inv ,
            dbl_base_currency_discount_given_inv,
            dbl_tran_currency_discount_given_inv,
            vchr_cust_currency_inv,
            dbl_cust_currency_roe_inv,
            dbl_cust_currency_selling_price_inv,
            dbl_cust_currency_debited_amount_inv,
            dbl_cust_currency_service_fee_inv,
            dbl_cust_currency_discount_given_inv,
            fk_bint_passenger_profile_id,
            fk_bint_created_id ,
            tim_created 
            
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """, lst_cv_sales_data)
            
        
            
        lst_cv_details_data = []
        for ins_service_base in lst_ins_cv :
          
            
            lst_cv_details_data.append((ins_service_base.str_voucher_number ,
                    ins_service_base.int_voucher_id,
                    ins_service_base.str_car_details,
                    ins_service_base.str_pick_up_date ,
                    ins_service_base.str_drop_off_date ,
                    ins_service_base.str_pick_up_location ,
                    ins_service_base.str_drop_off_location ,
                    ins_service_base.int_no_of_days ,
                    ins_service_base.int_no_of_car ,
                    ins_service_base.int_no_of_days*ins_service_base.int_no_of_car,
                    ins_service_base.str_pax_name ,
                    
                    ins_service_base.str_base_currency ,
                    ins_service_base.str_tran_currency ,
                    ins_service_base.flt_supplier_currency_roe ,
                    ins_service_base.flt_service_fee_percentage ,
                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_fare_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_total_tax_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_total_tax_inv ,
                    
                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_net_payable_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_service_fee ,
                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_selling_price_inv ,
                    ins_general_methods.convert_amount(ins_service_base.flt_discount,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.flt_discount ,
                    ins_service_base.str_cust_currency,
                    ins_service_base.flt_cust_currency_roe,
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_service_base.int_cust_traveller_id #45745
                    ))
            
        cr.executemany("""

            INSERT INTO tbl_car_voucher_details
            
            (
            vchr_voucher_number ,
            fk_bint_car_voucher_id,
            vchr_car_vehicle,
            dat_car_start ,
            dat_car_end ,
            vchr_car_renting_station,
            vchr_car_drop_station,
            bint_no_of_days,
            bint_no_of_cars,
            int_no_of_car_days,
            vchr_guest_name,
        
            vchr_base_currency ,
            vchr_tran_currency ,
            dbl_tran_currency_roe ,
            dbl_service_fee_percentage ,
            
            dbl_base_currency_fare_per_car ,
            dbl_tran_currency_fare_per_car ,
            dbl_base_currency_tax_per_car,
            dbl_tran_currency_tax_per_car,
            dbl_base_currency_net_payable ,
            dbl_tran_currency_net_payable ,
            dbl_base_currency_service_fee ,
            dbl_tran_currency_service_fee ,
            dbl_base_currency_selling_price ,
            dbl_tran_currency_selling_price ,
            dbl_base_currency_discount_given,
            dbl_tran_currency_discount_given,
            vchr_cust_currency,
            dbl_cust_currency_roe,
            dbl_cust_currency_selling_price,
            dbl_cust_currency_service_fee,
            dbl_cust_currency_discount_given,
            fk_bint_passenger_profile_id
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """, lst_cv_details_data)
            
        lst_car_sale_details_data = []
        
        for tpl_data in lst_cv_details_data :
            
            lst_temp_data = list(tpl_data)
            int_voucher_sale_id = ins_general_methods.get_car_voucher_sales_id(lst_temp_data[0])
            lst_temp_data.append(int_voucher_sale_id)
            lst_car_sale_details_data.append(tuple(lst_temp_data))
            
            pass
        
        cr.executemany("""

            INSERT INTO tbl_car_voucher_sales_details
            
            (
            vchr_voucher_number ,
            fk_bint_car_voucher_id,
            vchr_car_vehicle,
            dat_car_start ,
            dat_car_end ,
            vchr_car_renting_station,
            vchr_car_drop_station,
            int_no_of_days,
            int_no_of_cars,
            int_no_of_car_days,
            vchr_guest_name,
        
            vchr_base_currency ,
            vchr_tran_currency ,
            dbl_tran_currency_roe ,
            dbl_service_fee_percentage ,
            
            dbl_base_currency_fare_per_car ,
            dbl_tran_currency_fare_per_car ,
            dbl_base_currency_tax_per_car,
            dbl_tran_currency_tax_per_car,
            dbl_base_currency_net_payable ,
            dbl_tran_currency_net_payable ,
            dbl_base_currency_service_fee ,
            dbl_tran_currency_service_fee ,
            dbl_base_currency_selling_price ,
            dbl_tran_currency_selling_price ,
            dbl_base_currency_discount_given,
            dbl_tran_currency_discount_given,
            vchr_cust_currency_inv,
            dbl_cust_currency_roe_inv,
            dbl_cust_currency_selling_price_inv,
            dbl_cust_currency_service_fee,
            dbl_cust_currency_discount_given,
            fk_bint_passenger_profile_id,
            fk_bint_car_voucher_sales_id
            
            
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """, lst_car_sale_details_data)
           
        for ins_service_base in lst_ins_cv :
            lst_car_extra_capturing_fields.append((
                    ins_service_base.int_voucher_id,
                    ins_service_base.str_voucher_number ,
                    'C',
                    ins_service_base.int_location_id ,
                    ins_service_base.int_department_id, 
                    ins_service_base.str_agency_sales_man ,
                    ins_service_base.str_agency_ticketing_staff,
                    ins_service_base.str_agency_traacs_user,
                    ins_service_base.str_agency_adv_receipt_no,
                    ins_service_base.str_agency_internal_remarks,
                    ins_service_base.str_agency_product_code,
                    ins_service_base.str_agency_sub_product_code,
                    ins_service_base.str_agency_auto_invoice_yes_no,
                    ins_service_base.str_party_file_job_card_no,
                    ins_service_base.str_party_lpo_no,
                    ins_service_base.int_party_maximum_tickets,
                    ins_service_base.str_party_multiple_fop_yes_no,
                    ins_service_base.str_cust_approver_name,
                    ins_service_base.str_cust_approver_email,
                    ins_service_base.str_customer_emp_no,
                    ins_service_base.str_cust_employee_grade,
                    ins_service_base.str_customer_cost_centre,
                    ins_service_base.str_cust_department,
                    ins_service_base.str_cust_accounting_unit,
                    ins_service_base.str_cust_internal_ac_no,
                    ins_service_base.str_cust_project_code,
                    ins_service_base.str_cust_action_no,
                    ins_service_base.str_cust_job_code,
                    ins_service_base.str_cust_resource_code,
                    ins_service_base.str_cust_commitment_no,
                    ins_service_base.str_cust_purpose_of_travel,
                    ins_service_base.str_cust_pax_mobile,
                    ins_service_base.str_cust_pax_email,
                    ins_service_base.str_cust_engagement_code,
                    ins_service_base.str_rm_opt1,
                    ins_service_base.str_rm_opt2,
                    ins_service_base.str_rm_opt3,
                    ins_service_base.str_rm_opt4 ,
                    ins_service_base.str_rm_opt5,
                    ins_service_base.str_rm_opt6,
                    ins_service_base.str_rm_opt7,
                    ins_service_base.str_rm_opt8 ,
                    ins_service_base.str_rm_opt9,
                    ins_service_base.str_rm_opt10,
                    ins_service_base.str_rm_opt11, 
                    ins_service_base.str_rm_opt12 ,
                    ins_service_base.str_rm_opt13 ,
                    ins_service_base.str_rm_opt14 ,
                    ins_service_base.str_rm_opt15 ,
                    ins_service_base.str_rm_opt16,
                    ins_service_base.str_rm_opt17,
                    ins_service_base.str_rm_opt18,
                    ins_service_base.str_rm_opt19 ,
                    ins_service_base.str_rm_opt20 ,
                    ins_service_base.str_against_doc_ext ,
                    ins_service_base.str_corp_card_code_ext ,
                    ins_service_base.str_compliance_ext ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.str_pnr_type_ext ,
                    ins_service_base.str_quot_option_1 ,
                    ins_service_base.str_quot_option_2 ,
                    ins_service_base.str_master_reference , 
                    ins_service_base.str_master_narration ,
                    ins_service_base.str_rm_field_data,
                    ins_service_base.str_rate_type, #40299
                    ins_service_base.str_book_rate,
                    ins_service_base.str_cc_number_ext, #39948
                    ins_service_base.str_cc_type,
                    ins_service_base.str_hotel_chain_code,
                    ins_service_base.json_user_defined_remark
                    
            ))
            #45552
            lst_car_tax_details.append([
                    ins_service_base.flt_fare_inv,
                    ins_service_base.flt_total_tax_inv,
                    0.0, #std_commission
                    ins_service_base.flt_fare_inv+ins_service_base.flt_total_tax_inv, #supplier_amount
                    0.0, #vat_in
                    0.0, #agency_charge
                    0.0, #airline_charge
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    0.0, #extra_earning
                    0.0, #payback_amount
                    0.0, #cc_charge
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe), #client_net
                    0.0, #vat_out
                    ins_service_base.str_voucher_issue_date,
                    'C',
                    '', #str_sector
                    '', #tax_details
                    '', #ticket_type
                    ins_service_base.int_supplier_id,
                    ins_service_base.int_customer_account_id_inv,
                    ins_service_base.int_country_id,
                    None ,
                    ins_service_base.flt_cust_currency_roe,
                    ins_service_base.flt_supplier_currency_roe,
                    'I', 
                    ins_service_base.int_voucher_id,
                    ins_service_base.str_voucher_number,
                    ins_service_base.str_base_currency,
                    ins_service_base.str_tran_currency,
                    ins_service_base.str_cust_currency,
                    False,
                    False
            ])
            
        if lst_car_tax_details:  #45552
            self.save_tax_details(lst_car_tax_details)
            
        if lst_car_extra_capturing_fields:
               self.save_voucher_details_to_tbl_extra_capturing_data(lst_car_extra_capturing_fields)
               
        cr.close()
        return lst_car_vouchers
    
    def save_captured_other_service_voucher_data(self,lst_os):
        
        cr = self.create_cursor()
        
        lst_other_vouchers = [ins_service_base.str_voucher_number for ins_service_base in lst_os]
        str_tim_created = ins_general_methods.get_current_date_time()
        
        lst_other_voucher_data = []
        lst_os_sale_details = []
        lst_other_extra_capturing_fields = []   #refer #40299
        lst_os_tax_details = []  #45552
        for ins_service_base in lst_os :
            
            if ins_service_base.int_card_payment_type == 1  :
                ins_service_base.flt_fare_uccf_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_uccf_amount = ins_service_base.flt_total_tax_credit_card_inv 
            else :
                ins_service_base.flt_fare_card_amount = ins_service_base.flt_fare_credit_card_inv
                ins_service_base.flt_tax_card_amount = ins_service_base.flt_total_tax_credit_card_inv 
                
            
            lst_other_voucher_data.append((
                ins_service_base.int_service_id ,
                ins_service_base.str_voucher_number ,
                'I' ,
                'P' ,
                ins_service_base.chr_supp_doc_category ,
                ins_service_base.str_crs_company ,
                ins_service_base.str_file_name_inv ,
                ins_service_base.str_pnr_no ,
                ins_service_base.str_voucher_booking_date ,
                ins_service_base.str_voucher_issue_date ,
                ins_service_base.str_from_date ,
                ins_service_base.str_to_date ,
                ins_service_base.str_customer_lpo_number ,
                ins_service_base.str_customer_cost_centre ,
                ins_service_base.str_customer_emp_no ,
                ins_service_base.int_counter_staff_id ,
                ins_service_base.int_corp_card_id ,
                ins_service_base.int_card_payment_type ,
                ins_service_base.str_pax_name ,
                ins_service_base.int_no_of_guest_inv ,
                ins_service_base.int_company_id ,
                ins_service_base.int_location_id ,
                ins_service_base.int_department_id ,
                ins_service_base.int_customer_account_id_inv ,
                ins_service_base.int_supplier_id ,

                ins_service_base.str_particulars ,
                ins_service_base.str_remarks ,
                ins_service_base.flt_service_fee_percentage ,
                ins_service_base.str_base_currency ,
                ins_service_base.str_tran_currency ,
                ins_service_base.flt_supplier_currency_roe ,
                ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_fare_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_fare_credit_card_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_fare_credit_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_fare_credit_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_fare_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_total_tax_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_total_tax_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_card_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_total_tax_credit_card_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_total_tax_credit_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_total_tax_credit_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_gross_payable_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_gross_payable_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_net_payable_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_actual_cost_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_actual_cost_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_service_fee ,
                ins_general_methods.convert_amount(ins_service_base.flt_discount,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_discount ,
                ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_selling_price_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_debited_amount_inv ,
                ins_general_methods.convert_amount(ins_service_base.flt_profit_inv,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_profit_inv ,
                2 ,
                str_tim_created  ,
                ins_service_base.str_supplier_confirm_number,
                ins_service_base.str_pick_up_time,
                
                ins_general_methods.convert_amount(ins_service_base.flt_fare_card_amount,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_fare_card_amount ,
                ins_general_methods.convert_amount(ins_service_base.flt_fare_uccf_amount,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_fare_uccf_amount ,
                ins_general_methods.convert_amount(ins_service_base.flt_tax_card_amount,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_tax_card_amount ,
                ins_general_methods.convert_amount(ins_service_base.flt_tax_uccf_amount,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.flt_tax_uccf_amount,
                ins_service_base.dat_rm_lpo_date,

                ins_service_base.str_cust_currency,
                ins_service_base.flt_cust_currency_roe,
                ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                ins_service_base.int_cust_traveller_id, #45745 
                ins_service_base.int_product_master_id_inv,  #add new columns above this line
                ins_service_base.int_product_master_id_rfd

                ))
        
        
        
        cr.executemany("""
                INSERT INTO tbl_other_service
                (
                fk_bint_service_id ,
                vchr_supporting_document_number ,
                chr_supporting_document_status ,
                chr_system_sale_part_action ,
                chr_supporting_document_category ,
                vchr_gds_company ,
                vchr_gds_file_name_inv ,
                vchr_pnr_number ,
                dat_booking ,
                dat_issue ,
                dat_from ,
                dat_to ,
                vchr_customer_lpo_or_to_number ,
                vchr_customer_cost_centre ,
                vchr_customer_employee_number ,
                fk_bint_counter_staff_id ,
                fk_bint_corporate_card_account_id ,
                int_card_payment_type ,
                vchr_pax_name ,
                int_no_of_pax ,
                fk_bint_company_id_inv ,
                fk_bint_cost_center_id_inv ,
                fk_bint_department_id_inv ,
                fk_bint_customer_account_id_inv ,
                fk_bint_supplier_account_id_inv ,
                vchr_particulars_inv ,
                vchr_remarks_inv ,
                dbl_service_fee_percentage_inv ,
                vchr_base_currency_inv ,
                vchr_tran_currency_inv ,
                dbl_tran_currency_roe_inv ,
                dbl_base_currency_fare_inv ,
                dbl_tran_currency_fare_inv ,
                dbl_base_currency_fare_credit_card_inv ,
                dbl_tran_currency_fare_credit_card_inv ,
                dbl_base_currency_fare_credit_inv ,
                dbl_tran_currency_fare_credit_inv ,
                dbl_base_currency_printing_fare_inv ,
                dbl_tran_currency_printing_fare_inv ,
                dbl_base_currency_tax_inv ,
                dbl_tran_currency_tax_inv ,
                dbl_base_currency_tax_credit_card_inv ,
                dbl_tran_currency_tax_credit_card_inv ,
                dbl_base_currency_tax_credit_inv ,
                dbl_tran_currency_tax_credit_inv ,
                dbl_base_currency_gross_payable_inv ,
                dbl_tran_currency_gross_payable_inv ,
                dbl_base_currency_net_payable_inv ,
                dbl_tran_currency_net_payable_inv ,
                dbl_base_currency_actual_cost_inv ,
                dbl_tran_currency_actual_cost_inv ,
                dbl_base_currency_service_fee_inv ,
                dbl_tran_currency_service_fee_inv ,
                dbl_base_currency_discount_given_inv ,
                dbl_tran_currency_discount_given_inv ,
                dbl_base_currency_selling_price_inv ,
                dbl_tran_currency_selling_price_inv ,
                dbl_base_currency_debited_amount_inv ,
                dbl_tran_currency_debited_amount_inv ,
                dbl_base_currency_profit_inv ,
                dbl_tran_currency_profit_inv ,
                fk_bint_created_id ,
                tim_created ,
                vchr_supplier_reference_inv,
                tim_from_time,
                
                dbl_base_currency_market_fare_card_amount_inv,
                dbl_tran_currency_market_fare_card_amount_inv,
                dbl_base_currency_market_fare_uccf_amount_inv,
                dbl_tran_currency_market_fare_uccf_amount_inv,
                dbl_base_currency_tax_card_amount_inv,
                dbl_tran_currency_tax_card_amount_inv,
                dbl_base_currency_tax_uccf_amount_inv,
                dbl_tran_currency_tax_uccf_amount_inv,
                dat_customer_lpo_date_inv,                
                vchr_cust_currency_inv,
                dbl_cust_currency_roe_inv,
                dbl_cust_currency_selling_price_inv,
                dbl_cust_currency_debited_amount_inv,
                dbl_cust_currency_service_fee_inv,
                dbl_cust_currency_discount_given_inv,
                fk_bint_passenger_profile_id,
                fk_bint_product_master_id_inv,
                fk_bint_product_master_id_rfd
                
                )
                VALUES
                (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
                            
                            
                )
                
                """,lst_other_voucher_data)
                
                
        lst_os_sales_data = []
        for tpl_os_data in lst_other_voucher_data :
            
            lst_data_temp = list(copy.copy(tpl_os_data[:-2]))
            int_voucher_id = ins_general_methods.get_other_voucher_details(tpl_os_data[1])
            lst_data_temp.append(int_voucher_id)
#            lst_data_temp.extend(list(copy.copy(tpl_os_data[-7:])))   
#            lst_data_temp.append(tpl_os_data[-1])

#            del lst_data_temp[2]
#            del lst_data_temp[26]
            lst_os_sales_data.append(tuple(lst_data_temp))   
                            
        for ins_service_base in lst_os :    
            if ins_service_base.bln_transfer_voucher :
                int_voucher_id = ins_general_methods.get_other_voucher_details(ins_service_base.str_voucher_number)
                lst_os_sale_details.append((
                                    int_voucher_id ,
                                    None,#paybackaccount
                                    None,#fk_bint_visa_status_id,
                                    None,#fk_bint_passenger_profile_id,
                                    0.00,#dbl_commission_percentage,
                                    ins_service_base.flt_service_fee_percentage,
                                    0.00,#dbl_extra_earning_percentage,
                                    0.00,#dbl_payback_service_percentage,
                                    0.00,# pdb dbl_discount_given_percentage,
                                    ins_service_base.str_base_currency,#vchr_base_currency,
                                    ins_service_base.str_tran_currency ,
                                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_fare_inv ,
                                    0.00,#dbl_base_currency_vip_express_charge,
                                    0.00,#dbl_base_currency_commission,
                                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_net_payable_inv ,
                                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_service_fee ,
                                    0.00,#dbl_base_currency_extra_earning,
                                    0.00,#dbl_base_currency_cc_charges,
                                    0.00,#dbl_base_currency_discount_given,
                                    0.00,#dbl_base_currency_payback_amount,
                                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_selling_price_inv ,
                                    
                                    ins_service_base.flt_supplier_currency_roe ,
                                    
                                    0.00,#dbl_tran_currency_vip_express_charge,
                                    0.00,#dbl_tran_currency_commission,
                                    
                                    0.00,#dbl_tran_currency_extra_earning,
                                    0.00,#dbl_tran_currency_cc_charges,
                                    0.00,#dbl_tran_currency_discount_given,
                                    0.00,#dbl_tran_currency_payback_amount,
                                    
                                    '',#vchr_visa_application_no ,
                                    0.00,#dbl_base_visa_charge_credit_card,
                                    0.000,#dbl_tran_visa_charge_credit_card,
                                    0.00,#dbl_tran_express_charge_credit_card,
                                    0.00,#dbl_base_express_charge_credit_card,
                                    None,#fk_bint_car_id,
                                    None,#fk_bint_chauffeur_id,
                                    'Hired Fleet Hired Chauffeur',#vchr_fleet_type,
                                    ins_service_base.str_car_type,#vchr_car_name,
                                    0.00,#dbl_cc_charge_percentage,
                                    None,#fk_bint_country_id
                                    ins_service_base.str_cust_currency,
                                    ins_service_base.flt_cust_currency_roe,
                                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.int_cust_traveller_id #45745
                
                                    ))
                                    
            elif ins_service_base.bln_visa_voucher :
                int_voucher_id = ins_general_methods.get_other_voucher_details(ins_service_base.str_voucher_number)
                lst_os_sale_details.append((
                                    int_voucher_id ,
                                    None,#paybackaccount
                                    None,#fk_bint_visa_status_id,
                                    None,#fk_bint_passenger_profile_id,
                                    0.00,#dbl_commission_percentage,
                                    ins_service_base.flt_service_fee_percentage,
                                    0.00,#dbl_extra_earning_percentage,
                                    0.00,#dbl_payback_service_percentage,
                                    0.00,# pdb dbl_discount_given_percentage,
                                    ins_service_base.str_base_currency,#vchr_base_currency,
                                    ins_service_base.str_tran_currency ,
                                    ins_general_methods.convert_amount(ins_service_base.flt_fare_inv,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_fare_inv ,
                                    0.00,#dbl_base_currency_vip_express_charge,
                                    0.00,#dbl_base_currency_commission,
                                    ins_general_methods.convert_amount(ins_service_base.flt_net_payable_inv,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_net_payable_inv ,
                                    ins_general_methods.convert_amount(ins_service_base.flt_service_fee,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_service_fee ,
                                    0.00,#dbl_base_currency_extra_earning,
                                    0.00,#dbl_base_currency_cc_charges,
                                    0.00,#dbl_base_currency_discount_given,
                                    0.00,#dbl_base_currency_payback_amount,
                                    ins_general_methods.convert_amount(ins_service_base.flt_selling_price_inv,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.flt_selling_price_inv ,
                                    
                                    ins_service_base.flt_supplier_currency_roe ,
                                    
                                    0.00,#dbl_tran_currency_vip_express_charge,
                                    0.00,#dbl_tran_currency_commission,
                                    
                                    0.00,#dbl_tran_currency_extra_earning,
                                    0.00,#dbl_tran_currency_cc_charges,
                                    0.00,#dbl_tran_currency_discount_given,
                                    0.00,#dbl_tran_currency_payback_amount,
                                    
                                    ins_service_base.str_hotel_confirm_number,#vchr_visa_application_no ,
                                    0.00,#dbl_base_visa_charge_credit_card,
                                    0.000,#dbl_tran_visa_charge_credit_card,
                                    0.00,#dbl_tran_express_charge_credit_card,
                                    0.00,#dbl_base_express_charge_credit_card,
                                    None,#fk_bint_car_id,
                                    None,#fk_bint_chauffeur_id,
                                    '',#vchr_fleet_type,
                                    '',#vchr_car_name,
                                    0.00,#dbl_cc_charge_percentage,
                                    ins_general_methods.get_country_id(ins_service_base.str_country),#fk_bint_country_id
                                    ins_service_base.str_cust_currency,
                                    ins_service_base.flt_cust_currency_roe,
                                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                                    ins_service_base.int_cust_traveller_id #45745
                
                                    ))
            
            
            
            
            
            
            
            
                
        cr.executemany("""
                INSERT INTO tbl_other_service_sale
                (
                fk_bint_service_id ,
                vchr_supporting_document_number ,
                chr_supporting_document_status ,
                chr_system_sale_part_action ,
                chr_supporting_document_category ,
                vchr_gds_company ,
                vchr_gds_file_name_inv ,
                vchr_pnr_number ,
                dat_booking ,
                dat_issue ,
                dat_from ,
                dat_to ,
                vchr_customer_lpo_or_to_number ,
                vchr_customer_cost_centre ,
                vchr_customer_employee_number ,
                fk_bint_counter_staff_id ,
                fk_bint_corporate_card_account_id ,
                int_card_payment_type ,
                vchr_pax_name ,
                int_no_of_pax ,
                fk_bint_company_id_inv ,
                fk_bint_cost_center_id_inv ,
                fk_bint_department_id_inv ,
                fk_bint_customer_account_id_inv ,
                fk_bint_supplier_account_id_inv ,
                vchr_particulars_inv ,
                vchr_remarks_inv ,
                dbl_service_fee_percentage_inv ,
                vchr_base_currency_inv ,
                vchr_tran_currency_inv ,
                dbl_tran_currency_roe_inv ,
                dbl_base_currency_fare_inv ,
                dbl_tran_currency_fare_inv ,
                dbl_base_currency_fare_credit_card_inv ,
                dbl_tran_currency_fare_credit_card_inv ,
                dbl_base_currency_fare_credit_inv ,
                dbl_tran_currency_fare_credit_inv ,
                dbl_base_currency_printing_fare_inv ,
                dbl_tran_currency_printing_fare_inv ,
                dbl_base_currency_tax_inv ,
                dbl_tran_currency_tax_inv ,
                dbl_base_currency_tax_credit_card_inv ,
                dbl_tran_currency_tax_credit_card_inv ,
                dbl_base_currency_tax_credit_inv ,
                dbl_tran_currency_tax_credit_inv ,
                dbl_base_currency_gross_payable_inv ,
                dbl_tran_currency_gross_payable_inv ,
                dbl_base_currency_net_payable_inv ,
                dbl_tran_currency_net_payable_inv ,
                dbl_base_currency_actual_cost_inv ,
                dbl_tran_currency_actual_cost_inv ,
                dbl_base_currency_service_fee_inv ,
                dbl_tran_currency_service_fee_inv ,
                dbl_base_currency_discount_given_inv ,
                dbl_tran_currency_discount_given_inv ,
                dbl_base_currency_selling_price_inv ,
                dbl_tran_currency_selling_price_inv ,
                dbl_base_currency_debited_amount_inv ,
                dbl_tran_currency_debited_amount_inv ,
                dbl_base_currency_profit_inv ,
                dbl_tran_currency_profit_inv ,
                fk_bint_created_id ,
                tim_created ,
                vchr_supplier_reference_inv ,
                tim_from_time,
                dbl_base_currency_market_fare_card_amount_inv,
                dbl_tran_currency_market_fare_card_amount_inv,
                dbl_base_currency_market_fare_uccf_amount_inv,
                dbl_tran_currency_market_fare_uccf_amount_inv,
                dbl_base_currency_tax_card_amount_inv,
                dbl_tran_currency_tax_card_amount_inv,
                dbl_base_currency_tax_uccf_amount_inv,
                dbl_tran_currency_tax_uccf_amount_inv,
                dat_customer_lpo_date_inv,
                
                vchr_cust_currency_inv,
                dbl_cust_currency_roe_inv,
                dbl_cust_currency_selling_price_inv,
                dbl_cust_currency_debited_amount_inv,
                dbl_cust_currency_service_fee_inv,
                dbl_cust_currency_discount_given_inv,
                fk_bint_passenger_profile_id,
                
                fk_bint_other_service_id
                
                
                )
                VALUES
                (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
                            
                )
                
                """,lst_os_sales_data)
        

        if lst_os_sale_details         :
            cr.executemany("""INSERT INTO tbl_other_service_details 
                            (fk_bint_other_service_id,
                            fk_bint_payback_account_id,
                            fk_bint_visa_status_id,
                            fk_bint_passenger_profile_id,
                            dbl_commission_percentage,
                            dbl_service_fee_percentage,
                            dbl_extra_earning_percentage,
                            dbl_payback_service_percentage,
                            dbl_discount_given_percentage,
                            vchr_base_currency,
                            vchr_tran_currency,
                            dbl_base_currency_visa_charge,
                            dbl_tran_currency_visa_charge,
                            dbl_base_currency_vip_express_charge,
                            dbl_base_currency_commission,
                            dbl_base_currency_net_payable,
                            dbl_tran_currency_net_payable,
                            dbl_base_currency_service_fee,
                            dbl_tran_currency_service_fee,
                            dbl_base_currency_extra_earning,
                            dbl_base_currency_cc_charges,
                            dbl_base_currency_discount_given,
                            dbl_base_currency_payback_amount,
                            dbl_base_currency_selling_price,
                            dbl_tran_currency_selling_price,
                            
                            dbl_tran_currency_roe,
                            
                            dbl_tran_currency_vip_express_charge,
                            dbl_tran_currency_commission,
                            
                            
                            dbl_tran_currency_extra_earning,
                            dbl_tran_currency_cc_charges,
                            dbl_tran_currency_discount_given,
                            dbl_tran_currency_payback_amount,
                            
                            vchr_visa_application_no ,
                            dbl_base_visa_charge_credit_card,
                            dbl_tran_visa_charge_credit_card,
                            dbl_tran_express_charge_credit_card,
                            dbl_base_express_charge_credit_card,
                            fk_bint_car_id,
                            fk_bint_chauffeur_id,
                            vchr_fleet_type,
                            vchr_car_name,
                            dbl_cc_charge_percentage,
                            fk_bint_country_id,
                            
                            vchr_cust_currency_inv,
                            dbl_cust_currency_roe_inv,
                            dbl_cust_currency_selling_price_inv,
                            dbl_cust_currency_service_fee_inv,
                            fk_bint_passenger_profile_id)
                            VALUES
                            
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s)
                            """,lst_os_sale_details)
        #refer #40299
        for ins_service_base in lst_os:
            ins_service_base.int_voucher_id = ins_general_methods.get_other_voucher_details(ins_service_base.str_voucher_number)
            lst_other_extra_capturing_fields.append((
                    ins_service_base.int_voucher_id,
                    ins_service_base.str_voucher_number ,
                    'O',
                    ins_service_base.int_location_id ,
                    ins_service_base.int_department_id, 
                    ins_service_base.str_agency_sales_man ,
                    ins_service_base.str_agency_ticketing_staff,
                    ins_service_base.str_agency_traacs_user,
                    ins_service_base.str_agency_adv_receipt_no,
                    ins_service_base.str_agency_internal_remarks,
                    ins_service_base.str_agency_product_code,
                    ins_service_base.str_agency_sub_product_code,
                    ins_service_base.str_agency_auto_invoice_yes_no,
                    ins_service_base.str_party_file_job_card_no,
                    ins_service_base.str_party_lpo_no,
                    ins_service_base.int_party_maximum_tickets,
                    ins_service_base.str_party_multiple_fop_yes_no,
                    ins_service_base.str_cust_approver_name,
                    ins_service_base.str_cust_approver_email,
                    ins_service_base.str_customer_emp_no,
                    ins_service_base.str_cust_employee_grade,
                    ins_service_base.str_customer_cost_centre,
                    ins_service_base.str_cust_department,
                    ins_service_base.str_cust_accounting_unit,
                    ins_service_base.str_cust_internal_ac_no,
                    ins_service_base.str_cust_project_code,
                    ins_service_base.str_cust_action_no,
                    ins_service_base.str_cust_job_code,
                    ins_service_base.str_cust_resource_code,
                    ins_service_base.str_cust_commitment_no,
                    ins_service_base.str_cust_purpose_of_travel,
                    ins_service_base.str_cust_pax_mobile,
                    ins_service_base.str_cust_pax_email,
                    ins_service_base.str_cust_engagement_code,
                    ins_service_base.str_rm_opt1,
                    ins_service_base.str_rm_opt2,
                    ins_service_base.str_rm_opt3,
                    ins_service_base.str_rm_opt4 ,
                    ins_service_base.str_rm_opt5,
                    ins_service_base.str_rm_opt6,
                    ins_service_base.str_rm_opt7,
                    ins_service_base.str_rm_opt8 ,
                    ins_service_base.str_rm_opt9,
                    ins_service_base.str_rm_opt10,
                    ins_service_base.str_rm_opt11, 
                    ins_service_base.str_rm_opt12 ,
                    ins_service_base.str_rm_opt13 ,
                    ins_service_base.str_rm_opt14 ,
                    ins_service_base.str_rm_opt15 ,
                    ins_service_base.str_rm_opt16,
                    ins_service_base.str_rm_opt17,
                    ins_service_base.str_rm_opt18,
                    ins_service_base.str_rm_opt19 ,
                    ins_service_base.str_rm_opt20 ,
                    ins_service_base.str_against_doc_ext ,
                    ins_service_base.str_corp_card_code_ext ,
                    ins_service_base.str_compliance_ext ,
                    ins_service_base.int_customer_account_id_inv ,
                    ins_service_base.str_pnr_type_ext ,
                    ins_service_base.str_quot_option_1 ,
                    ins_service_base.str_quot_option_2 ,
                    ins_service_base.str_master_reference , 
                    ins_service_base.str_master_narration ,
                    ins_service_base.str_rm_field_data,
                    ins_service_base.str_rate_type, #40299
                    ins_service_base.str_book_rate,
                    ins_service_base.str_cc_number_ext,#39948
                    ins_service_base.str_cc_type,
                    ins_service_base.str_hotel_chain_code,
                    ins_service_base.json_user_defined_remark
                    
                    ))
                    
            #45552
            lst_os_tax_details.append([
                    ins_service_base.flt_fare_inv,
                    ins_service_base.flt_total_tax_inv,
                    0.0, #std_commission
                    ins_service_base.flt_fare_inv+ins_service_base.flt_total_tax_inv, #supplier_amount
                    0.0, #vat_in
                    0.0, #agency_charge
                    0.0, #airline_charge
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_service_fee,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    0.0, #extra_earning
                    0.0, #payback_amount
                    0.0, #cc_charge
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_discount,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_selling_price_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe),
                    ins_general_methods.convert_to_customer_currency(ins_service_base.flt_debited_amount_inv,ins_service_base.flt_cust_currency_roe,ins_service_base.flt_supplier_currency_roe), #client_net
                    0.0, #vat_out
                    ins_service_base.str_voucher_issue_date,
                    'O',
                    '', #str_sector
                    '', #tax_details
                    '', #ticket_type
                    ins_service_base.int_supplier_id,
                    ins_service_base.int_customer_account_id_inv,
                    ins_service_base.int_country_id,
                    ins_service_base.int_service_id ,
                    ins_service_base.flt_cust_currency_roe,
                    ins_service_base.flt_supplier_currency_roe,
                    'I', 
                    ins_service_base.int_voucher_id,
                    ins_service_base.str_voucher_number,
                    ins_service_base.str_base_currency,
                    ins_service_base.str_tran_currency,
                    ins_service_base.str_cust_currency,
                    False,
                    False
            ])
        
        if lst_os_tax_details:  #45552
            self.save_tax_details(lst_os_tax_details)
            
        if lst_other_extra_capturing_fields:
               self.save_voucher_details_to_tbl_extra_capturing_data(lst_other_extra_capturing_fields)
                       
        cr.close()                    
        return lst_other_vouchers
    
    
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
                cr.execute(""" SELECT tc.fk_bint_creditor_account_id,
                                    ta.vchr_currency_code
                       FROM tbl_creditor tc
                       INNER JOIN tbl_account ta
                        ON tc.fk_bint_creditor_account_id = ta.pk_bint_account_id
                        AND ta.chr_document_status = 'N'
                        AND tc.chr_document_status = 'N'
                        AND ta.vchr_currency_code = %s
                       WHERE substring(tc.vchr_iata_number,1,7) = %s
                       AND tc.chr_document_status = 'N' """,
                         
                         ( ins_ticket_base.str_defult_currency_code,
                            ins_ticket_base.str_ticketing_agency_iata_no[0:7]))

                rst = cr.fetchone()
                if rst:
                    if rst['vchr_currency_code'] == ins_ticket_base.str_defult_currency_code :
                        ins_ticket_base.int_supplier_id = rst['fk_bint_creditor_account_id']
                        ins_ticket_base.chr_supplier_type = 'S'
                        pass
                    else :
                        ins_ticket_base.int_supplier_id = None
                        ins_ticket_base.chr_supplier_type = ''
                        
            
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
                                        vchr_last_conjection_ticket_number,
                                        vchr_original_issue,
                                        vchr_customer_lpo_or_to_number,
                                        
                                        
                                        fk_bint_corporate_card_id,
                                        fk_bint_supplier_account_id_inv,
                                        dat_ticket_issue,
                                        
                                        vchr_booking_agent_code,
                                        vchr_booking_agent_numeric_code,
                                        vchr_ticketing_agent_code_inv,
                                        vchr_ticketing_agent_numeric_code_inv,
                                        fk_bint_airline_id,
                                        fk_bint_airline_account_id,
                                        fk_bint_region_id,
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
                            int_card_payment_type_rfd = int_card_payment_type ,
                            
                            dbl_base_currency_market_fare_card_amount_rfd = dbl_base_currency_market_fare_card_amount_inv , 	
                            dbl_base_currency_market_fare_uccf_amount_rfd = dbl_base_currency_market_fare_uccf_amount_inv , 	
                            dbl_tran_currency_market_fare_card_amount_rfd = dbl_tran_currency_market_fare_card_amount_inv , 	
                            dbl_tran_currency_market_fare_uccf_amount_rfd = dbl_tran_currency_market_fare_uccf_amount_inv , 	
                            dbl_base_currency_tax_card_amount_rfd = dbl_base_currency_tax_card_amount_inv , 	
                            dbl_base_currency_tax_uccf_amount_rfd = dbl_base_currency_tax_uccf_amount_inv , 	
                            dbl_tran_currency_tax_card_amount_rfd = dbl_tran_currency_tax_card_amount_inv , 	
                            dbl_tran_currency_tax_uccf_amount_rfd = dbl_tran_currency_tax_uccf_amount_inv,
                            dbl_base_currency_vat_in_rfd = dbl_base_currency_vat_in_inv,
                            dbl_tran_currency_vat_in_rfd = dbl_tran_currency_vat_in_inv,
                            vchr_cust_currency_rfd = vchr_cust_currency_inv,
                            dbl_cust_currency_roe_rfd = dbl_cust_currency_roe_inv,
                            dbl_cust_currency_client_net_rfd = dbl_cust_currency_selling_price_inv,
                            dbl_cust_currency_credited_amount_rfd =dbl_cust_currency_debited_amount_inv


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
                            dbl_tran_currency_vat_in_rfd = tkt.dbl_tran_currency_vat_in_inv,
                            dbl_cust_currency_client_net_rfd = tkt.dbl_cust_currency_selling_price_inv,
                            dbl_cust_currency_credited_amount_rfd = tkt.dbl_cust_currency_debited_amount_inv

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
        lst_tax_details_void = []  #45552
        #36781
        if ins_general_methods.check_ticket_already_exists_in_tbl_ticket_refund_or_not(ins_ticket_base.str_ticket_number):
            print(('\n\nDuplication on refund:',ins_ticket_base.str_ticket_number,'\n\n'))
            raise
        
        #45552
        lst_tax_details_void.append([
                ins_ticket_base.flt_market_fare_rfd,
                ins_ticket_base.flt_total_tax_rfd,
                ins_ticket_base.flt_standard_commission_rfd,
                ins_ticket_base.flt_supplier_amount_rfd,
                ins_ticket_base.flt_vat_in_rfd,
                0.0,#agency_charge
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),#airline_charge
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_service_charge_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_extra_earning_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_discount_given_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_selling_price_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                0.0,#vat_out
                ins_ticket_base.str_refund_date,
                'T',
                ins_ticket_base.str_sector,
                ins_ticket_base.str_tax_details_rfd,
                ins_ticket_base.str_ticket_type,
                ins_ticket_base.int_supplier_id_rfd,
                ins_ticket_base.int_account_master_id,
                None, #country_id
                None, #ticket, hotel - service id
                ins_ticket_base.flt_cust_currency_roe_rfd,
                ins_ticket_base.flt_supplier_currency_roe_rfd,
                ins_ticket_base.chr_ticket_status, #issue,rfd
                ins_ticket_base.int_ticket_id,
                ins_ticket_base.str_ticket_number,
                ins_ticket_base.str_base_currency_rfd,
                ins_ticket_base.str_defult_currency_code,
                ins_ticket_base.str_cust_currency_rfd,
                False,
                False
        ])
        
        str_current_date_time = ins_general_methods.get_current_date_time()

        cr.execute("""UPDATE tbl_ticket SET dat_refund = %s,
                                            chr_ticket_status = 'V',
                                            chr_supplier_ticket_status = 'V',
                                            chr_system_refund_part_action = 'I',
                                            vchr_ticketing_agent_code_rfd = %s,
                                            vchr_ticketing_agent_numeric_code_rfd = %s,
                                            vchr_gds_file_name_rfd = %s,
                                            vchr_tran_currency_rfd = %s,
                                            vchr_base_currency_rfd = %s,
                                            vchr_pax_name_rfd = %s,
                                            vchr_sector_rfd = %s,
                                            int_no_of_segments_rfd = %s,
                                            int_no_of_pax_rfd = %s,
                                            fk_bint_supplier_account_id_rfd = %s ,
                                            fk_bint_payback_account_id_rfd = %s ,
                                            dbl_payback_service_percentage_rfd = %s ,
                                            dbl_base_currency_payback_service_fee_rfd = %s,
                                            dbl_tran_currency_payback_service_fee_rfd = %s ,
                                            fk_bint_counter_staff_id_rfd = %s,
                                            fk_bint_modified_user_id = 2,
                                            tim_modified = now(),
                                            vchr_cust_currency_rfd = %s,
                                            fk_bint_cost_center_id_rfd = %s,
                                            fk_bint_department_id_rfd = %s,
                                            int_distribution_type_rfd = %s

                      WHERE vchr_ticket_number = %s
                            AND chr_system_refund_part_action = '' 
                            AND COALESCE(vchr_document_no_rfd,'') = '' """,
                      (
                      ins_ticket_base.str_refund_date,
                      ins_ticket_base.str_ticketing_agent_code,
                      ins_ticket_base.str_ticketing_agent_numeric_code,
                      ins_ticket_base.str_file_name_rfd,
                      ins_ticket_base.str_tran_currency_rfd, #36583
                      ins_general_methods.str_base_currency,
                      ins_ticket_base.str_pax_name_rfd,
                      ins_ticket_base.str_sector,
                      ins_ticket_base.int_no_of_segments,
                      ins_ticket_base.int_no_of_pax_rfd,
                      ins_ticket_base.int_supplier_id_rfd,
                      ins_ticket_base.int_pay_back_account_id_rfd ,
                      ins_ticket_base.flt_pay_back_commission_percentage_rfd ,
                      ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                      ins_ticket_base.flt_pay_back_commission_rfd ,
                      ins_ticket_base.int_counter_staff_id_rfd ,
                      ins_ticket_base.str_cust_currency_rfd,   #40225
                      ins_ticket_base.int_location_id_rfd,  #44730
                      ins_ticket_base.int_branch_id_rfd,
                      ins_ticket_base.int_distribution_type_rfd, #46542
                      ins_ticket_base.str_ticket_number))

        if not cr.rowcount: #39067
            raise
         
             
        cr.execute(""" INSERT INTO tbl_ticket_refund
                            (
                            fk_bint_ticket_id,
                            vchr_ticket_number,
                            vchr_original_issue,
                            vchr_last_conjection_ticket_number,
                            chr_ticket_status,
                            chr_system_refund_part_action,
                            chr_ticket_category,
                            vchr_gds_company,
                            vchr_gds_file_name_rfd,
                            vchr_pnr_number,
                            dat_refund,
                            vchr_booking_agent_code,
                            vchr_booking_agent_numeric_code,
                            vchr_ticketing_agent_code_rfd,
                            vchr_ticketing_agent_numeric_code_rfd,
                            fk_bint_ticket_type_id,
                            fk_bint_airline_id,
                            fk_bint_airline_account_id,
                            fk_bint_region_id,
                            vchr_class_chr,
                            fk_bint_booking_class_id,
                            vchr_class,
                            vchr_return_class_chr,
                            fk_bint_return_booking_class_id,
                            vchr_return_class,
                            vchr_tour_code,
                            vchr_fare_basis,
                            vchr_customer_lpo_or_to_number,
                            vchr_customer_cost_centre,
                            vchr_customer_employee_number,
                            vchr_pax_type,
                            fk_bint_company_id_rfd,
                            fk_bint_cost_center_id_rfd,
                            fk_bint_department_id_rfd,
                            fk_bint_customer_account_id_rfd,
                            fk_bint_supplier_account_id_rfd,
                            vchr_sector_rfd,
                            int_no_of_segments_rfd,
                            vchr_pax_name_rfd,
                            int_no_of_pax_rfd,
                            vchr_remarks_rfd,
                            vchr_base_currency_rfd,
                            vchr_tran_currency_rfd,
                            dbl_tran_currency_roe_rfd,
                            chr_document_status,
                            fk_bint_created_user_id,
                            tim_created,
                            vchr_card_approval_code,
                            fk_bint_payback_account_id_rfd  ,
                            dbl_payback_service_percentage_rfd  ,
                            dbl_base_currency_payback_service_fee_rfd ,
                            dbl_tran_currency_payback_service_fee_rfd ,
                            fk_bint_counter_staff_id,
                            fk_bint_booking_agent_counter_staff_id,
                            vchr_crm_reference,
                            vchr_cust_currency_rfd,
                            dbl_cust_currency_roe_rfd,
                            int_distribution_type_rfd
                            )

                            VALUES

                            (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s)


                        """ ,(

                            ins_ticket_base.int_ticket_id,
                            ins_ticket_base.str_ticket_number,
                            ins_ticket_base.str_original_issue,
                            ins_ticket_base.str_last_conjection_ticket_number,
                            'V',
                            'I',
                            'P',
                            ins_ticket_base.str_crs_company,
                            ins_ticket_base.str_file_name_rfd,
                            ins_ticket_base.str_pnr_no,
                            ins_ticket_base.str_refund_date,
                            ins_ticket_base.str_booking_agent_code,
                            ins_ticket_base.str_booking_agent_numeric_code,
                            ins_ticket_base.str_ticketing_agent_code,
                            ins_ticket_base.str_ticketing_agent_numeric_code,
                            ins_ticket_base.int_ticket_type_id,
                            ins_ticket_base.int_airline_id,
                            ins_ticket_base.int_airline_account_id,
                            ins_ticket_base.int_region_id,
                            ins_ticket_base.str_class,
                            ins_ticket_base.int_class_id,
                            ins_ticket_base.str_class_group,
                            ins_ticket_base.str_return_class,
                            ins_ticket_base.int_return_class_id,
                            ins_ticket_base.str_return_class_group,
                            ins_ticket_base.str_tour_code,
                            ins_ticket_base.str_fare_basis,
                            ins_ticket_base.str_lpo_number,
                            ins_ticket_base.str_cust_cost_centre,
                            ins_ticket_base.str_employee_number,
                            ins_ticket_base.str_pax_type,
                            1, #compony id
                            ins_ticket_base.int_location_id_rfd,  #44032
                            ins_ticket_base.int_branch_id_rfd,
                            ins_ticket_base.int_account_master_id,
                            ins_ticket_base.int_supplier_id_rfd,
                            ins_ticket_base.str_sector,
                            ins_ticket_base.int_no_of_segments,
                            ins_ticket_base.str_pax_name,
                            ins_ticket_base.int_no_of_pax_rfd,
                            ins_ticket_base.str_remarks_rfd,
                            ins_general_methods.str_base_currency, #vchr_base_currency_rfd
                            ins_ticket_base.str_tran_currency_rfd, #vchr_tran_currency_rfd #36583
                            ins_ticket_base.flt_supplier_currency_roe,
                            'N',
                            2,
                            str_current_date_time,
                            ins_ticket_base.str_card_approval_code ,
                            ins_ticket_base.int_pay_back_account_id_rfd ,
                            ins_ticket_base.flt_pay_back_commission_percentage_rfd ,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_pay_back_commission_rfd ,
                            ins_ticket_base.int_counter_staff_id_rfd,
                            ins_ticket_base.int_booking_agent_counter_staff_id,
                            ins_ticket_base.str_crm_reference,
                            ins_ticket_base.str_cust_currency_rfd,  #40225
                            ins_ticket_base.flt_cust_currency_roe_rfd,
                            ins_ticket_base.int_distribution_type_rfd   #46542

                        ))


        cr.execute("""UPDATE tbl_ticket_sales SET chr_ticket_status = 'V'
                            WHERE vchr_ticket_number = %s
                            AND chr_document_status = 'N'

                        """,(ins_ticket_base.str_ticket_number,))

        if lst_tax_details_void:  #45552
            self.save_tax_details(lst_tax_details_void)
            
        if ins_general_methods.bln_enable_multiple_discount_details:#45196
            ins_general_methods.save_discount_data_to_tbl_multi_discount_details(ins_ticket_base,'V')
        
        cr.close()

        if ins_general_methods.bln_enable_non_iata_capture :
            ins_general_methods.ins_global.lst_process_list_void.append(ins_ticket_base)    #37364
#            thread = Thread(target = ins_general_methods.create_json_and_upload,args = [ins_ticket_base])
#            thread.start()
#            thread.join()

##    def update_refund_sides_when_ticket_void(self,str_ticket_number):
##        
##        cr = self.create_cursor()
##        
##        cr.execute("""UPDATE tbl_ticket
#                        SET dbl_std_commission_percentage_rfd = dbl_std_commission_percentage_inv ,
#                            dbl_service_fee_percentage_rfd = dbl_service_fee_percentage_inv ,
#                            dbl_extra_earning_percentage_rfd = dbl_extra_earning_percentage_inv ,
#                            dbl_payback_service_percentage_rfd = dbl_payback_service_percentage_inv,
#                            dbl_discount_given_percentage_rfd = dbl_discount_given_percentage_inv,
#                            dbl_billing_tax_percentage_rfd = dbl_billing_tax_percentage_inv,
#                            dbl_cc_charge_collected_percentage_rfd = dbl_cc_charge_collected_percentage_inv,
#                            dbl_base_currency_published_fare_rfd = dbl_base_currency_published_fare_inv ,
#                            dbl_base_currency_market_fare_rfd = dbl_base_currency_market_fare_inv ,
#                            dbl_base_currency_market_fare_credit_card_rfd = dbl_base_currency_market_fare_credit_card_inv ,
#                            dbl_base_currency_market_fare_credit_rfd = dbl_base_currency_market_fare_credit_inv ,
#                            dbl_base_currency_special_fare_rfd = dbl_base_currency_special_fare_inv ,
#                            dbl_base_currency_printing_fare_rfd = dbl_base_currency_printing_fare_inv ,
#                            dbl_base_currency_tax_rfd = dbl_base_currency_tax_inv,
#                            dbl_base_currency_tax_credit_card_rfd = dbl_base_currency_tax_credit_card_inv,
#                            dbl_base_currency_tax_credit_rfd = dbl_base_currency_tax_credit_inv,
#                            dbl_base_currency_supplier_fee_rfd = dbl_base_currency_supplier_fee_inv,
#                            dbl_base_currency_std_commission_amount_rfd = dbl_base_currency_std_commission_amount_inv,
#                            dbl_base_currency_gross_payable_rfd = dbl_base_currency_gross_payable_inv,
#                            dbl_base_currency_inv_net_payable_rfd = dbl_base_currency_net_payable_inv,
#                            dbl_base_currency_actual_cost_rfd = dbl_base_currency_actual_cost_inv,
#                            dbl_base_currency_supplier_bsp_file_amount_rfd = dbl_base_currency_supplier_bsp_file_amount_inv,
#                            dbl_base_currency_service_fee_rfd = dbl_base_currency_service_fee_inv,
#                            dbl_base_currency_extra_earning_rfd = dbl_base_currency_extra_earning_inv,
#                            dbl_base_currency_payback_service_fee_rfd = dbl_base_currency_payback_service_fee_inv,
#                            dbl_base_currency_iata_insurance_payable_rfd = dbl_base_currency_iata_insurance_payable_inv,
#                            dbl_base_currency_iata_insurance_collected_rfd = dbl_base_currency_iata_insurance_collected_inv,
#                            dbl_base_currency_discount_given_rfd = dbl_base_currency_discount_given_inv,
#                            dbl_base_currency_billing_tax_rfd = dbl_base_currency_billing_tax_inv,
#                            dbl_base_currency_cc_charge_collected_rfd = dbl_base_currency_cc_charge_collected_inv,
#                            dbl_base_currency_selling_price_rfd = dbl_base_currency_selling_price_inv,
#                            dbl_base_currency_debited_amount_rfd = dbl_base_currency_debited_amount_inv,
#                            dbl_base_currency_profit_rfd = dbl_base_currency_profit_inv,
#                            dbl_base_currency_fare_differece_rfd = dbl_base_currency_fare_differece_inv,
#                            dbl_base_currency_adm_expect_rfd = dbl_base_currency_adm_expect_inv,
#                            dbl_base_currency_acm_expect_rfd = dbl_base_currency_acm_expect_inv,
#                            dbl_tran_currency_roe_rfd = dbl_tran_currency_roe_inv,
#                            dbl_tran_currency_published_fare_rfd = dbl_tran_currency_published_fare_inv,
#                            dbl_tran_currency_market_fare_rfd = dbl_tran_currency_market_fare_inv,
#                            dbl_tran_currency_market_fare_credit_card_rfd = dbl_tran_currency_market_fare_credit_card_inv,
#                            dbl_tran_currency_market_fare_credit_rfd = dbl_tran_currency_market_fare_credit_inv,
#                            dbl_tran_currency_special_fare_rfd = dbl_tran_currency_special_fare_inv,
#                            dbl_tran_currency_printing_fare_rfd = dbl_tran_currency_printing_fare_inv,
#                            dbl_tran_currency_tax_rfd  = dbl_tran_currency_tax_inv,
#                            dbl_tran_currency_tax_credit_card_rfd = dbl_tran_currency_tax_credit_card_inv,
#                            dbl_tran_currency_tax_credit_rfd = dbl_tran_currency_tax_credit_inv,
#                            dbl_tran_currency_supplier_fee_rfd = dbl_tran_currency_supplier_fee_inv,
#                            dbl_tran_currency_std_commission_amount_rfd = dbl_tran_currency_std_commission_amount_inv,
#                            dbl_tran_currency_gross_payable_rfd = dbl_tran_currency_gross_payable_inv,
#                            dbl_tran_currency_inv_net_payable_rfd = dbl_tran_currency_net_payable_inv,
#                            dbl_tran_currency_actual_cost_rfd = dbl_tran_currency_actual_cost_inv,
#                            dbl_tran_currency_supplier_bsp_file_amount_rfd = dbl_tran_currency_supplier_bsp_file_amount_inv,
#                            dbl_tran_currency_service_fee_rfd = dbl_tran_currency_service_fee_inv,
#                            dbl_tran_currency_payback_service_fee_rfd = dbl_tran_currency_payback_service_fee_inv,
#                            dbl_tran_currency_iata_insurance_payable_rfd = dbl_tran_currency_iata_insurance_payable_inv,
#                            dbl_tran_currency_iata_insurance_collected_rfd = dbl_tran_currency_iata_insurance_collected_inv,
#                            dbl_tran_currency_discount_given_rfd = dbl_tran_currency_discount_given_inv,
#                            dbl_tran_currency_billing_tax_rfd = dbl_tran_currency_billing_tax_inv,
#                            dbl_tran_currency_cc_charge_collected_rfd = dbl_tran_currency_cc_charge_collected_inv,
#                            dbl_tran_currency_selling_price_rfd = dbl_tran_currency_selling_price_inv,
#                            dbl_tran_currency_debited_amount_rfd = dbl_tran_currency_debited_amount_inv,
#                            dbl_tran_currency_profit_rfd = dbl_tran_currency_profit_inv,
#                            dbl_tran_currency_fare_differece_rfd = dbl_tran_currency_fare_differece_inv,
#                            dbl_tran_currency_adm_expect_rfd = dbl_tran_currency_adm_expect_inv,
#                            dbl_tran_currency_acm_expect_rfd = dbl_tran_currency_acm_expect_inv,
#                            dbl_trancurrency_extra_earning_rfd = dbl_tran_currency_extra_earning_inv
#                        WHERE vchr_ticket_number = %s#""",(str_ticket_number,))
##                        
##        cr.close()
##        pass
##    
    def save_refund_tickets(self, ins_ticket_base):
        cr = self.create_cursor()
        lst_tax_details_rfd = []  #45552
        #36781
        if ins_general_methods.check_ticket_already_exists_in_tbl_ticket_refund_or_not(ins_ticket_base.str_ticket_number):
            print(('\n\nDuplication on refund:',ins_ticket_base.str_ticket_number,'\n\n'))
            raise
        
        str_current_date_time = ins_general_methods.get_current_date_time()
        # refer 40225
        if ins_ticket_base.str_ticket_issue_date  or ins_ticket_base.str_refund_date and ins_general_methods.ins_capture_base.bln_multi_currency:
                        ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.str_cust_currency_rfd = ins_general_methods.get_customer_currency_roe(ins_ticket_base.int_account_master_id,
                               ins_ticket_base.str_ticket_issue_date or ins_ticket_base.str_refund_date ,ins_general_methods.str_base_currency)
        
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
        
        #45552
        lst_tax_details_rfd.append([
                ins_ticket_base.flt_market_fare_rfd,
                ins_ticket_base.flt_total_tax_rfd,
                ins_ticket_base.flt_standard_commission_rfd,
                ins_ticket_base.flt_supplier_amount_rfd,
                ins_ticket_base.flt_vat_in_rfd,
                0.0,#agency_charge
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),#airline_charge
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_service_charge_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_extra_earning_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_cc_charge_collected_ext,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_discount_given_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_selling_price_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                0.0, #vat_out
                ins_ticket_base.str_refund_date,
                'T',
                ins_ticket_base.str_sector,
                ins_ticket_base.str_tax_details_rfd,
                ins_ticket_base.str_ticket_type,
                ins_ticket_base.int_supplier_id_rfd,
                ins_ticket_base.int_account_master_id,
                None, #country_id
                None, #ticket, hotel - service id
                ins_ticket_base.flt_cust_currency_roe_rfd,
                ins_ticket_base.flt_supplier_currency_roe_rfd,
                'IR', #issue,rfd ,#47520 - status to IR for refund captured
                ins_ticket_base.int_ticket_id,
                ins_ticket_base.str_ticket_number,
                ins_ticket_base.str_base_currency_rfd,
                ins_ticket_base.str_defult_currency_code,
                ins_ticket_base.str_cust_currency_rfd,
                False,
                False
        ])

        cr.execute("""UPDATE tbl_ticket SET dat_refund = %s,
                                            dbl_base_currency_market_fare_rfd = %s,
                                            dbl_tran_currency_market_fare_rfd = %s,
                                            dbl_base_currency_market_fare_credit_rfd = %s,
                                            dbl_tran_currency_market_fare_credit_rfd = %s,
                                            dbl_base_currency_market_fare_credit_card_rfd = %s,
                                            dbl_tran_currency_market_fare_credit_card_rfd = %s,
                                            dbl_base_currency_special_fare_rfd = %s,
                                            dbl_tran_currency_special_fare_rfd = %s,
                                            dbl_base_currency_published_fare_rfd = %s,
                                            dbl_tran_currency_published_fare_rfd = %s,
                                            dbl_base_currency_tax_rfd = %s,
                                            dbl_tran_currency_tax_rfd = %s,
                                            dbl_base_currency_tax_credit_rfd = %s,
                                            dbl_tran_currency_tax_credit_rfd = %s,
                                            dbl_base_currency_tax_credit_card_rfd = %s,
                                            dbl_tran_currency_tax_credit_card_rfd = %s,
                                            dbl_base_currency_supplier_charge_rfd = %s,
                                            dbl_tran_currency_supplier_charge_rfd = %s,
                                            dbl_base_currency_sup_charge_credit_card_rfd = %s,
                                            dbl_tran_currency_sup_charge_credit_card_rfd = %s,
                                            dbl_base_currency_sup_charge_credit_rfd = %s,
                                            dbl_tran_currency_sup_charge_credit_rfd = %s,
                                            chr_ticket_status = 'R',
                                            chr_supplier_ticket_status = 'R',
                                            chr_system_refund_part_action = 'I',
                                            vchr_ticketing_agent_code_rfd = %s,
                                            vchr_ticketing_agent_numeric_code_rfd = %s,
                                            dbl_base_currency_client_charge_rfd = %s,
                                            dbl_tran_currency_client_charge_rfd = %s,
                                            vchr_gds_file_name_rfd = %s,
                                            vchr_tran_currency_rfd = %s,
                                            dbl_tran_currency_roe_rfd = %s,
                                            int_no_of_pax_rfd = %s,
                                            vchr_pax_name_rfd = %s,
                                            int_no_of_segments_rfd = %s,
                                            vchr_base_currency_rfd = %s,
                                            dbl_base_currency_supplier_net_rfd = %s,
                                            dbl_tran_currency_supplier_net_rfd = %s,
                                            dbl_base_currency_client_net_rfd = %s,
                                            dbl_tran_currency_client_net_rfd = %s,
                                            dbl_base_currency_inv_net_payable_rfd = %s,
                                            dbl_tran_currency_inv_net_payable_rfd = %s,
                                            dbl_base_currency_actual_cost_rfd = %s,
                                            dbl_tran_currency_actual_cost_rfd = %s,
                                            dbl_base_currency_service_fee_rfd = %s,
                                            dbl_tran_currency_service_fee_rfd = %s,
                                            dbl_base_currency_std_commission_amount_rfd = %s,
                                            dbl_tran_currency_std_commission_amount_rfd = %s,
                                            dbl_base_currency_selling_price_rfd = %s,
                                            dbl_tran_currency_selling_price_rfd = %s,

                                            dbl_base_currency_debited_amount_rfd = %s,
                                            dbl_tran_currency_debited_amount_rfd = %s,
                                            dbl_base_currency_inv_debited_amount_rfd = %s,
                                            dbl_tran_currency_inv_debited_amount_rfd = %s,
                                            dbl_base_currency_credited_amount_rfd = %s,
                                            dbl_tran_currency_credited_amount_rfd = %s,

                                            fk_bint_supplier_account_id_rfd = %s ,
                                            dbl_base_currency_profit_rfd = %s,
                                            dbl_tran_currency_profit_rfd = %s ,
                                            dbl_base_currency_gross_payable_rfd = %s ,
                                            dbl_tran_currency_gross_payable_rfd = %s ,
                                            int_card_payment_type_rfd = %s ,
                                            vchr_sector_rfd = %s ,
                                            fk_bint_payback_account_id_rfd = %s ,
                                            dbl_payback_service_percentage_rfd = %s ,
                                            dbl_base_currency_payback_service_fee_rfd = %s,
                                            dbl_tran_currency_payback_service_fee_rfd = %s ,
                                            
                                            dbl_base_currency_discount_given_rfd = %s ,
                                            dbl_tran_currency_discount_given_rfd = %s ,
                                            fk_bint_discount_account_id_rfd = %s ,
                                            dbl_discount_given_percentage_rfd = %s ,
                                            fk_bint_extra_earning_account_id_rfd = %s ,
                                            dbl_extra_earning_percentage_rfd = %s ,
                                            dbl_base_currency_extra_earning_rfd = %s ,
                                            dbl_trancurrency_extra_earning_rfd = %s ,
                                            fk_bint_counter_staff_id_rfd = %s ,
                                            dbl_base_currency_printing_fare_rfd = %s ,
                                            dbl_tran_currency_printing_fare_rfd = %s ,
                                            
                                            dbl_base_currency_market_fare_card_amount_rfd = %s ,	
                                            dbl_tran_currency_market_fare_card_amount_rfd = %s ,
                                            dbl_base_currency_market_fare_uccf_amount_rfd = %s , 	
                                            dbl_tran_currency_market_fare_uccf_amount_rfd = %s ,	

                                            dbl_base_currency_tax_card_amount_rfd = %s , 	
                                            dbl_tran_currency_tax_card_amount_rfd = %s ,
                                            dbl_base_currency_tax_uccf_amount_rfd = %s ,	
                                            dbl_tran_currency_tax_uccf_amount_rfd = %s ,

                                            dbl_base_currency_sup_charge_card_amount_rfd = %s ,	
                                            dbl_tran_currency_sup_charge_card_amount_rfd = %s ,
                                            dbl_base_currency_sup_charge_uccf_amount_rfd = %s ,
                                            dbl_tran_currency_sup_charge_uccf_amount_rfd = %s ,
                                            fk_bint_corporate_card_id_rfd = %s,
                                            dbl_base_currency_vat_in_rfd = %s,
                                            dbl_tran_currency_vat_in_rfd = %s,
                                            fk_bint_customer_account_id_rfd = %s,
					    fk_bint_cost_center_id_rfd = %s,
					    fk_bint_department_id_rfd = %s,
					    dbl_service_fee_percentage_rfd = %s,
                                            dbl_std_commission_percentage_rfd = %s,
                                            vchr_tax_details_rfd = %s,
                                            vchr_cust_currency_rfd = %s,
                                            dbl_cust_currency_roe_rfd = %s,
                                            dbl_cust_currency_client_net_rfd = %s,
                                            dbl_cust_currency_credited_amount_rfd =%s,
                                            int_distribution_type_rfd = %s
                                            
                                            
                      WHERE vchr_ticket_number = %s 
                            AND chr_system_refund_part_action = '' 
                            AND COALESCE(vchr_document_no_rfd,'') = '' """,
                      (ins_ticket_base.str_refund_date,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_market_fare_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_market_fare_cash_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_market_fare_credit_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_special_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_special_fare_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_published_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_published_fare_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_total_tax_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_total_tax_cash_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_total_tax_credit_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_supplier_refund_charge,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_credit,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_supplier_refund_charge_credit,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_cash,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_supplier_refund_charge_cash,
                       ins_ticket_base.str_ticketing_agent_code,
                       ins_ticket_base.str_ticketing_agent_numeric_code_rfd,  #44586
                       ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_client_refund_charge,
                       ins_ticket_base.str_file_name_rfd,
                       ins_ticket_base.str_tran_currency_rfd,
                       ins_ticket_base.flt_supplier_currency_roe_rfd,
                       ins_ticket_base.int_no_of_pax_rfd,
                       ins_ticket_base.str_pax_name,
                       ins_ticket_base.int_number_of_segments_rfd,
                       ins_general_methods.str_base_currency,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_supplier_refund_net,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_client_refund_net,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_net_payable_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_net_payable_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_actual_cost_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_actual_cost_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_service_charge_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_service_charge_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_standard_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_standard_commission_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_selling_price_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                       ins_ticket_base.flt_selling_price_rfd,
                       ins_general_methods.convert_amount(ins_ticket_base.flt_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_debited_amount_rfd,
                        ins_general_methods.convert_amount(ins_ticket_base.flt_inv_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_inv_debited_amount_rfd,
                        ins_general_methods.convert_amount(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_credited_amount_rfd,
                        ins_ticket_base.int_supplier_id_rfd,
                        ins_general_methods.convert_amount(ins_ticket_base.flt_profit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_profit_rfd ,
                        ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_supplier_amount_rfd,
                        ins_ticket_base.int_card_payment_type_rfd,
                        ins_ticket_base.str_sector,
                        ins_ticket_base.int_pay_back_account_id_rfd ,
                        ins_ticket_base.flt_pay_back_commission_percentage_rfd ,
                        ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_pay_back_commission_rfd ,
                        
                        
                        ins_general_methods.convert_amount(ins_ticket_base.flt_discount_given_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_discount_given_rfd,
                        ins_ticket_base.int_discount_account_id_rfd ,
                        ins_ticket_base.flt_discount_given_percentage_rfd ,
                        ins_ticket_base.int_extra_earning_account_id_rfd ,
                        ins_ticket_base.flt_extra_earninig_percentage_rfd ,
                        ins_general_methods.convert_amount(ins_ticket_base.flt_extra_earning_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_extra_earning_rfd ,
                        ins_ticket_base.int_counter_staff_id_rfd ,
                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_market_fare_rfd,
                        
                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_market_fare_card_amount_rfd , 

                        ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_market_fare_uccf_amount_rfd  , 

                        ins_general_methods.convert_amount(ins_ticket_base.flt_tax_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_tax_card_amount_rfd ,	

                        ins_general_methods.convert_amount(ins_ticket_base.flt_tax_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_tax_uccf_amount_rfd ,	

                        ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_sup_charge_card_amount_rfd , 

                        ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_sup_charge_uccf_amount_rfd,
                        
                        int_corporate_card_id_rfd,
                        
                        ins_general_methods.convert_amount(ins_ticket_base.flt_vat_in_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.flt_vat_in_rfd,
                        ins_ticket_base.int_account_master_id,
			ins_ticket_base.int_location_id_rfd, #44032
                        ins_ticket_base.int_branch_id_rfd,
                        ins_ticket_base.flt_service_charge_percentage_rfd,
                        ins_ticket_base.flt_std_commn_percentage_rfd,
                        ins_ticket_base.str_tax_details_rfd,
                        ins_ticket_base.str_cust_currency_rfd,  #40225
                        ins_ticket_base.flt_cust_currency_roe_rfd,
                        ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                        ins_ticket_base.int_distribution_type_rfd, #46542
                        
                       ins_ticket_base.str_ticket_number

                       ))
                       
        if not cr.rowcount: #39067
            raise
        
        cr.execute(""" INSERT INTO tbl_ticket_refund
                            (
                            fk_bint_ticket_id,
                            vchr_ticket_number,
                            vchr_original_issue,
                            vchr_last_conjection_ticket_number,
                            chr_ticket_status,
                            chr_system_refund_part_action,
                            chr_ticket_category,
                            vchr_gds_company,
                            vchr_gds_file_name_rfd,
                            vchr_pnr_number,
                            dat_refund,
                            vchr_booking_agent_code,
                            vchr_booking_agent_numeric_code,
                            vchr_ticketing_agent_code_rfd,
                            vchr_ticketing_agent_numeric_code_rfd,
                            fk_bint_ticket_type_id,
                            fk_bint_airline_id,
                            fk_bint_airline_account_id,
                            fk_bint_region_id,
                            vchr_class_chr,
                            fk_bint_booking_class_id,
                            vchr_class,
                            vchr_return_class_chr,
                            fk_bint_return_booking_class_id,
                            vchr_return_class,
                            vchr_tour_code,
                            vchr_fare_basis,
                            vchr_customer_lpo_or_to_number,
                            vchr_customer_cost_centre,
                            vchr_customer_employee_number,
                            vchr_pax_type,
                            fk_bint_company_id_rfd,
                            fk_bint_cost_center_id_rfd,
                            fk_bint_department_id_rfd,
                            fk_bint_customer_account_id_rfd,
                            fk_bint_supplier_account_id_rfd,
                            vchr_sector_rfd,
                            int_no_of_segments_rfd,
                            vchr_pax_name_rfd,
                            int_no_of_pax_rfd,
                            vchr_remarks_rfd,
                            vchr_base_currency_rfd,
                            vchr_tran_currency_rfd,
                            dbl_tran_currency_roe_rfd,
                            chr_document_status,
                            fk_bint_created_user_id,
                            tim_created ,

                            dbl_base_currency_published_fare_rfd,
                            dbl_tran_currency_published_fare_rfd,
                            dbl_base_currency_market_fare_rfd,
                            dbl_tran_currency_market_fare_rfd,
                            dbl_base_currency_market_fare_credit_card_rfd,
                            dbl_tran_currency_market_fare_credit_card_rfd,
                            dbl_base_currency_market_fare_credit_rfd,
                            dbl_tran_currency_market_fare_credit_rfd,
                            dbl_base_currency_special_fare_rfd,
                            dbl_tran_currency_special_fare_rfd,
                            dbl_base_currency_tax_rfd,
                            dbl_tran_currency_tax_rfd,
                            dbl_base_currency_tax_credit_card_rfd,
                            dbl_tran_currency_tax_credit_card_rfd,
                            dbl_base_currency_tax_credit_rfd,
                            dbl_tran_currency_tax_credit_rfd,
                            dbl_base_currency_gross_payable_rfd,
                            dbl_tran_currency_gross_payable_rfd,
                            dbl_base_currency_supplier_charge_rfd,
                            dbl_tran_currency_supplier_charge_rfd,
                            dbl_base_currency_sup_charge_credit_card_rfd,
                            dbl_tran_currency_sup_charge_credit_card_rfd,
                            dbl_base_currency_sup_charge_credit_rfd,
                            dbl_tran_currency_sup_charge_credit_rfd,
                            dbl_base_currency_supplier_net_rfd,
                            dbl_tran_currency_supplier_net_rfd,
                            dbl_base_currency_selling_price_rfd,
                            dbl_tran_currency_selling_price_rfd,
                            dbl_base_currency_client_charge_rfd,
                            dbl_tran_currency_client_charge_rfd,
                            dbl_base_currency_client_net_rfd,
                            dbl_tran_currency_client_net_rfd,
                            dbl_base_currency_inv_net_payable_rfd,
                            dbl_tran_currency_inv_net_payable_rfd,
                            dbl_base_currency_actual_cost_rfd,
                            dbl_tran_currency_actual_cost_rfd,
                            dbl_base_currency_service_fee_rfd,
                            dbl_tran_currency_service_fee_rfd ,

                            dbl_base_currency_debited_amount_rfd,
                            dbl_tran_currency_debited_amount_rfd,
                            dbl_base_currency_inv_debited_amount_rfd,
                            dbl_tran_currency_inv_debited_amount_rfd,
                            dbl_base_currency_credited_amount_rfd,
                            dbl_tran_currency_credited_amount_rfd ,
                            dbl_base_currency_profit_rfd ,
                            dbl_tran_currency_profit_rfd ,
                            int_card_payment_type ,
                            fk_bint_payback_account_id_rfd  ,
                            dbl_payback_service_percentage_rfd  ,
                            dbl_base_currency_payback_service_fee_rfd ,
                            dbl_tran_currency_payback_service_fee_rfd ,
                            
                            dbl_base_currency_discount_given_rfd  ,
                            dbl_tran_currency_discount_given_rfd  ,
                            fk_bint_discount_account_id_rfd ,
                            dbl_discount_given_percentage_rfd  ,
                            fk_bint_extra_earning_account_id_rfd ,
                            dbl_extra_earning_percentage_rfd ,
                            dbl_base_currency_extra_earning_rfd  ,
                            dbl_trancurrency_extra_earning_rfd ,
                            fk_bint_counter_staff_id ,
                            dbl_base_currency_printing_fare_rfd ,
                            dbl_tran_currency_printing_fare_rfd,
                            fk_bint_booking_agent_counter_staff_id,
                            
                            dbl_base_currency_market_fare_card_amount_rfd, 	
                            dbl_tran_currency_market_fare_card_amount_rfd,
                            dbl_base_currency_market_fare_uccf_amount_rfd, 	
                            dbl_tran_currency_market_fare_uccf_amount_rfd, 	

                            dbl_base_currency_tax_card_amount_rfd, 	
                            dbl_tran_currency_tax_card_amount_rfd,
                            dbl_base_currency_tax_uccf_amount_rfd, 	
                            dbl_tran_currency_tax_uccf_amount_rfd,

                            dbl_base_currency_sup_charge_card_amount_rfd, 	
                            dbl_tran_currency_sup_charge_card_amount_rfd,
                            dbl_base_currency_sup_charge_uccf_amount_rfd, 
                            dbl_tran_currency_sup_charge_uccf_amount_rfd,
                            
                            fk_bint_corporate_card_id,
                            dbl_base_currency_vat_in_rfd,
                            dbl_tran_currency_vat_in_rfd,
                            vchr_card_approval_code,
                            dbl_service_fee_percentage_rfd,
                            dbl_std_commission_percentage_rfd,
                            dbl_base_currency_std_commission_amount_rfd,
                            dbl_tran_currency_std_commission_amount_rfd,
                            fk_bint_credit_card_pos_id,
                            vchr_cc_number,
                            vchr_tax_details_rfd,
                            vchr_cust_currency_rfd,
                            dbl_cust_currency_roe_rfd,
                            dbl_cust_currency_credited_amount_rfd,
                            dbl_cust_currency_client_net_rfd,
                            int_distribution_type_rfd
                            )

                            VALUES

                            (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s )


                        """ ,(


                            ins_ticket_base.int_ticket_id,
                            ins_ticket_base.str_ticket_number,
                            ins_ticket_base.str_original_issue,
                            ins_ticket_base.str_last_conjection_ticket_number,
                            'R',
                            'I',
                            'P',
                            ins_ticket_base.str_crs_company,
                            ins_ticket_base.str_file_name_rfd,
                            ins_ticket_base.str_pnr_no,
                            ins_ticket_base.str_refund_date,
                            ins_ticket_base.str_booking_agent_code,
                            ins_ticket_base.str_booking_agent_numeric_code,
                            ins_ticket_base.str_ticketing_agent_code,
                            ins_ticket_base.str_ticketing_agent_numeric_code_rfd, #44586
                            ins_ticket_base.int_ticket_type_id,
                            ins_ticket_base.int_airline_id,
                            ins_ticket_base.int_airline_account_id,
                            ins_ticket_base.int_region_id,
                            ins_ticket_base.str_class,
                            ins_ticket_base.int_class_id,
                            ins_ticket_base.str_class_group,
                            ins_ticket_base.str_return_class,
                            ins_ticket_base.int_return_class_id,
                            ins_ticket_base.str_return_class_group,
                            ins_ticket_base.str_tour_code,
                            ins_ticket_base.str_fare_basis,
                            ins_ticket_base.str_lpo_number,
                            ins_ticket_base.str_cust_cost_centre,
                            ins_ticket_base.str_employee_number,
                            ins_ticket_base.str_pax_type,
                            1, #compony id
                            ins_ticket_base.int_location_id_rfd,  #44032
                            ins_ticket_base.int_branch_id_rfd,
                            ins_ticket_base.int_account_master_id,
                            ins_ticket_base.int_supplier_id_rfd,
                            ins_ticket_base.str_sector,
                            ins_ticket_base.int_no_of_segments,
                            ins_ticket_base.str_pax_name,
                            ins_ticket_base.int_no_of_pax_rfd,
                            ins_ticket_base.str_remarks_rfd,
                            ins_general_methods.str_base_currency, #vchr_base_currency_rfd
                            ins_ticket_base.str_defult_currency_code, #vchr_tran_currency_rfd
                            ins_ticket_base.flt_supplier_currency_roe_rfd,#36583
                            'N',
                            2,
                            str_current_date_time,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_published_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_published_fare_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_market_fare_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_market_fare_credit_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_market_fare_cash_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_special_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_special_fare_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_total_tax_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_credit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_total_tax_credit_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_total_tax_cash_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_total_tax_cash_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_supplier_amount_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_supplier_refund_charge,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_credit,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_supplier_refund_charge_credit,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_charge_cash,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_supplier_refund_charge_cash,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_supplier_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_supplier_refund_net,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_selling_price_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_selling_price_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_charge,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_client_refund_charge,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_client_refund_net,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_net_payable_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_net_payable_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_actual_cost_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_actual_cost_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_service_charge_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_service_charge_rfd ,

                            ins_general_methods.convert_amount(ins_ticket_base.flt_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_debited_amount_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_inv_debited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_inv_debited_amount_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_credited_amount_rfd ,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_profit_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_profit_rfd ,
                            ins_ticket_base.int_card_payment_type_rfd ,
                            ins_ticket_base.int_pay_back_account_id_rfd ,
                            ins_ticket_base.flt_pay_back_commission_percentage_rfd ,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_pay_back_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_pay_back_commission_rfd ,
                            
                            ins_general_methods.convert_amount(ins_ticket_base.flt_discount_given_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_discount_given_rfd,
                            ins_ticket_base.int_discount_account_id_rfd ,
                            ins_ticket_base.flt_discount_given_percentage_rfd ,
                            ins_ticket_base.int_extra_earning_account_id_rfd ,
                            ins_ticket_base.flt_extra_earninig_percentage_rfd ,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_extra_earning_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_extra_earning_rfd ,
                            ins_ticket_base.int_counter_staff_id_rfd ,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_market_fare_rfd,
                            ins_ticket_base.int_booking_agent_counter_staff_id,
                            
                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_market_fare_card_amount_rfd , 

                            ins_general_methods.convert_amount(ins_ticket_base.flt_market_fare_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_market_fare_uccf_amount_rfd  , 

                            ins_general_methods.convert_amount(ins_ticket_base.flt_tax_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_tax_card_amount_rfd ,	

                            ins_general_methods.convert_amount(ins_ticket_base.flt_tax_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_tax_uccf_amount_rfd ,	

                            ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_card_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_sup_charge_card_amount_rfd , 

                            ins_general_methods.convert_amount(ins_ticket_base.flt_sup_charge_uccf_amount_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_sup_charge_uccf_amount_rfd,

                            int_corporate_card_id_rfd,
                            
                            ins_general_methods.convert_amount(ins_ticket_base.flt_vat_in_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_vat_in_rfd,
                            ins_ticket_base.str_card_approval_code,
                            ins_ticket_base.flt_service_charge_percentage_rfd,
                            ins_ticket_base.flt_std_commn_percentage_rfd,
                            ins_general_methods.convert_amount(ins_ticket_base.flt_standard_commission_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.flt_standard_commission_rfd,
                            ins_ticket_base.int_credit_card_pos_id,     #40837
                            ins_ticket_base.str_cc_number,
                            ins_ticket_base.str_tax_details_rfd,
                            ins_ticket_base.str_cust_currency_rfd,  #40225
                            ins_ticket_base.flt_cust_currency_roe_rfd,
                            ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_credited_amount_rfd,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_general_methods.convert_to_customer_currency(ins_ticket_base.flt_client_refund_net,ins_ticket_base.flt_cust_currency_roe_rfd,ins_ticket_base.flt_supplier_currency_roe_rfd),
                            ins_ticket_base.int_distribution_type_rfd  #46542
                            
                        ))
        cr.execute("""UPDATE tbl_ticket_sales SET chr_ticket_status = 'R'
                            WHERE vchr_ticket_number = %s
                            AND chr_document_status = 'N'

                        """,(ins_ticket_base.str_ticket_number,))
        
        if lst_tax_details_rfd:  #45552
            self.save_tax_details(lst_tax_details_rfd)
            
        if ins_general_methods.bln_enable_multiple_discount_details:#45196
            ins_general_methods.save_discount_data_to_tbl_multi_discount_details(ins_ticket_base,'R')
                        
        cr.close()

#    def clear_tickets_refund_data(self, str_ticket_number):
#
#        cr = self.create_cursor()
#
#
#        cr.execute("""UPDATE tbl_ticket SET dat_refund = NULL,
#                                            bl_base_currency_market_fare_rfd = 0.00,
#                                            dbl_tran_currency_market_fare_rfd = 0.00,
#                                            dbl_base_currency_tax_rfd = 0.00,
#                                            dbl_tran_currency_tax_rfd = 0.00,
#                                            dbl_base_currency_supplier_refund_charge_rfd = 0.00,
#                                            dbl_tran_currency_supplier_refund_charge_rfd = 0.00,
#                                            chr_ticket_status = CASE WHEN vchr_document_no_inv = ''
#                                                THEN 'I'
#                                                ELSE 'S'
#                                                END,
#                                            vchr_ticketing_agent_code_rfd = '',
#                                            dbl_base_currency_client_charge_rfd = 0.00,
#                                            dbl_tran_currency_client_charge_rfd = 0.00,
#                                            vchr_gds_file_name_rfd = ''
#                      WHERE vchr_ticket_number = %s #""",(str_ticket_number, ))
#
#        cr.close()

    def clear_tickets_refund_data(self, str_ticket_number):

        cr = self.create_cursor()

        cr.execute("""UPDATE tbl_ticket SET dat_refund = NULL,
                                        dbl_base_currency_market_fare_rfd = 0.00,
                                            dbl_tran_currency_market_fare_rfd = 0.00,
                                        dbl_base_currency_market_fare_credit_rfd = 0.00,
                                        dbl_tran_currency_market_fare_credit_rfd = 0.00,
                                        dbl_base_currency_market_fare_credit_card_rfd = 0.00,
                                        dbl_tran_currency_market_fare_credit_card_rfd = 0.00,
                                        dbl_base_currency_special_fare_rfd = 0.0,
                                        dbl_tran_currency_special_fare_rfd = 0.0,
                                        dbl_base_currency_published_fare_rfd = 0.0,
                                        dbl_tran_currency_published_fare_rfd = 0.0,
                                        dbl_base_currency_tax_rfd = 0.0,
                                        dbl_tran_currency_tax_rfd = 0.0,
                                        dbl_base_currency_tax_credit_rfd = 0.0,
                                        dbl_tran_currency_tax_credit_rfd = 0.0,
                                        dbl_base_currency_tax_credit_card_rfd = 0.0,
                                        dbl_tran_currency_tax_credit_card_rfd = 0.0,
                                        dbl_base_currency_supplier_charge_rfd = 0.0,
                                        dbl_tran_currency_supplier_charge_rfd = 0.0,
                                        dbl_base_currency_sup_charge_credit_card_rfd = 0.0,
                                        dbl_tran_currency_sup_charge_credit_card_rfd = 0.0,
                                        dbl_base_currency_sup_charge_credit_rfd = 0.0,
                                        dbl_tran_currency_sup_charge_credit_rfd = 0.0,
                                        chr_ticket_status = CASE WHEN COALESCE(vchr_document_no_inv,'') = ''
                                            THEN 'I' ELSE 'S' END,
                                        chr_supplier_ticket_status = CASE WHEN COALESCE(vchr_document_no_inv,'') = ''
                                            THEN 'I' ELSE 'S' END,
                                        chr_system_refund_part_action = '',
                                            vchr_ticketing_agent_code_rfd = '',
                                        vchr_ticketing_agent_numeric_code_rfd = '',
                                        dbl_base_currency_client_charge_rfd = 0.0,
                                        dbl_tran_currency_client_charge_rfd = 0.0,
                                        vchr_gds_file_name_rfd = '',
                                        vchr_tran_currency_rfd = '',
                                        dbl_tran_currency_roe_rfd = 0.0,
                                        int_no_of_pax_rfd = 0,
                                        vchr_pax_name_rfd = '',
                                        int_no_of_segments_rfd = 0,
                                        vchr_base_currency_rfd = '',
                                        dbl_base_currency_supplier_net_rfd = 0.0,
                                        dbl_tran_currency_supplier_net_rfd = 0.0,
                                        dbl_base_currency_client_net_rfd = 0.0,
                                        dbl_tran_currency_client_net_rfd = 0.0,
                                        dbl_base_currency_inv_net_payable_rfd = 0.0,
                                        dbl_tran_currency_inv_net_payable_rfd = 0.0,
                                        dbl_base_currency_actual_cost_rfd = 0.0,
                                        dbl_tran_currency_actual_cost_rfd = 0.0,
                                        dbl_base_currency_service_fee_rfd = 0.0,
                                        dbl_tran_currency_service_fee_rfd = 0.0,
                                        dbl_base_currency_std_commission_amount_rfd = 0.0,
                                        dbl_tran_currency_std_commission_amount_rfd = 0.0,
                                        dbl_base_currency_selling_price_rfd = 0.0,
                                        dbl_tran_currency_selling_price_rfd = 0.0,

                                        dbl_base_currency_debited_amount_rfd = 0.0,
                                        dbl_tran_currency_debited_amount_rfd = 0.0,
                                        dbl_base_currency_inv_debited_amount_rfd = 0.0,
                                        dbl_tran_currency_inv_debited_amount_rfd = 0.0,
                                        dbl_base_currency_credited_amount_rfd = 0.0,
                                        dbl_tran_currency_credited_amount_rfd = 0.0,

                                        fk_bint_supplier_account_id_rfd = NULL ,
                                        dbl_base_currency_profit_rfd = 0.0,
                                        dbl_tran_currency_profit_rfd = 0.0 ,
                                        dbl_base_currency_gross_payable_rfd = 0.0 ,
                                        dbl_tran_currency_gross_payable_rfd = 0.0 ,
                                        int_card_payment_type_rfd = NULL ,
                                        vchr_sector_rfd = '' ,
                                        fk_bint_payback_account_id_rfd = NULL ,
                                        dbl_payback_service_percentage_rfd = 0.0 ,
                                        dbl_base_currency_payback_service_fee_rfd = 0.0,
                                        dbl_tran_currency_payback_service_fee_rfd = 0.0 ,

                                        dbl_base_currency_discount_given_rfd = 0.0 ,
                                        dbl_tran_currency_discount_given_rfd = 0.0 ,
                                        fk_bint_discount_account_id_rfd = NULL ,
                                        dbl_discount_given_percentage_rfd = 0.0 ,
                                        fk_bint_extra_earning_account_id_rfd = NULL ,
                                        dbl_extra_earning_percentage_rfd = 0.0 ,
                                        dbl_base_currency_extra_earning_rfd = 0.0 ,
                                        dbl_trancurrency_extra_earning_rfd = 0.0 ,
                                        fk_bint_counter_staff_id_rfd = NULL ,
                                        dbl_base_currency_printing_fare_rfd = 0.0 ,
                                        dbl_tran_currency_printing_fare_rfd = 0.0 ,

                                        dbl_base_currency_market_fare_card_amount_rfd = 0.0 ,	
                                        dbl_tran_currency_market_fare_card_amount_rfd = 0.0 ,
                                        dbl_base_currency_market_fare_uccf_amount_rfd = 0.0 , 	
                                        dbl_tran_currency_market_fare_uccf_amount_rfd = 0.0 ,	

                                        dbl_base_currency_tax_card_amount_rfd = 0.0 , 	
                                        dbl_tran_currency_tax_card_amount_rfd = 0.0 ,
                                        dbl_base_currency_tax_uccf_amount_rfd = 0.0 ,	
                                        dbl_tran_currency_tax_uccf_amount_rfd = 0.0 ,

                                        dbl_base_currency_sup_charge_card_amount_rfd = 0.0 ,	
                                        dbl_tran_currency_sup_charge_card_amount_rfd = 0.0 ,
                                        dbl_base_currency_sup_charge_uccf_amount_rfd = 0.0 ,
                                        dbl_tran_currency_sup_charge_uccf_amount_rfd = 0.0 ,
                                        fk_bint_corporate_card_id_rfd = NULL,
                                        
                                        dbl_base_currency_vat_in_rfd = 0.0, 
                                        dbl_tran_currency_vat_in_rfd = 0.0,
                                        fk_bint_customer_account_id_rfd = NULL,
                                        fk_bint_cost_center_id_rfd = NULL,
                                        fk_bint_department_id_rfd = NULL, 
                                        dbl_service_fee_percentage_rfd = 0.0, 
                                        dbl_std_commission_percentage_rfd = 0.0, 
                                        vchr_tax_details_rfd = ''

                      WHERE vchr_ticket_number = %s AND COALESCE(vchr_document_no_rfd,'') = '' """,(str_ticket_number,))
        if not cr.rowcount:
            raise

        cr.execute("""UPDATE tbl_ticket_sales 
                        SET chr_ticket_status = CASE WHEN COALESCE(vchr_document_no_inv,'') = ''
                                   THEN 'I' ELSE 'S' END
                        WHERE vchr_ticket_number = %s """,(str_ticket_number,))

        cr.execute("""UPDATE tbl_ticket_refund 
                        SET chr_document_status = 'D' 
                        WHERE vchr_ticket_number = %s """,(str_ticket_number,))
                            
        cr.close()
    
    def save_voucher_details_to_tbl_extra_capturing_data(self,lst_extra_capturing_fields):
        try:
            cr = self.create_cursor()        
            cr.executemany("""INSERT INTO tbl_extra_capturing_data
                            (   fk_bint_supporting_document_id  ,
                                vchr_supporting_document_no ,
                                chr_supporting_document_type ,
                                fk_bint_cost_center_id ,
                                fk_bint_department_id ,
                                vchr_agency_sales_man ,
                                vchr_agency_ticketing_staff ,
                                vchr_agency_traacs_user ,
                                vchr_agency_adv_receipt_no ,
                                vchr_agency_internal_remarks  ,
                                vchr_agency_product_code ,
                                vchr_agency_sub_product_code ,
                                vchr_agency_auto_invoice_yes_no  ,
                                vchr_party_file_job_card_no ,
                                vchr_party_lpo_no ,
                                int_party_maximum_tickets ,
                                vchr_party_multiple_fop_yes_no ,
                                vchr_cust_approver_name ,
                                vchr_cust_approver_email ,
                                vchr_cust_employee_no ,
                                vchr_cust_employee_grade ,
                                vchr_cust_cost_centre ,
                                vchr_cust_department ,
                                vchr_cust_accounting_unit ,
                                vchr_cust_internal_ac_no ,
                                vchr_cust_project_code ,
                                vchr_cust_action_no ,
                                vchr_cust_job_code ,
                                vchr_cust_resource_code ,
                                vchr_cust_commitment_no ,
                                vchr_cust_purpose_of_travel ,
                                vchr_cust_pax_mobile ,
                                vchr_cust_pax_email ,
                                vchr_cust_engagement_code ,
                                vchr_field_1 ,
                                vchr_field_2 ,
                                vchr_field_3 ,
                                vchr_field_4 ,
                                vchr_field_5 ,
                                vchr_field_6 ,
                                vchr_field_7 ,
                                vchr_field_8 ,
                                vchr_field_9 ,
                                vchr_field_10 ,
                                vchr_field_11 ,
                                vchr_field_12 ,
                                vchr_field_13 ,
                                vchr_field_14 ,
                                vchr_field_15 ,
                                vchr_field_16 ,
                                vchr_field_17 ,
                                vchr_field_18 ,
                                vchr_field_19 ,
                                vchr_field_20 ,
                                vchr_against_doc_no ,
                                vchr_corp_card_code ,
                                vchr_compliance ,
                                fk_bint_customer_account_id ,
                                vchr_pnr_type ,
                                vchr_quot_option_1,
                                vchr_quot_option_2,
                                vchr_master_reference,
                                vchr_master_narration,
                                vchr_rm_field,
                                vchr_rate_type,
                                vchr_book_rate,
                                vchr_card_no,
                                vchr_card_type,
                                vchr_hotel_chain_code,
                                json_extra_userdefined
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", lst_extra_capturing_fields)
                                        
    
        except Exception as msg:
            ins_general_methods.ins_db.rollback()
            cr.close()
            raise
        else:
            ins_general_methods.ins_db.commit()
            cr.close()
            
    def save_refund_void_data_of_original_tickets(self , lst_original_ticket_details ,
                                                        str_refund_date ,
                                                        str_status, 
                                                        str_file_name_rfd,
                                                        str_base_currency,
                                                        str_tran_currency,
                                                        int_customer_id_rfd,
                                                        int_supplier_id_rfd,
                                                        int_cost_center_id_rfd,
                                                        int_department_id_rfd,
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
                          vchr_base_currency_rfd = '%s',
                          vchr_tran_currency_rfd = '%s' ,
                          fk_bint_customer_account_id_rfd = %s ,
                          fk_bint_supplier_account_id_rfd = %s ,
                          fk_bint_cost_center_id_rfd = %s ,
                          fk_bint_department_id_rfd = %s ,
                          vchr_ticketing_agent_code_rfd = '%s' ,
                          vchr_ticketing_agent_numeric_code_rfd = '%s' ,
                          fk_bint_counter_staff_id_rfd = %s 
                       WHERE vchr_ticket_number IN (%s)
                    """%(str_refund_date ,
                        str_status,
                        str_file_name_rfd,
                        str_base_currency,
                        str_tran_currency,
                        int_customer_id_rfd  or 'NULL',
                        int_supplier_id_rfd or 'NULL',
                        int_cost_center_id_rfd  or 'NULL',
                        int_department_id_rfd  or 'NULL',
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
                    or (ins_general_methods.ins_auto_inv.bln_auto_invoice and ins_capture_emd_ticket_data.str_agency_auto_invoice_yes_no.upper() in ('T','TRUE','YES','Y')))\
                    and not (ins_capture_ticket_data.str_tour_code and ins_general_methods.ins_auto_inv.bln_skip_tkt_with_tour_code): # // refer #42644 //refer 21791 
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
                            if ins_capture_ticket_data.str_void_date :#pdb check
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
        
        
        
        ins_non_iata_settings = ins_general_methods.get_non_iata_instance(ins_ticket_base)
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
            
            ins_general_methods.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        'Creating session failed , please check server side working or not !!!' + str(msg),
                                        str_status
                                        
                                        )
            return

    
        if response.status_code == 200 and (response.content.decode().find('"STATUS":1') != -1 or response.content.decode().find('"STATUS":2') != -1):
            try :
                    print((response.content.decode()))
            except :
                pass
            print('success')
        else :
            print('Json upload Failed')
            ins_general_methods.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        ('Saving Failed Error Code : ' + str(response.status_code) + '    ' +  str(response.content.decode()))[:500],
                                        str_status
                                        
                                        )
    
        try :
            session.close()
        except :
            pass
        
        
    #37364    
    def create_void_json_and_upload(self,ins_ticket_base): 
        ins_non_iata_settings = ins_general_methods.get_non_iata_instance(ins_ticket_base)
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
            
            ins_general_methods.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        'Creating session failed , please check server side working or not !!!###' + str(msg),
                                        str_status
                                        
                                        )
            return

        
        if response.status_code == 200 and (response.content.decode().find('"STATUS":1') != -1 or response.content.decode().find('"STATUS":5') != -1):
            try :
                print((response.content.decode()))
            except :
                pass
            print('Void Json upload success')
        else :
            print('Void Json upload Failed')
            ins_general_methods.write_error_message(ins_non_iata_settings.int_non_iata_capturing_settings_id,
                                        ins_ticket_base.int_ticket_id,
                                        ('Saving Failed Error Code : ' + str(response.status_code) + '    ' +  str(response.content.decode()))[:500],
                                        str_status
                                        
                                        )
    
        try :
            session.close()
        except :
            pass
        
    def save_airport(self,lst_sector):

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
        
    def add_new_airline_into_db(self, str_airline_chr_code,str_airline_numeric_code, str_airline_name ):

        if  not str_airline_numeric_code:
            str_airline_numeric_code = str_airline_chr_code

        if not str_airline_chr_code:
            str_airline_chr_code = str_airline_numeric_code

        if not str_airline_name:
            str_airline_name =  str_airline_chr_code + str_airline_numeric_code

        str_airline_code = str_airline_chr_code + str_airline_numeric_code
        str_current_date_time = ins_general_methods.get_current_date_time()

        int_account_id = ins_general_methods.get_max_value('tbl_account','pk_bint_account_id')

        while ins_general_methods.get_account_data(str_airline_code)[0] :
            str_airline_code += '0'

        cr = self.create_cursor()
        cr.execute("""
                    INSERT INTO tbl_account
                        (
                        vchr_system_mapping_code ,
                        vchr_account_code ,
                        vchr_account_name ,
                        dat_create_date ,
                        vchr_currency_code ,
                        int_account_type
                        )
                        VALUES
                        (
                        %s,%s,%s,
                        %s,%s,%s
                        )
                        """,
                        (str_airline_code,
                        str_airline_code,
                        str_airline_name,
                        str_current_date_time,
                        ins_general_methods.str_base_currency ,
                        17 ))
 
        int_account_id = ins_general_methods.get_max_value('tbl_account','pk_bint_account_id 	')- 1

        cr.execute("""
        INSERT INTO tbl_airline
            (
            fk_bint_airline_account_id ,
            vchr_airline_numeric_code ,
            vchr_airline_chr_code ,
            vchr_airline_code ,
            vchr_airline_name ,
            fk_bint_created_id ,
            tim_created
            )
            VALUES
            (
            %s, %s, %s,
            %s, %s, %s,
            %s
            )
            """
            ,(
            int_account_id ,
            str_airline_numeric_code ,
            str_airline_chr_code ,
            str_airline_code ,
            str_airline_name ,
            2 ,
            str_current_date_time

            ))
        int_airline_id = ins_general_methods.get_max_value('tbl_airline','pk_bint_airline_id')- 1
        
        cr.close()
        return int_airline_id ,int_account_id
    
    def save_emd_sector(self, str_reason_for_issuance):
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
                            fk_bint_created_id,
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
                            vchr_hotel_fax ,
                            vchr_hotel_email ,
                            int_star_rating ,
                            chr_document_status ,
                            fk_bint_created_user_id ,
                            tim_created
                            )
                            values
                            (%s,%s,%s,%s,
                                %s,%s,%s,%s,
                                    %s,%s,%s)

                             """,(int_city_id,
                                   str_hotel_code,
                                    str_hotel_name,
                                     str_hotel_address,
                                      str_hotel_phone,
                                       str_hotel_fax,
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
        
    #45552    
    def save_tax_details(self, lst_tax_details):
        
        """
        function arguments
        -------------------
        dbl_fare ,
        dbl_tax ,
        dbl_std_commission ,
        dbl_supplier_amount ,
        dbl_vat_in ,
        dbl_agency_charge ,
        dbl_airline_charge ,
        dbl_service_fee ,
        dbl_extra_earning ,
        dbl_payback_amount ,
        dbl_cc_charge ,
        dbl_discount ,
        dbl_selling_price ,
        dbl_client_net ,
        dbl_vat_out ,
        effective_date, 
        supporting_document_type ,
        str_sector ,
        str_tax_details ,
        str_ticket_type ,
        int_supplier_account_id ,
        int_customer_account_id ,
        int_country_id ,
        int_service_id ,
        dbl_cust_roe ,
        dbl_supplier_roe ,
        str_module ,
        int_supporting_doc_id ,
        supporting_document_no ,
        base_currency ,
        tran_currency ,
        cust_currency ,
        bln_zero_ip_vat_set ,
        bln_zero_op_vat_set 
        """
        cr = self.create_cursor()
        for lst_temp in lst_tax_details:
            if lst_temp[17] == '/': lst_temp[17] = ''
            if lst_temp[4] == 0.0 : lst_temp[32] = True  #46458
            
            cr.execute("""SELECT  insert_into_tax_details
                                            (
                                            %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s
                                            )""",lst_temp)

            #checking whether the function insertion is success or fail
            rst = cr.fetchone()
            if not rst or rst[0] != 'SUCCESS' :
                cr.close()
                raise Exception('Unable to save tax details')

        cr.close()
