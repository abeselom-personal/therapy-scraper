import gc
import os
from datetime import datetime

import numpy as np
import pandas as pd


class LargeExcelProcessor:
    def __init__(self, file_path, chunksize=1000):
        self.file_path = file_path
        self.chunksize = chunksize
        self.sheets_info = {}
        self.all_data_chunks = []
        
    def read_sheets_info(self):
        """Read only sheet names and basic info without loading data"""
        try:
            print("Reading sheet information...")
            excel_file = pd.ExcelFile(self.file_path)
            sheet_names = excel_file.sheet_names[:5]
            
            for i, sheet_name in enumerate(sheet_names, 1):
                # Read just the first row to get headers
                first_row = pd.read_excel(self.file_path, sheet_name=sheet_name, nrows=1)
                total_rows = self._get_sheet_row_count(sheet_name)
                
                self.sheets_info[f'Sheet{i}'] = {
                    'original_name': sheet_name,
                    'headers': list(first_row.columns),
                    'total_rows': total_rows,
                    'dtypes': first_row.dtypes.to_dict()
                }
                print(f"✓ Sheet {i} ('{sheet_name}'): {total_rows} rows, {len(first_row.columns)} columns")
                
            return True
        except Exception as e:
            print(f"Error reading file: {e}")
            return False
    
    def _get_sheet_row_count(self, sheet_name):
        """Get row count without loading entire sheet"""
        try:
            # Read only the first column to count rows efficiently
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, usecols=[0])
            return len(df)
        except Exception as e:
            print(f"Warning: Could not get row count for {sheet_name}: {e}")
            return "Unknown"
    
    def display_headers(self):
        """Display headers for mapping reference"""
        print("\n" + "="*80)
        print("HEADERS FOR MAPPING REFERENCE")
        print("="*80)
        
        for sheet_key, sheet_info in self.sheets_info.items():
            print(f"\n{sheet_key} ('{sheet_info['original_name']}') - {sheet_info['total_rows']} rows:")
            print("-" * 50)
            for i, header in enumerate(sheet_info['headers']):
                print(f"  {i+1:2d}. {header}")

    def read_excel_in_chunks(self, sheet_name, chunk_size=1000):
        """Generator to read Excel file in chunks manually"""
        try:
            # First, get total rows
            total_rows = self.sheets_info[f'Sheet{list(self.sheets_info.keys()).index([k for k in self.sheets_info.keys() if self.sheets_info[k]["original_name"] == sheet_name][0]) + 1}']['total_rows']
            
            if total_rows == "Unknown":
                # If we don't know the row count, read all and yield as single chunk
                print(f"  Reading entire sheet at once (row count unknown)...")
                df = pd.read_excel(self.file_path, sheet_name=sheet_name)
                yield df
                return
            
            # Read in chunks
            for start_row in range(0, total_rows, chunk_size):
                end_row = min(start_row + chunk_size, total_rows)
                print(f"  Reading rows {start_row} to {end_row-1}...")
                
                # Use skiprows and nrows to read chunks
                df_chunk = pd.read_excel(
                    self.file_path, 
                    sheet_name=sheet_name,
                    skiprows=range(1, start_row + 1),  # +1 to skip header
                    nrows=chunk_size
                )
                
                if not df_chunk.empty:
                    yield df_chunk
                else:
                    break
                    
        except Exception as e:
            print(f"Error reading chunks from {sheet_name}: {e}")
            # Fallback: read entire sheet
            try:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name)
                yield df
            except Exception as e2:
                print(f"Failed to read sheet {sheet_name}: {e2}")

    # COMPLETE MAPPING FUNCTIONS FOR ALL 5 SHEETS
    def map_sheet1(self, row):
        """Mapping for therapyfinder_therapy sheet"""
        return {
            'ID': str(row.get('NPI', '')) if pd.notna(row.get('NPI')) else '',
            'Url': str(row.get('Url', '')) if pd.notna(row.get('Url')) else '',
            'Name': str(row.get('Name', '')) if pd.notna(row.get('Name')) else '',
            'Profession': str(row.get('Profession', '')) if pd.notna(row.get('Profession')) else '',
            'Clinic Name': str(row.get('Clinic Name', '')) if pd.notna(row.get('Clinic Name')) else '',
            'Bio': str(row.get('Bio', '')) if pd.notna(row.get('Bio')) else '',
            'Additional Focus Areas': str(row.get('Additional Focus Areas', '')) if pd.notna(row.get('Additional Focus Areas')) else '',
            'Treatment Approaches': str(row.get('Treatment Approaches', '')) if pd.notna(row.get('Treatment Approaches')) else '',
            'Appointment Types': str(row.get('Appointment Types', '')) if pd.notna(row.get('Appointment Types')) else '',
            'Communities': str(row.get('Communities', '')) if pd.notna(row.get('Communities')) else '',
            'Age Groups': str(row.get('Age Groups', '')) if pd.notna(row.get('Age Groups')) else '',
            'Languages': str(row.get('Languages', '')) if pd.notna(row.get('Languages')) else '',
            'Highlights': str(row.get('Highlights', '')) if pd.notna(row.get('Highlights')) else '',
            'Gender': str(row.get('Gender', '')) if pd.notna(row.get('Gender')) else '',
            'Pronouns': str(row.get('Pronouns', '')) if pd.notna(row.get('Pronouns')) else '',
            'Race Ethnicity': str(row.get('Race Ethnicity', '')) if pd.notna(row.get('Race Ethnicity')) else '',
            'Licenses': str(row.get('Licenses', '')) if pd.notna(row.get('Licenses')) else '',
            'Locations': str(row.get('Locations', '')) if pd.notna(row.get('Locations')) else '',
            'Education': str(row.get('Education', '')) if pd.notna(row.get('Education')) else '',
            'Faiths': str(row.get('Faiths', '')) if pd.notna(row.get('Faiths')) else '',
            'Min Session Price': str(row.get('Min Session Price', '')) if pd.notna(row.get('Min Session Price')) else '',
            'Max Session Price': str(row.get('Max Session Price', '')) if pd.notna(row.get('Max Session Price')) else '',
            'Pay Out Of Pocket Status': str(row.get('Pay Out Of Pocket Status', '')) if pd.notna(row.get('Pay Out Of Pocket Status')) else '',
            'Individual Service Rates': str(row.get('Individual Service Rates', '')) if pd.notna(row.get('Individual Service Rates')) else '',
            'General Payment Options': str(row.get('General Payment Options', '')) if pd.notna(row.get('General Payment Options')) else '',
            'Booking Summary': str(row.get('Booking Summary', '')) if pd.notna(row.get('Booking Summary')) else '',
            'Booking Url': str(row.get('Booking Url', '')) if pd.notna(row.get('Booking Url')) else '',
            'Listed In States': str(row.get('Listed In States', '')) if pd.notna(row.get('Listed In States')) else '',
            'States': str(row.get('States', '')) if pd.notna(row.get('States')) else '',
            'Listed In Websites': str(row.get('Listed In Websites', '')) if pd.notna(row.get('Listed In Websites')) else '',
            'Urls': str(row.get('Urls', '')) if pd.notna(row.get('Urls')) else '',
            'Connect Link - Facebook': str(row.get('Connect Link - Facebook', '')) if pd.notna(row.get('Connect Link - Facebook')) else '',
            'Connect Link - Instagram': str(row.get('Connect Link - Instagram', '')) if pd.notna(row.get('Connect Link - Instagram')) else '',
            'Connect Link - LinkedIn': str(row.get('Connect Link - LinkedIn', '')) if pd.notna(row.get('Connect Link - LinkedIn')) else '',
            'Connect Link - Twitter': str(row.get('Connect Link - Twitter', '')) if pd.notna(row.get('Connect Link - Twitter')) else '',
            'Connect Link - Website': str(row.get('Connect Link - Website', '')) if pd.notna(row.get('Connect Link - Website')) else '',
            'Main Specialties': str(row.get('Main Specialties', '')) if pd.notna(row.get('Main Specialties')) else '',
            'Accepted IPs': str(row.get('Accepted IPs', '')) if pd.notna(row.get('Accepted IPs')) else '',
            'Sr. NO': str(row.get('Sr. NO', '')) if pd.notna(row.get('Sr. NO')) else '',
            'Source': 'therapyfinder_therapy'
        }
    
    def map_sheet2(self, row):
        """Mapping for headway_therapy sheet"""
        return {
            'ID': str(row.get('NPI', '')) if pd.notna(row.get('NPI')) else '',
            'Url': str(row.get('Url', '')) if pd.notna(row.get('Url')) else '',
            'Name': str(row.get('Name', '')) if pd.notna(row.get('Name')) else '',
            'Profession': str(row.get('Profession', '')) if pd.notna(row.get('Profession')) else '',
            'Clinic Name': str(row.get('Clinic Name', '')) if pd.notna(row.get('Clinic Name')) else '',
            'Bio': str(row.get('Bio', '')) if pd.notna(row.get('Bio')) else '',
            'Additional Focus Areas': str(row.get('Additional Focus Areas', '')) if pd.notna(row.get('Additional Focus Areas')) else '',
            'Treatment Approaches': str(row.get('Treatment Approaches', '')) if pd.notna(row.get('Treatment Approaches')) else '',
            'Appointment Types': str(row.get('Appointment Types', '')) if pd.notna(row.get('Appointment Types')) else '',
            'Communities': str(row.get('Communities', '')) if pd.notna(row.get('Communities')) else '',
            'Age Groups': str(row.get('Age Groups', '')) if pd.notna(row.get('Age Groups')) else '',
            'Languages': str(row.get('Languages', '')) if pd.notna(row.get('Languages')) else '',
            'Highlights': str(row.get('Highlights', '')) if pd.notna(row.get('Highlights')) else '',
            'Gender': str(row.get('Gender', '')) if pd.notna(row.get('Gender')) else '',
            'Pronouns': str(row.get('Pronouns', '')) if pd.notna(row.get('Pronouns')) else '',
            'Race Ethnicity': str(row.get('Race Ethnicity', '')) if pd.notna(row.get('Race Ethnicity')) else '',
            'Licenses': str(row.get('Licenses', '')) if pd.notna(row.get('Licenses')) else '',
            'Locations': str(row.get('Locations', '')) if pd.notna(row.get('Locations')) else '',
            'Education': str(row.get('Education', '')) if pd.notna(row.get('Education')) else '',
            'Faiths': str(row.get('Faiths', '')) if pd.notna(row.get('Faiths')) else '',
            'Min Session Price': str(row.get('Min Session Price', '')) if pd.notna(row.get('Min Session Price')) else '',
            'Max Session Price': str(row.get('Max Session Price', '')) if pd.notna(row.get('Max Session Price')) else '',
            'Pay Out Of Pocket Status': str(row.get('Pay Out Of Pocket Status', '')) if pd.notna(row.get('Pay Out Of Pocket Status')) else '',
            'Individual Service Rates': str(row.get('Individual Service Rates', '')) if pd.notna(row.get('Individual Service Rates')) else '',
            'General Payment Options': str(row.get('General Payment Options', '')) if pd.notna(row.get('General Payment Options')) else '',
            'Booking Summary': str(row.get('Booking Summary', '')) if pd.notna(row.get('Booking Summary')) else '',
            'Booking Url': str(row.get('Booking Url', '')) if pd.notna(row.get('Booking Url')) else '',
            'Listed In States': str(row.get('Listed In States', '')) if pd.notna(row.get('Listed In States')) else '',
            'States': str(row.get('States', '')) if pd.notna(row.get('States')) else '',
            'Listed In Websites': str(row.get('Listed In Websites', '')) if pd.notna(row.get('Listed In Websites')) else '',
            'Urls': str(row.get('Urls', '')) if pd.notna(row.get('Urls')) else '',
            'Connect Link - Facebook': str(row.get('Connect Link - Facebook', '')) if pd.notna(row.get('Connect Link - Facebook')) else '',
            'Connect Link - Instagram': str(row.get('Connect Link - Instagram', '')) if pd.notna(row.get('Connect Link - Instagram')) else '',
            'Connect Link - LinkedIn': str(row.get('Connect Link - LinkedIn', '')) if pd.notna(row.get('Connect Link - LinkedIn')) else '',
            'Connect Link - Twitter': str(row.get('Connect Link - Twitter', '')) if pd.notna(row.get('Connect Link - Twitter')) else '',
            'Connect Link - Website': str(row.get('Connect Link - Website', '')) if pd.notna(row.get('Connect Link - Website')) else '',
            'Main Specialties': str(row.get('Main Specialties', '')) if pd.notna(row.get('Main Specialties')) else '',
            'Accepted IPs': str(row.get('Accepted IPs', '')) if pd.notna(row.get('Accepted IPs')) else '',
            'Sr. NO': str(row.get('Sr. NO', '')) if pd.notna(row.get('Sr. NO')) else '',
            'Source': 'headway_therapy'
        }
    
    def map_sheet3(self, row):
        """Mapping for alma_therapy sheet"""
        return {
            'ID': str(row.get('NPI', '')) if pd.notna(row.get('NPI')) else '',
            'Url': str(row.get('Url', '')) if pd.notna(row.get('Url')) else '',
            'Name': str(row.get('Name', '')) if pd.notna(row.get('Name')) else '',
            'Profession': str(row.get('Profession', '')) if pd.notna(row.get('Profession')) else '',
            'Clinic Name': str(row.get('Clinic Name', '')) if pd.notna(row.get('Clinic Name')) else '',
            'Bio': str(row.get('Bio', '')) if pd.notna(row.get('Bio')) else '',
            'Additional Focus Areas': str(row.get('Additional Focus Areas', '')) if pd.notna(row.get('Additional Focus Areas')) else '',
            'Treatment Approaches': str(row.get('Treatment Approaches', '')) if pd.notna(row.get('Treatment Approaches')) else '',
            'Appointment Types': str(row.get('Appointment Types', '')) if pd.notna(row.get('Appointment Types')) else '',
            'Communities': str(row.get('Communities', '')) if pd.notna(row.get('Communities')) else '',
            'Age Groups': str(row.get('Age Groups', '')) if pd.notna(row.get('Age Groups')) else '',
            'Languages': str(row.get('Languages', '')) if pd.notna(row.get('Languages')) else '',
            'Highlights': str(row.get('Highlights', '')) if pd.notna(row.get('Highlights')) else '',
            'Gender': str(row.get('Gender', '')) if pd.notna(row.get('Gender')) else '',
            'Pronouns': str(row.get('Pronouns', '')) if pd.notna(row.get('Pronouns')) else '',
            'Race Ethnicity': str(row.get('Race Ethnicity', '')) if pd.notna(row.get('Race Ethnicity')) else '',
            'Licenses': str(row.get('Licenses', '')) if pd.notna(row.get('Licenses')) else '',
            'Locations': str(row.get('Locations', '')) if pd.notna(row.get('Locations')) else '',
            'Education': str(row.get('Education', '')) if pd.notna(row.get('Education')) else '',
            'Faiths': str(row.get('Faiths', '')) if pd.notna(row.get('Faiths')) else '',
            'Min Session Price': str(row.get('Min Session Price', '')) if pd.notna(row.get('Min Session Price')) else '',
            'Max Session Price': str(row.get('Max Session Price', '')) if pd.notna(row.get('Max Session Price')) else '',
            'Pay Out Of Pocket Status': str(row.get('Pay Out Of Pocket Status', '')) if pd.notna(row.get('Pay Out Of Pocket Status')) else '',
            'Individual Service Rates': str(row.get('Individual Service Rates', '')) if pd.notna(row.get('Individual Service Rates')) else '',
            'General Payment Options': str(row.get('General Payment Options', '')) if pd.notna(row.get('General Payment Options')) else '',
            'Booking Summary': str(row.get('Booking Summary', '')) if pd.notna(row.get('Booking Summary')) else '',
            'Booking Url': str(row.get('Booking Url', '')) if pd.notna(row.get('Booking Url')) else '',
            'Listed In States': str(row.get('Listed In States', '')) if pd.notna(row.get('Listed In States')) else '',
            'States': str(row.get('States', '')) if pd.notna(row.get('States')) else '',
            'Listed In Websites': str(row.get('Listed In Websites', '')) if pd.notna(row.get('Listed In Websites')) else '',
            'Urls': str(row.get('Urls', '')) if pd.notna(row.get('Urls')) else '',
            'Connect Link - Facebook': str(row.get('Connect Link - Facebook', '')) if pd.notna(row.get('Connect Link - Facebook')) else '',
            'Connect Link - Instagram': str(row.get('Connect Link - Instagram', '')) if pd.notna(row.get('Connect Link - Instagram')) else '',
            'Connect Link - LinkedIn': str(row.get('Connect Link - LinkedIn', '')) if pd.notna(row.get('Connect Link - LinkedIn')) else '',
            'Connect Link - Twitter': str(row.get('Connect Link - Twitter', '')) if pd.notna(row.get('Connect Link - Twitter')) else '',
            'Connect Link - Website': str(row.get('Connect Link - Website', '')) if pd.notna(row.get('Connect Link - Website')) else '',
            'Main Specialties': str(row.get('Main Specialties', '')) if pd.notna(row.get('Main Specialties')) else '',
            'Accepted IPs': str(row.get('Accepted IPs', '')) if pd.notna(row.get('Accepted IPs')) else '',
            'Sr. NO': str(row.get('Sr. NO', '')) if pd.notna(row.get('Sr. NO')) else '',
            'Source': 'alma_therapy'
        }
    
    def map_sheet4(self, row):
        """Mapping for rula_therapy sheet"""
        return {
            'ID': str(row.get('NPI Number', '')) if pd.notna(row.get('NPI Number')) else '',
            'Url': str(row.get('Url', '')) if pd.notna(row.get('Url')) else '',
            'Name': str(row.get('Name', '')) if pd.notna(row.get('Name')) else '',
            'Profession': str(row.get('Profession', '')) if pd.notna(row.get('Profession')) else '',
            'Clinic Name': str(row.get('Clinic Name', '')) if pd.notna(row.get('Clinic Name')) else '',
            'Bio': str(row.get('Bio', '')) if pd.notna(row.get('Bio')) else '',
            'Additional Focus Areas': str(row.get('Additional Focus Areas', '')) if pd.notna(row.get('Additional Focus Areas')) else '',
            'Treatment Approaches': str(row.get('Treatment Approaches', '')) if pd.notna(row.get('Treatment Approaches')) else '',
            'Appointment Types': str(row.get('Appointment Types', '')) if pd.notna(row.get('Appointment Types')) else '',
            'Communities': str(row.get('Communities', '')) if pd.notna(row.get('Communities')) else '',
            'Age Groups': str(row.get('Age Groups', '')) if pd.notna(row.get('Age Groups')) else '',
            'Languages': str(row.get('Languages', '')) if pd.notna(row.get('Languages')) else '',
            'Highlights': str(row.get('Highlights', '')) if pd.notna(row.get('Highlights')) else '',
            'Gender': str(row.get('Gender', '')) if pd.notna(row.get('Gender')) else '',
            'Pronouns': str(row.get('Pronouns', '')) if pd.notna(row.get('Pronouns')) else '',
            'Race Ethnicity': str(row.get('Race Ethnicity', '')) if pd.notna(row.get('Race Ethnicity')) else '',
            'Licenses': str(row.get('Licenses', '')) if pd.notna(row.get('Licenses')) else '',
            'Locations': str(row.get('Locations', '')) if pd.notna(row.get('Locations')) else '',
            'Education': str(row.get('Education', '')) if pd.notna(row.get('Education')) else '',
            'Faiths': str(row.get('Faiths', '')) if pd.notna(row.get('Faiths')) else '',
            'Min Session Price': str(row.get('Min Session Price', '')) if pd.notna(row.get('Min Session Price')) else '',
            'Max Session Price': str(row.get('Max Session Price', '')) if pd.notna(row.get('Max Session Price')) else '',
            'Pay Out Of Pocket Status': str(row.get('Pay Out Of Pocket Status', '')) if pd.notna(row.get('Pay Out Of Pocket Status')) else '',
            'Individual Service Rates': str(row.get('Individual Service Rates', '')) if pd.notna(row.get('Individual Service Rates')) else '',
            'General Payment Options': str(row.get('General Payment Options', '')) if pd.notna(row.get('General Payment Options')) else '',
            'Booking Summary': str(row.get('Booking Summary', '')) if pd.notna(row.get('Booking Summary')) else '',
            'Booking Url': str(row.get('Booking Url', '')) if pd.notna(row.get('Booking Url')) else '',
            'Listed In States': str(row.get('Listed In States', '')) if pd.notna(row.get('Listed In States')) else '',
            'States': str(row.get('States', '')) if pd.notna(row.get('States')) else '',
            'Listed In Websites': str(row.get('Listed In Websites', '')) if pd.notna(row.get('Listed In Websites')) else '',
            'Urls': str(row.get('Urls', '')) if pd.notna(row.get('Urls')) else '',
            'Connect Link - Facebook': str(row.get('Connect Link - Facebook', '')) if pd.notna(row.get('Connect Link - Facebook')) else '',
            'Connect Link - Instagram': str(row.get('Connect Link - Instagram', '')) if pd.notna(row.get('Connect Link - Instagram')) else '',
            'Connect Link - LinkedIn': str(row.get('Connect Link - LinkedIn', '')) if pd.notna(row.get('Connect Link - LinkedIn')) else '',
            'Connect Link - Twitter': str(row.get('Connect Link - Twitter', '')) if pd.notna(row.get('Connect Link - Twitter')) else '',
            'Connect Link - Website': str(row.get('Connect Link - Website', '')) if pd.notna(row.get('Connect Link - Website')) else '',
            'Main Specialties': str(row.get('Main Specialties', '')) if pd.notna(row.get('Main Specialties')) else '',
            'Accepted IPs': str(row.get('Accepted IPs', '')) if pd.notna(row.get('Accepted IPs')) else '',
            'Sr. NO': str(row.get('Sr. NO', '')) if pd.notna(row.get('Sr. NO')) else '',
            'Source': 'rula_therapy'
        }
    
    def map_sheet5(self, row):
        """Mapping for the fifth sheet"""
        return {
            'ID': str(row.get('NPI', '')),
            'Url': str(row.get('Url', '')) if pd.notna(row.get('Url')) else '',
            'Name': str(row.get('Name', '')) if pd.notna(row.get('Name')) else '',
            'Profession': str(row.get('Profession', '')) if pd.notna(row.get('Profession')) else '',
            'Clinic Name': str(row.get('Clinic Name', '')) if pd.notna(row.get('Clinic Name')) else '',
            'Bio': str(row.get('Bio', '')) if pd.notna(row.get('Bio')) else '',
            'Additional Focus Areas': str(row.get('Additional Focus Areas', '')) if pd.notna(row.get('Additional Focus Areas')) else '',
            'Treatment Approaches': str(row.get('Treatment Approaches', '')) if pd.notna(row.get('Treatment Approaches')) else '',
            'Appointment Types': str(row.get('Appointment Types', '')) if pd.notna(row.get('Appointment Types')) else '',
            'Communities': str(row.get('Communities', '')) if pd.notna(row.get('Communities')) else '',
            'Age Groups': str(row.get('Age Groups', '')) if pd.notna(row.get('Age Groups')) else '',
            'Languages': str(row.get('Languages', '')) if pd.notna(row.get('Languages')) else '',
            'Highlights': str(row.get('Highlights', '')) if pd.notna(row.get('Highlights')) else '',
            'Gender': str(row.get('Gender', '')) if pd.notna(row.get('Gender')) else '',
            'Pronouns': str(row.get('Pronouns', '')) if pd.notna(row.get('Pronouns')) else '',
            'Race Ethnicity': str(row.get('Race Ethnicity', '')) if pd.notna(row.get('Race Ethnicity')) else '',
            'Licenses': str(row.get('Licenses', '')) if pd.notna(row.get('Licenses')) else '',
            'Locations': str(row.get('Locations', '')) if pd.notna(row.get('Locations')) else '',
            'Education': str(row.get('Education', '')) if pd.notna(row.get('Education')) else '',
            'Faiths': str(row.get('Faiths', '')) if pd.notna(row.get('Faiths')) else '',
            'Min Session Price': str(row.get('Min Session Price', '')) if pd.notna(row.get('Min Session Price')) else '',
            'Max Session Price': str(row.get('Max Session Price', '')) if pd.notna(row.get('Max Session Price')) else '',
            'Pay Out Of Pocket Status': str(row.get('Pay Out Of Pocket Status', '')) if pd.notna(row.get('Pay Out Of Pocket Status')) else '',
            'Individual Service Rates': str(row.get('Individual Service Rates', '')) if pd.notna(row.get('Individual Service Rates')) else '',
            'General Payment Options': str(row.get('General Payment Options', '')) if pd.notna(row.get('General Payment Options')) else '',
            'Booking Summary': str(row.get('Booking Summary', '')) if pd.notna(row.get('Booking Summary')) else '',
            'Booking Url': str(row.get('Booking Url', '')) if pd.notna(row.get('Booking Url')) else '',
            'Listed In States': str(row.get('Listed In States', '')) if pd.notna(row.get('Listed In States')) else '',
            'States': str(row.get('States', '')) if pd.notna(row.get('States')) else '',
            'Listed In Websites': str(row.get('Listed In Websites', '')) if pd.notna(row.get('Listed In Websites')) else '',
            'Urls': str(row.get('Urls', '')) if pd.notna(row.get('Urls')) else '',
            'Connect Link - Facebook': str(row.get('Connect Link - Facebook', '')) if pd.notna(row.get('Connect Link - Facebook')) else '',
            'Connect Link - Instagram': str(row.get('Connect Link - Instagram', '')) if pd.notna(row.get('Connect Link - Instagram')) else '',
            'Connect Link - LinkedIn': str(row.get('Connect Link - LinkedIn', '')) if pd.notna(row.get('Connect Link - LinkedIn')) else '',
            'Connect Link - Twitter': str(row.get('Connect Link - Twitter', '')) if pd.notna(row.get('Connect Link - Twitter')) else '',
            'Connect Link - Website': str(row.get('Connect Link - Website', '')) if pd.notna(row.get('Connect Link - Website')) else '',
            'Main Specialties': str(row.get('Main Specialties', '')) if pd.notna(row.get('Main Specialties')) else '',
            'Accepted IPs': str(row.get('Accepted IPs', '')) if pd.notna(row.get('Accepted IPs')) else '',
            'Sr. NO': str(row.get('Sr. NO', '')) if pd.notna(row.get('Sr. NO')) else '',
            'Source': 'sheet5'
        }
    
    def process_sheets_chunked(self):
        """Process all sheets in chunks to save memory"""
        # Map sheet names to functions
        sheet_mapping = {
            'therapyfinder_therapy': self.map_sheet1,
            'headway_therapy': self.map_sheet2, 
            'alma_therapy': self.map_sheet3,
            'rula_therapy': self.map_sheet4,
        }
        
        analysis_data = []
        total_processed = 0
        
        for sheet_key, sheet_info in self.sheets_info.items():
            sheet_name = sheet_info['original_name']
            
            # Get the appropriate mapping function
            map_function = None
            for map_key, func in sheet_mapping.items():
                if map_key in sheet_name.lower():
                    map_function = func
                    break
            
            if not map_function:
                print(f"⚠️  No specific mapping found for {sheet_name}, using default")
                map_function = self.map_sheet5
            
            sheet_total = 0
            print(f"\nProcessing {sheet_key} ({sheet_name}) in chunks...")
            
            # Process sheet in chunks using our custom chunk reader
            chunk_number = 0
            for chunk_df in self.read_excel_in_chunks(sheet_name, self.chunksize):
                chunk_number += 1
                chunk_processed = 0
                chunk_mapped_data = []
                
                for index, row in chunk_df.iterrows():
                    row_dict = row.to_dict()
                    mapped_row = map_function(row_dict)
                    chunk_mapped_data.append(mapped_row)
                    chunk_processed += 1
                
                # Store chunk and free memory
                if chunk_mapped_data:
                    chunk_df_mapped = pd.DataFrame(chunk_mapped_data)
                    self.all_data_chunks.append(chunk_df_mapped)
                    sheet_total += chunk_processed
                    total_processed += chunk_processed
                    
                    # Clear memory
                    del chunk_mapped_data, chunk_df
                    gc.collect()
                
                print(f"  Chunk {chunk_number}: {chunk_processed} records processed")
            
            analysis_data.append({
                'Sheet Name': sheet_key,
                'Original Name': sheet_name,
                'Record Count': sheet_total,
                'Column Count': len(sheet_info['headers']),
                'Status': 'Completed',
                'Processing Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            print(f"✓ {sheet_key}: Completed {sheet_total} records")
        
        # Add total to analysis
        analysis_data.append({
            'Sheet Name': 'TOTAL',
            'Original Name': 'All Sheets Combined',
            'Record Count': total_processed,
            'Column Count': len(self.all_data_chunks[0].columns) if self.all_data_chunks else 0,
            'Status': 'All Data Consolidated',
            'Processing Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        self.analysis_data = analysis_data
        print(f"\n✓ Total records processed: {total_processed:,}")
        
        return total_processed
    
    def create_final_output(self, output_path):
        """Create final output with memory efficiency"""
        try:
            print(f"\nConsolidating {len(self.all_data_chunks)} chunks into final output...")
            
            if self.all_data_chunks:
                # Use efficient concatenation
                final_data = pd.concat(self.all_data_chunks, ignore_index=True)
                
                # Create analysis DataFrame
                analysis_df = pd.DataFrame(self.analysis_data)
                
                # Export to Excel
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    final_data.to_excel(writer, sheet_name='all_data', index=False)
                    analysis_df.to_excel(writer, sheet_name='analysis', index=False)
                
                print(f"✓ Successfully exported:")
                print(f"   - 'all_data' sheet: {len(final_data):,} records")
                print(f"   - 'analysis' sheet: {len(analysis_df)} summary rows")
                
                # Memory cleanup
                del final_data
                gc.collect()
                
                return True
            else:
                print("No data to export.")
                return False
                
        except Exception as e:
            print(f"Error exporting file: {e}")
            return False

# USAGE
def main():
    # Initialize with your file path
    file_path = "./final_data.xlsx"  # REPLACE WITH YOUR FILE PATH
    
    # Use smaller chunksize for very large files
    processor = LargeExcelProcessor(file_path, chunksize=1000)
    
    # Step 1: Read sheet info (memory safe)
    print("Initializing large file processor...")
    if not processor.read_sheets_info():
        return
    
    # Step 2: Display headers for reference
    processor.display_headers()
    
    # Step 3: Process in chunks (memory safe)
    print("\n" + "="*80)
    print("PROCESSING DATA IN CHUNKS (Memory Safe)")
    print("="*80)
    
    total_records = processor.process_sheets_chunked()
    
    # Step 4: Export final result
    print("\n" + "="*80)
    print("EXPORTING FINAL RESULTS")
    print("="*80)
    
    output_path = f"consolidated_therapy_data_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    success = processor.create_final_output(output_path)
    
    print("\n" + "="*80)
    print("PROCESSING COMPLETE - MEMORY SAFE")
    print("="*80)
    if success:
        print(f"✓ Total records processed: {total_records:,}")
        print(f"✓ Output file: {output_path}")
        print(f"✓ Memory usage optimized for large files")
    else:
        print("❌ Processing failed")

if __name__ == "__main__":
    main()
