# Sales Data Analytics System

**Assignment Name**: Sales Data Analytics System  
**Purpose**: Build a complete Python application that reads messy sales transaction data, cleans and validates it, performs business analysis, integrates with an external API (DummyJSON), enriches the data, and generates a comprehensive report.

**Student name **: SUNIL  
**Student ID **: BITSoM_BA_25071994 
**Location**: Avadi, Tamil Nadu, India  
**Date**: January 2026

## Project Overview

This application performs the following main tasks:

1. Reads and handles encoding issues in `sales_data.txt` (pipe-delimited file)
2. Parses, cleans and validates sales records
3. Allows optional interactive filtering (region + min/max amount)
4. Performs various sales analytics (total revenue, region-wise, top products, top customers, daily trends, peak day, low performers)
5. Fetches product information from https://dummyjson.com/products API
6. Enriches transactions with category, brand, rating
7. Saves enriched data in pipe-delimited format
8. Generates a professional formatted text report with 8 required sections

## Features Implemented

- Multi-encoding file reading (utf-8, latin-1, cp1252)
- Cleaning: removes commas from product names & numbers, correct type conversion
- Validation: removes records with invalid IDs, negative/zero quantity/price, missing region/customer
- User interaction: shows regions & amount range, asks if filtering is needed
- Analytics: total revenue, region performance, top products/customers, daily trends, peak day, low performers
- API enrichment: adds API_Category, API_Brand, API_Rating, API_Match flag
- Report: 8 sections with tables, summaries, formatting

## Requirements

- **Python version**: 3.8 or higher
- **External library**: only `requests` (for DummyJSON API)
- **Internet connection**: required for API calls
- **Operating System**: Windows / macOS / Linux

### Install dependencies

```bash
pip install -r requirements.txt

How to Run the Application (Step-by-Step)
Option 1: Single-file version (recommended for submission)

Save the code as sales_analytics_all_in_one.py
Place your sales_data.txt file in the same folder
Open terminal / command prompt in that folder
Install requests (if not already installed)Bashpip install requests
Run the program:
python main.py
python sales_analytics_all_in_one.py

What happens when you run the program?

Reads and shows number of lines found in sales_data.txt
Parses records and shows count
Displays available regions and transaction amount range
Asks:
Do you want to apply filters? (y/n):
If yes → asks for region, min amount, max amount (you can press Enter to skip any)
Validates records and shows summary
Fetches products from DummyJSON API
Enriches transactions
Saves enriched file → data/enriched_sales_data.txt
Generates full report → output/sales_report.txt
Shows success message with file locations
