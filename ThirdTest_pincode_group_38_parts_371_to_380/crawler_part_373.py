"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 373 / 400
================================================================================
- Group: ThirdTest_pincode_group_38_parts_371_to_380
- Assigned PIN Codes: 48 (Range: 797115 to 799104)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_373.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_373.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_373"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-373] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "797115",
  "797116",
  "797117",
  "797118",
  "797120",
  "797121",
  "798601",
  "798602",
  "798603",
  "798604",
  "798607",
  "798611",
  "798612",
  "798613",
  "798614",
  "798615",
  "798616",
  "798618",
  "798619",
  "798620",
  "798621",
  "798622",
  "798623",
  "798625",
  "798627",
  "799001",
  "799002",
  "799003",
  "799004",
  "799005",
  "799006",
  "799007",
  "799008",
  "799009",
  "799010",
  "799011",
  "799012",
  "799013",
  "799014",
  "799015",
  "799022",
  "799035",
  "799045",
  "799046",
  "799101",
  "799102",
  "799103",
  "799104"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "797115": {
    "pincode": "797115",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Artc S.O",
      "Aoyimti B.O",
      "Daroga Pathar B.O",
      "Diphupar B.O",
      "Eralibill B.O",
      "Showba Old B.O",
      "Sugar Mill Project B.O",
      "Sukhovi B.O",
      "Ikishe B.O"
    ]
  },
  "797116": {
    "pincode": "797116",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Dimapur Bazar S.O",
      "Khehekhu B.O",
      "Kuhuboto B.O",
      "Kushiabill B.O",
      "Padampukhuri B.O",
      "Vihokhu B.O"
    ]
  },
  "797117": {
    "pincode": "797117",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Circular Road S.O",
      "Singal Village B.O",
      "Pwd Colony B.O",
      "Zeliangrong B.O"
    ]
  },
  "797118": {
    "pincode": "797118",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Signal Village",
      "Rangapahar B.O",
      "Thahekhu B.O"
    ]
  },
  "797120": {
    "pincode": "797120",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "P.R.Hill S.O",
      "Lalmati B.O",
      "Phesama B.O",
      "Zubza B.O"
    ]
  },
  "797121": {
    "pincode": "797121",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "High School Junction S.O"
    ]
  },
  "798601": {
    "pincode": "798601",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Mokokchung S.O",
      "Aolijen B.O",
      "Arree Old B.O",
      "Changki B.O",
      "Chare B.O",
      "Chungtia B.O",
      "Fazl Ali College B.O",
      "Kubza B.O",
      "Longkhum B.O",
      "Longmisa B.O",
      "Longsa B.O",
      "Mangmentong B.O",
      "Nst Colony B.O",
      "S. E. Colony B.O",
      "Tronger B.O",
      "Ungma B.O",
      "V. K. Town B.O",
      "Ongpangkong B.O",
      "Khensa B.O"
    ]
  },
  "798602": {
    "pincode": "798602",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Tizit S.O",
      "Namsa B.O",
      "Yannu B.O",
      "Zaboka B.O",
      "Zangkham B.O"
    ]
  },
  "798603": {
    "pincode": "798603",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Champang S.O",
      "Angphang B.O",
      "Chenloisho B.O",
      "Chenwetnyu B.O",
      "Chingkao B.O",
      "Chinglong B.O",
      "Jakpang B.O",
      "Longching B.O",
      "Monyakshu B.O",
      "Pessao B.O",
      "Tobu B.O",
      "Ukha B.O"
    ]
  },
  "798604": {
    "pincode": "798604",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Mongkholemba S.O",
      "Alungkima B.O",
      "Alungtaki B.O",
      "Chungtia Yimsen B.O",
      "Khari B.O",
      "Longchem B.O",
      "Longnak B.O",
      "Mongchen B.O",
      "Waromung B.O"
    ]
  },
  "798607": {
    "pincode": "798607",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Alichen S.O"
    ]
  },
  "798611": {
    "pincode": "798611",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Kiphire S.O",
      "Amahatore B.O",
      "Anatongere B.O",
      "Kisethong B.O",
      "Longmatra B.O",
      "Moya B.O",
      "Phalonger B.O",
      "Pokhpur B.O",
      "Pungro B.O",
      "Seyochang B.O",
      "Sikur B.O",
      "Singrup B.O",
      "Sitimi B.O",
      "Phisami B.O"
    ]
  },
  "798612": {
    "pincode": "798612",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Tuensang S.O",
      "Yangpi B.O",
      "Noksen B.O",
      "Choklangan B.O",
      "Nokhu B.O",
      "Pangsha B.O",
      "Pessu B.O",
      "Sanglao B.O",
      "Chessore B.O",
      "Chingmei B.O",
      "Huker B.O",
      "Kuthur B.O",
      "Panso B.O",
      "Sangsanyu B.O",
      "Saramati B.O",
      "Shamator B.O",
      "Shampur B.O",
      "Shiponger B.O",
      "Sotokur B.O",
      "Thonokhunyu B.O",
      "Tuensang Village B.O",
      "Noklak B.O"
    ]
  },
  "798613": {
    "pincode": "798613",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Chantongia S.O",
      "Yaongyimsen B.O"
    ]
  },
  "798614": {
    "pincode": "798614",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Chuchuyimlang S.O",
      "Phangsang B.O",
      "Longkong B.O",
      "Mongsenyimti B.O",
      "Salulamang B.O",
      "Ungar B.O",
      "Yisemyong B.O"
    ]
  },
  "798615": {
    "pincode": "798615",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Impur S.O",
      "Kubulong B.O",
      "Longchang B.O",
      "Sungratsu B.O"
    ]
  },
  "798616": {
    "pincode": "798616",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Longkhim S.O",
      "Angangba B.O",
      "Chimonger B.O",
      "Chungtore B.O",
      "Yangli B.O"
    ]
  },
  "798618": {
    "pincode": "798618",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Merangkong B.O",
      "Tuli S.O",
      "Asangma B.O",
      "Barakangtsung B.O",
      "Luyang Valley B.O",
      "Molungkhimong B.O",
      "Molungyimsen B.O",
      "Tamlu B.O",
      "Wamaken B.O"
    ]
  },
  "798619": {
    "pincode": "798619",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Asukuto B.O"
    ]
  },
  "798620": {
    "pincode": "798620",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Akuhaito B.O",
      "Zunheboto S.O",
      "Aghunato B.O",
      "Asukhomi B.O",
      "Asuto B.O",
      "Atoizu B.O",
      "Hosephu B.O",
      "Kichilimi B.O",
      "Saghemi B.O",
      "Satakha B.O",
      "Satami B.O",
      "Satoi B.O",
      "Suruhuto B.O",
      "Surumi B.O",
      "Tokiye B.O"
    ]
  },
  "798621": {
    "pincode": "798621",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Mon S.O",
      "Chui B.O",
      "Longpong Senghe B.O",
      "Longwa B.O",
      "Pongkong B.O",
      "Singha Chingnyu B.O",
      "Tang B.O",
      "Totak B.O",
      "Waunching B.O",
      "Wakching B.O"
    ]
  },
  "798622": {
    "pincode": "798622",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Naginimora S.O",
      "Baranamsang B.O",
      "Kongon B.O"
    ]
  },
  "798623": {
    "pincode": "798623",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Paper Nagar S.O"
    ]
  },
  "798625": {
    "pincode": "798625",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Longleng S.O",
      "Hukpang B.O",
      "Nian B.O",
      "Phomching B.O",
      "Sakchi B.O",
      "Yachem B.O",
      "Yongyah B.O",
      "Bhumnyu BO"
    ]
  },
  "798627": {
    "pincode": "798627",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Nagaland Division",
    "offices": [
      "Aizuto B.O",
      "Lumami S.O",
      "Akuluto B.O"
    ]
  },
  "799001": {
    "pincode": "799001",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Agartala H.O",
      "Agartala Bazar S.O",
      "Agartala Court S.O",
      "Jaganath Bari Road S.O"
    ]
  },
  "799002": {
    "pincode": "799002",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Ramnagar S.O (West Tripura)",
      "AgartalaGuard Post B.O",
      "Barjala B.O",
      "Bhati Abhoynagar B.O",
      "Border Rampur B.O",
      "Paschim Bhubanban B.O",
      "Town Rampur B.O",
      "Krishnanagar B.O"
    ]
  },
  "799003": {
    "pincode": "799003",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Arundhutinagar S.O",
      "Badharghat B.O",
      "Bardowali B.O",
      "Bhattapukur B.O",
      "Charipara B.O",
      "Ishanchandranagar B.O",
      "Madhuban B.O",
      "Ranirkhamar B.O",
      "Siddhi Ashram B.O",
      "S.D.Mission Colony B.O"
    ]
  },
  "799004": {
    "pincode": "799004",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Anandanagar B.O",
      "Agartala College S.O",
      "Gabordi B.O",
      "Jogendranagar B.O",
      "Nagicherra B.O",
      "Purba Pratapgarh B.O",
      "Renters Colony B.O",
      "Srinagar Kacharipara B.O",
      "Town pratapgarh B.O",
      "Aralia B.O",
      "Dhupcherra B.O",
      "Jarul Bachai B.O"
    ]
  },
  "799005": {
    "pincode": "799005",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Abhoynagar S.O"
    ]
  },
  "799006": {
    "pincode": "799006",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Kunjaban S.O",
      "Bankumari E/SB B.O",
      "G.B.Hospital B.O",
      "Indranagar B.O",
      "Kathalbagan B.O",
      "Kunjabon Road B.O",
      "Nandannagar B.O",
      "Noagaon Krishnagar B.O"
    ]
  },
  "799007": {
    "pincode": "799007",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Dhaleswar S.O",
      "Banamalipur B.O"
    ]
  },
  "799008": {
    "pincode": "799008",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Khayerpur S.O",
      "Khasnoagaon B.O",
      "Mariamnagar B.O",
      "Neepco B.O",
      "Old Agartala B.O",
      "Paschim Champamur B.O",
      "Paschim Noabadi B.O",
      "Ramachandranagar B.O",
      "Reshambagan B.O"
    ]
  },
  "799009": {
    "pincode": "799009",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Durjaynagar B.O",
      "Lankamura B.O",
      "Agartala Aerodrome S.O",
      "Narayanpur B.O",
      "Nutannagar B.O",
      "Agartala Airport Building S.O"
    ]
  },
  "799010": {
    "pincode": "799010",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Agartala Secretariat S.O"
    ]
  },
  "799011": {
    "pincode": "799011",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Jampuijala SO",
      "Ujan Ghaniamara B.O",
      "Jampuijala Colony B.O",
      "Kanakraipara B.O",
      "Kendraicherra B.O",
      "Takarjala B.O",
      "Sankumabari B.O"
    ]
  },
  "799012": {
    "pincode": "799012",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Salbagan S.O",
      "Gandhigram B.O"
    ]
  },
  "799013": {
    "pincode": "799013",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Matabari S.O",
      "Abani Reangpara B.O",
      "Dakshin Chandrapur B.O",
      "Dakshin Maharani B.O",
      "Fulkumari B.O",
      "Holakhet B.O",
      "Jamjuri B.O",
      "Joalikhamar B.O",
      "Lakshipati B.O",
      "Maharani B.O",
      "Murapara B.O"
    ]
  },
  "799014": {
    "pincode": "799014",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Agartala ONGC S.O"
    ]
  },
  "799015": {
    "pincode": "799015",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Narayanpur Bazar SO",
      "Tebaria B.O"
    ]
  },
  "799022": {
    "pincode": "799022",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Tripura University S.O"
    ]
  },
  "799035": {
    "pincode": "799035",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Ranirbazar S.O",
      "Burakha Landless Colony B.O",
      "Dinabandhupara B.O",
      "Durganagar B.O",
      "Majlishpur B.O",
      "Meghlipara B.O",
      "Purba Noagaon B.O",
      "Sonamani sepaipara B.O",
      "Wakinagar B.O"
    ]
  },
  "799045": {
    "pincode": "799045",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Birendranagar S.O",
      "Ashigarh B.O",
      "Shantinagar B.O",
      "Belbari B.O",
      "Brigudaspara B.O",
      "Champaknagar B.O",
      "Gurudas Colony B.O",
      "Janmejoynagar B.O",
      "Mandainagar B.O",
      "Mdhabbari B.O",
      "Noabadi B.O",
      "Radhamohanpur B.O",
      "Sanchindranagar Colony B.O",
      "Tripura Engineering College B.O",
      "Bisrambari B.O."
    ]
  },
  "799046": {
    "pincode": "799046",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "NIT Agartala S.O"
    ]
  },
  "799101": {
    "pincode": "799101",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Amarpur S.O (South Tripura)",
      "Ampinagar B.O",
      "Bampur B.O",
      "Chechuajamatiabari B.O",
      "Chellagong B.O",
      "Dakshin taidu B.O",
      "Debbari B.O",
      "Dhanlekha B.O",
      "Gamakubari B.O",
      "Kurma B.O",
      "Mahuamillan B.O",
      "Malbassa B.O",
      "Nagrai B.O",
      "Paharpur B.O",
      "Palku B.O",
      "Rangamati B.O",
      "Rangkhang B.O",
      "Sarbong B.O",
      "Taidubari B.O"
    ]
  },
  "799102": {
    "pincode": "799102",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Bishalgarh S.O",
      "Boxonagar B.O",
      "Brajapur B.O",
      "Dhajanagar B.O",
      "Gazaria B.O",
      "Golaghati B.O",
      "Harishnagar T.E. B.O",
      "Jampuijala B.O",
      "Jampuijala Colony B.O",
      "Kaiyadhepa B.O",
      "Kalamcherra B.O",
      "Kalasimura B.O",
      "Kalkalia B.O",
      "Kamalasagar B.O",
      "Kanakraipara B.O",
      "Kendraicherra B.O",
      "Konabon B.O",
      "Krishnakishorenagar B.O",
      "Lalsingmura B.O",
      "Madhupur B.O",
      "Nabasantiganj Bazar B.O",
      "Nabinagar B.O",
      "No.2 Chandranagar B.O",
      "Purathalrajnagar B.O",
      "Purba Gokulnagar B.O",
      "Purba Laxmi Bill B.O",
      "Putia B.O",
      "Rohimpur B.O",
      "Rokhia G.T Project B.O",
      "Sankumabari B.O",
      "Sepahijala B.O",
      "Sutarmura B.O",
      "Takarjala B.O",
      "Ujan Ghaniamara B.O",
      "Veluarchar B.O",
      "South Nehalchandranagar B.O",
      "West Laxmibill B.O"
    ]
  },
  "799103": {
    "pincode": "799103",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Bishramganj S.O",
      "Amarendranagar B.O",
      "Bagmara B.O",
      "Barjala B.O",
      "Charilam B.O",
      "Chechrimile B.O",
      "Goliraibari B.O",
      "Latiacherra B.O",
      "Padmanagar B.O",
      "Pathaliaghat B.O",
      "Promodenagar B.O",
      "Ramnagar Bazar B.O",
      "Rangamala Bazar B.O",
      "South Charilam B.O",
      "Tekshapara B.O"
    ]
  },
  "799104": {
    "pincode": "799104",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Jatanbari S.O",
      "Barbil B.O",
      "Boalkhali B.O",
      "Dumburnagar B.O",
      "Ghorakappa B.O",
      "Jalaya B.O",
      "Karbook B.O",
      "Lebacherra B.O",
      "Paticherri B.O",
      "Purba Manikya Dewan B.O",
      "Raima B.O",
      "Silacharri B.O",
      "Suknacherri B.O",
      "Tirthamukh B.O",
      "Tuichama B.O",
      "Uttar Chellagonj B.O"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
