"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 371 / 400
================================================================================
- Group: ThirdTest_pincode_group_38_parts_371_to_380
- Assigned PIN Codes: 48 (Range: 795116 to 796161)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_371.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_371.csv & .json
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

PART_ID = "part_371"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-371] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "795116",
  "795117",
  "795118",
  "795122",
  "795124",
  "795125",
  "795126",
  "795127",
  "795128",
  "795129",
  "795130",
  "795131",
  "795132",
  "795133",
  "795134",
  "795135",
  "795136",
  "795138",
  "795139",
  "795140",
  "795141",
  "795142",
  "795144",
  "795145",
  "795146",
  "795147",
  "795148",
  "795149",
  "795150",
  "795159",
  "796001",
  "796004",
  "796005",
  "796007",
  "796008",
  "796009",
  "796012",
  "796014",
  "796015",
  "796017",
  "796036",
  "796070",
  "796075",
  "796081",
  "796091",
  "796101",
  "796111",
  "796161"
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
  "795116": {
    "pincode": "795116",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Jiribam Bazar S.O",
      "Bongmun B.O",
      "Borobekra B.O",
      "Champanagar B.O",
      "Chingmun B.O",
      "Jakurdhar B.O",
      "Kh Jaikhan B.O",
      "Latingkhal B.O",
      "Longpi B.O",
      "Patpuimun B.O",
      "Phaibok Mullen B.O",
      "Sonapur B.O",
      "Suangpuimun B.O",
      "Tuitengmun B.O",
      "Chongmun B.O"
    ]
  },
  "795117": {
    "pincode": "795117",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Saikot S.O",
      "Khouwpuibung B.O",
      "Mual Vaiphei B.O",
      "Tuikham B.O",
      "Tuining B.O",
      "Tuitengphai B.O"
    ]
  },
  "795118": {
    "pincode": "795118",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Saikul S.O",
      "Bunglong B.O",
      "Dolang B.O",
      "Galam B.O",
      "Gangpijang B.O",
      "Ichaigojang B.O",
      "Jangnoi B.O",
      "Makeng Ngarolu B.O",
      "Makokchung B.O",
      "Molkon B.O",
      "Mutukhong B.O",
      "Pangjang B.O",
      "Phaikon B.O",
      "Sangpei Khullen B.O",
      "T. Awkhumbung B.O",
      "Tingpibung B.O",
      "Zelengphai B.O"
    ]
  },
  "795122": {
    "pincode": "795122",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Kalapahar S.O",
      "Chandraman B.O",
      "Haipi B.O",
      "Keithelmanbi B.O",
      "Kheljang B.O",
      "Persian B.O",
      "Thanamba B.O",
      "Tokpa B.O"
    ]
  },
  "795124": {
    "pincode": "795124",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Loktak Project S.O",
      "Charoi Khullel B.O",
      "Jiban Nager B.O",
      "Leimatak B.O",
      "Mayuran B.O",
      "Sadukhoiroi B.O",
      "Tokpalamdan B.O"
    ]
  },
  "795125": {
    "pincode": "795125",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Tamei S.O",
      "Atang Khunou B.O",
      "Chaiton B.O",
      "Dullen (T/C/D) B.O",
      "Illong B.O",
      "Jampii (T/C/D) B.O",
      "Kadi B.O",
      "Kasanlong B.O",
      "Khumphung B.O",
      "Kuilong B.O",
      "Lemta B.O",
      "Lenglong B.O",
      "Magulong B.O",
      "New Lamlaba B.O",
      "Takou B.O",
      "Upper Selsi (T/c/d) B.O"
    ]
  },
  "795126": {
    "pincode": "795126",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Dolang B.O",
      "Dolang Khunou B.O",
      "Bishnupur S.O (Bishnupur)",
      "Khoijuman B.O",
      "Khongbung B.O",
      "Khunpi Naosem B.O",
      "Lamdangmei B.O",
      "Ngaikhong Khullen B.O",
      "Ngarian B.O",
      "Nungsai Chiru B.O",
      "Thangal B.O",
      "Toubul B.O",
      "Zouzangtek B.O",
      "Khoupum B.O"
    ]
  },
  "795127": {
    "pincode": "795127",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Chandel S.O",
      "Duthang B.O",
      "Larong B.O",
      "Liwasarai B.O",
      "Mittong B.O"
    ]
  },
  "795128": {
    "pincode": "795128",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Churachandpur S.O",
      "Aina B.O",
      "Chehjang B.O",
      "Chothemunpi B.O",
      "Geljang B.O",
      "Hamkeilon B.O",
      "Henglep B.O",
      "Kangvai Bazar B.O",
      "Khanpi B.O",
      "Kolhen B.O",
      "Kumbipukhri B.O",
      "Kwanpui B.O",
      "Lailong B.O",
      "Lungsai B.O",
      "Lungshung B.O",
      "Mission Compound B.O",
      "Munpi B.O",
      "Phaipheng B.O",
      "Saiden B.O",
      "Saikhul Village B.O",
      "Sangphou B.O",
      "Santing B.O",
      "Sielmet B.O",
      "South Kotlein B.O",
      "Takvom B.O",
      "Teiyong B.O",
      "Thangshi B.O",
      "Thinkew B.O",
      "Tollen B.O",
      "Tolphei B.O",
      "Tuinom B.O",
      "Tulaphei B.O",
      "Ukha B.O",
      "Kamkeilon B.O",
      "Chingkonpang S.O",
      "M.Songel B.O"
    ]
  },
  "795129": {
    "pincode": "795129",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Kangpokpi S.O",
      "Henbung B.O",
      "Kangpokpi Mission B.O",
      "Maohiing B.O",
      "Maohing B.O",
      "Taphou B.O",
      "Thonglang B.O",
      "Toribari B.O"
    ]
  },
  "795130": {
    "pincode": "795130",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Lilong S.O",
      "Arapti B.O",
      "Chajing pt. 1 B.O",
      "Haoreibi Mayai Leikai B.O",
      "Heinoumakhong B.O",
      "Lairabokhong B.O",
      "Nungei B.O",
      "Phunalmaring B.O",
      "Thiyamkonjil B.O",
      "Turelahnabi B.O",
      "Urup B.O"
    ]
  },
  "795131": {
    "pincode": "795131",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Bongajang B.O",
      "Bongjang B.O",
      "Changpol B.O",
      "Gamphajal B.O",
      "Khudengthabi B.O",
      "Maojam B.O",
      "Molcham B.O",
      "New Somtal B.O",
      "Moreh S.O",
      "T  Bongmun B.O",
      "Tengnoupal B.O",
      "T minou B.O",
      "Yangoulen B.O",
      "Khenjoi B.O"
    ]
  },
  "795132": {
    "pincode": "795132",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Mayang Imphal S.O",
      "Arong B.O",
      "Bengoon Maning Leikai B.O",
      "Chabung Company B.O",
      "Chirai Muslim B.O",
      "Heibong Makhong B.O",
      "Irong Chessaba B.O",
      "Irong Umang Khunou B.O",
      "Khelakhong Khongjil B.O",
      "Komlakhong B.O",
      "Laphupat B.O",
      "Loukok Mayai Leikai B.O",
      "Maibam Konjil B.O",
      "Mayang Impahl Bengoon B.O",
      "Mutum Phibou B.O",
      "Phoubakchao B.O",
      "Santhel B.O",
      "Sekmaijin  Khunou Konuma B.O",
      "Sekmaijin B.O",
      "Sekmaijin Khunou Litan Makhong B.O",
      "Tera Khoidum Mayai Leikai B.O",
      "Uchiwa B.O",
      "Uchiwa Wangban B.O",
      "Hayel Hangoon B.O"
    ]
  },
  "795133": {
    "pincode": "795133",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Kumbi (P) B.O",
      "Kwakta B.O",
      "Molphei Tampak B.O",
      "Nabil (P) B.O",
      "Ngangkha Louwai B.O",
      "Nungthangtampak B.O",
      "Phousabung B.O",
      "Sagang B.O",
      "Saiton B.O",
      "Terakhongshangbi B.O",
      "Thanga Karang B.O",
      "Thanga(P) B.O",
      "Thumkhonglok B.O",
      "Torbung B.O",
      "Tronglaobi B.O",
      "Tuisang Gothal B.O",
      "Wangoo B.O",
      "Wangoo Tera B.O",
      "Moirang S.O",
      "Boroyangbi B.O",
      "Bunglon B.O",
      "Dopkon B.O",
      "Ethai Bazar B.O",
      "Gelman (G.Gothal) B.O",
      "Haotakphailen B.O",
      "Kangathel B.O",
      "Khathinungei B.O",
      "Khoirentak B.O",
      "Khordak Ichin B.O",
      "Khousabung B.O"
    ]
  },
  "795134": {
    "pincode": "795134",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Nambol S.O",
      "Awangjiri B.O",
      "Bungte Chiru B.O",
      "Heikrujam B.O",
      "Irengbam B.O",
      "Isok B.O",
      "Kabowakching B.O",
      "Kangmong B.O",
      "Keinou B.O",
      "Khabi B.O",
      "Langpok B.O",
      "Leimapokpam B.O",
      "Leimaram B.O",
      "Maibam B.O",
      "Manamayang B.O",
      "Naorem B.O",
      "Oinam B.O",
      "Pukhrambam B.O",
      "Yarou Bomdiar B.O",
      "Thingkai Khullen B.O",
      "Utlou B.O"
    ]
  },
  "795135": {
    "pincode": "795135",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Pallel S.O",
      "Aimol Kodamphai B.O",
      "Angbrashu B.O",
      "Bongli B.O",
      "Chatong B.O",
      "Chelhep B.O",
      "Kambang Khunou B.O",
      "Khangbarol B.O",
      "Kharou Khullen B.O",
      "Khoibu B.O",
      "Khousat (T/C/D) B.O",
      "Khudei Khullen B.O",
      "Khunbi B.O",
      "Laiching Tangsang B.O",
      "Lamkang Khunou B.O",
      "Lamlong Khullen B.O",
      "Lamlong Khunou B.O",
      "Leibi B.O",
      "Liwachangning B.O",
      "Machi B.O",
      "Minou Khunjao B.O",
      "Molhang B.O",
      "Moltek B.O",
      "Narum B.O",
      "Saibhom B.O",
      "Sita B.O",
      "Songjang B.O",
      "Thamnapokpi B.O"
    ]
  },
  "795136": {
    "pincode": "795136",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Sekmai S.O",
      "Awang Leikinthabi B.O",
      "Ch. Khongnangpokpi B.O",
      "Kanglatombi B.O",
      "Luwangsangol B.O",
      "Makhan B.O",
      "Potsangbam Khoiru B.O"
    ]
  },
  "795138": {
    "pincode": "795138",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Thoubal S.O",
      "Athokpam B.O",
      "Athokpam Khunou B.O",
      "Charangpat B.O",
      "Haokha B.O",
      "Irong Thokchom (T/C/D) B.O",
      "Keibung Mamang B.O",
      "Khangabok B.O",
      "Khekman B.O",
      "Kiyam Siphai B.O",
      "Kshetri Leikai B.O",
      "Moijing B.O",
      "Okram Wangmataba B.O",
      "Phoudel B.O",
      "Poirou Kongjil B.O",
      "Sabaltongba B.O",
      "Thoubal Block (T/C/D) B.O",
      "Thoubal Khunou B.O",
      "Thoubal Leisangthem B.O",
      "Thoubal Ningombam B.O",
      "Wangbal B.O"
    ]
  },
  "795139": {
    "pincode": "795139",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Singhat S.O",
      "Behiang B.O",
      "Hengtam B.O",
      "Kangkap B.O",
      "Lama Camp B.O",
      "Lungchin B.O",
      "Lungthul(L.Daijang) B.O",
      "Mualmun B.O",
      "Siabu B.O",
      "Suangdoh B.O",
      "Songtal B.O",
      "Vokbual B.O"
    ]
  },
  "795140": {
    "pincode": "795140",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Tulihal S.O",
      "Laiphrakom B.O",
      "Sangaiprou B.O",
      "Yarou Mitram B.O"
    ]
  },
  "795141": {
    "pincode": "795141",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Tamenglong S.O",
      "Tamenglong Khunjao B.O",
      "Tharon B.O",
      "Thuilon B.O",
      "Tousem B.O",
      "Wairengba B.O",
      "Farmland B.O",
      "Akhui B.O",
      "Atengba B.O",
      "Bhalok B.O",
      "Bongoijang B.O",
      "Dailong B.O",
      "Kahulong B.O",
      "Keikao B.O",
      "Khongjaron B.O",
      "Khongjaron Khunthak B.O",
      "Longpram B.O",
      "Namtiram B.O",
      "New Pallong B.O",
      "Songpram B.O"
    ]
  },
  "795142": {
    "pincode": "795142",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Ukhrul S.O",
      "Chamu B.O",
      "Chingai (P) B.O",
      "Chingjaroi (p) B.O",
      "Chither (P) B.O",
      "Huining B.O",
      "Hundung B.O",
      "Jessami B.O",
      "Kagai B.O",
      "Khamasom B.O",
      "Kharasom B.O",
      "Khayang B.O",
      "Lamlang Gate B.O",
      "Mapum B.O",
      "Ngaimu B.O",
      "Nungbi Khunou B.O",
      "Nungshang B.O",
      "Paorei B.O",
      "Paoyii B.O",
      "Phungcham B.O",
      "Poii B.O",
      "Pushing B.O",
      "Rajai Khunou B.O",
      "Sirarakhong B.O",
      "Siroi B.O",
      "Soraphung B.O",
      "T.C.Compound B.O",
      "Tolloi B.O",
      "Tuinem B.O",
      "Tungou B.O",
      "Tushem B.O"
    ]
  },
  "795144": {
    "pincode": "795144",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Somdal S.O",
      "Huime B.O",
      "Kachai B.O",
      "Phadang B.O",
      "Thiwa B.O"
    ]
  },
  "795145": {
    "pincode": "795145",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Litan S.O",
      "Aishi B.O",
      "Chadong B.O",
      "Chassad B.O",
      "Grihang B.O",
      "Kongpat Khunou B.O",
      "L Tangkhul B.O",
      "Lambui B.O",
      "Leiting B.O",
      "Lungphu B.O",
      "Maku B.O",
      "Maokot B.O",
      "Phungyar B.O",
      "Sanakeithel B.O",
      "Semol B.O",
      "Shangsak B.O",
      "Siyamongjang B.O",
      "Sorde B.O",
      "Thawai B.O",
      "Yaingangpokpi B.O"
    ]
  },
  "795146": {
    "pincode": "795146",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Lamsang S.O",
      "Christian Centre Haochong B.O",
      "Heibongpokpi B.O",
      "Ijairong B.O",
      "Irengnaga B.O",
      "Kangchup Chiru B.O",
      "Kangchup Hill (P) B.O",
      "Kangchup Makhong B.O",
      "Karam Vaiphei B.O",
      "Khungdong Khungkhaiba B.O",
      "Lairen Sajik B.O",
      "Lamdeng B.O",
      "Mayang Langjing B.O",
      "Oktan B.O",
      "Phayeng B.O",
      "Ponlian B.O"
    ]
  },
  "795147": {
    "pincode": "795147",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Nungba S.O",
      "Khongsang B.O",
      "Kambiron B.O",
      "Mukti Khullen B.O",
      "Rengpang B.O",
      "Sibilong B.O"
    ]
  },
  "795148": {
    "pincode": "795148",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Wangjing S.O",
      "Cherapur B.O",
      "Heirok Part 2 B.O",
      "Herok Part 1 B.O",
      "Karongthel B.O",
      "Khongjom B.O",
      "Phundrei B.O",
      "Puleipokpi B.O",
      "Salungpham B.O",
      "Samaram B.O",
      "Sangaiyumpham B.O",
      "Sapam Salai B.O",
      "Tekcham B.O",
      "Tentha B.O",
      "Tollen B.O",
      "Langathel B.O"
    ]
  },
  "795149": {
    "pincode": "795149",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Yairipok S.O",
      "Lembakhul B.O",
      "Andro B.O",
      "Angtha B.O",
      "Bongbal Khullen B.O",
      "Changamdabi B.O",
      "Haokhongching B.O",
      "Heinganglok B.O",
      "Huikap B.O",
      "Irong Tangkhul B.O",
      "Kamu Tampak B.O",
      "Kamu Tongnom B.O",
      "Kamuching B.O",
      "Kasom Khullen B.O",
      "Khoirom B.O",
      "Khonglou Vaiphei (T/C/D) B.O",
      "Lourembam B.O",
      "Lungthar B.O",
      "Mollen (T/C/D) B.O",
      "Nambasi B.O",
      "Nongpok Sekmai B.O",
      "Phouoibi B.O",
      "Saram Patong B.O",
      "Soichang B.O",
      "Top Chingtha B.O",
      "Yairipok Tulihal B.O",
      "Yambem Laxmi Bazar B.O",
      "Ningthounai B.O",
      "Theiyong B.O"
    ]
  },
  "795150": {
    "pincode": "795150",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "Mao S.O",
      "Pfukhro B.O",
      "Phunanamei B.O",
      "Pudunamei B.O"
    ]
  },
  "795159": {
    "pincode": "795159",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Manipur Division",
    "offices": [
      "None S.O",
      "Awangkhul B.O",
      "Charoi Tupul B.O",
      "Haochong B.O",
      "Lukhambi B.O",
      "Nagaching B.O",
      "New Kabui Khullen B.O",
      "Karuangmuan (Nungtek) B.O",
      "Thingra B.O",
      "Nungnang B.O"
    ]
  },
  "796001": {
    "pincode": "796001",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Aizawl H.O",
      "Dawrpui S.O"
    ]
  },
  "796004": {
    "pincode": "796004",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Mizoram University S.O"
    ]
  },
  "796005": {
    "pincode": "796005",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Kulikawn S.O",
      "Hualngohmun B.O",
      "Kelsih B.O",
      "Khawchhete B.O",
      "Maubawk B.O",
      "Missionvengthlang B.O",
      "Muallungthu B.O",
      "Mualmawi B.O",
      "Mualpui B.O",
      "N.Lungleng B.O",
      "S,hlimen B.O",
      "Sabualkawn B.O",
      "Thingdawlmelriat B.O",
      "New Secretariat Complex B.O",
      "Bungkawn B.O",
      "College Veng B.O",
      "Republic Veng B.O",
      "Thakthingbazar B.O"
    ]
  },
  "796007": {
    "pincode": "796007",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Edenthar B.O",
      "Chandmary S.O",
      "Armed Veng B.O",
      "Bethlehem B.O",
      "Ramthar B.O",
      "Tuithiang B.O",
      "Electric Veng B.O"
    ]
  },
  "796008": {
    "pincode": "796008",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Armed Veng S.O"
    ]
  },
  "796009": {
    "pincode": "796009",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Vaivakawn S.O",
      "Chawlhhmun B.O",
      "Govt. Complex B.O",
      "Luangmual B.O",
      "Rangvamual B.O",
      "Sakawrtuichhun B.O",
      "Tanhril B.O",
      "Dinthar B.O"
    ]
  },
  "796012": {
    "pincode": "796012",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Ramhlun S.O",
      "Chaltlang B.O"
    ]
  },
  "796014": {
    "pincode": "796014",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Bawngkawn S.O",
      "Durtlang B.O",
      "Sihphir B.O"
    ]
  },
  "796015": {
    "pincode": "796015",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Lungdai B.O",
      "Muthi B.O",
      "Selesih B.O",
      "Zanlawn B.O",
      "Durtlang SO",
      "Tlangnuam S.O"
    ]
  },
  "796017": {
    "pincode": "796017",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Zemabawk S.O",
      "Cti Sesawng B.O",
      "Kepran B.O",
      "Khanpui B.O",
      "Khawruhlian B.O",
      "Phaileng 'E' B.O",
      "Sawleng B.O",
      "Sesawng B.O",
      "Zuangtui B.O",
      "Tuirial Airfield B.O",
      "Putlungasih B.O"
    ]
  },
  "796036": {
    "pincode": "796036",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Sihphir S.O"
    ]
  },
  "796070": {
    "pincode": "796070",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Kawnpui S.O",
      "Bualpui B.O",
      "Hortoki B.O",
      "Mualvum B.O",
      "N.Chaltlang B.O",
      "Bukpui B.O",
      "Lungmuat B.O",
      "N.Hlimen B.O",
      "Nisapui B.O",
      "Thingthelh B.O",
      "Serkhan B.O"
    ]
  },
  "796075": {
    "pincode": "796075",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Thingdawl S.O"
    ]
  },
  "796081": {
    "pincode": "796081",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Kolasib S.O",
      "Thingdawl B.O",
      "Bairabi B.O",
      "Builum B.O",
      "Chuhvel B.O",
      "Diakkawn B.O",
      "Saikhawthlir B.O",
      "Suarhliap B.O",
      "Vengthar B.O"
    ]
  },
  "796091": {
    "pincode": "796091",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Bilkhawthlir S.O",
      "Buhchang B.O",
      "Phaisen B.O",
      "N.Chawnpui B.O",
      "Saiphai B.O",
      "Saipum B.O",
      "Tuirial H.E.P B.O"
    ]
  },
  "796101": {
    "pincode": "796101",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Vairengte S.O",
      "Phainuam B.O"
    ]
  },
  "796111": {
    "pincode": "796111",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Darlawn S.O",
      "Khawpuar B.O",
      "Lungsum B.O",
      "Mauchar B.O",
      "N.Serzawl B.O",
      "N.Tinghmun B.O",
      "New Vervek B.O",
      "Palsang B.O",
      "Ratu B.O",
      "Sailutar B.O",
      "Sakawrdai B.O",
      "Thingsat B.O",
      "Vaitin B.O",
      "Vervek B.O",
      "Zohmun B.O"
    ]
  },
  "796161": {
    "pincode": "796161",
    "circle": "North Eastern Circle",
    "region": "North Eastern Region",
    "division": "Mizoram Division",
    "offices": [
      "Thingsulthliah S.O",
      "Baktawng B.O",
      "Chhingchhip B.O",
      "Hmuntha B.O",
      "Hualtu B.O",
      "Khawbel B.O",
      "Khumtung B.O",
      "Seling B.O",
      "Thentlang B.O",
      "Tlungvel B.O"
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
