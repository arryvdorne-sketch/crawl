"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 379 / 400
================================================================================
- Group: ThirdTest_pincode_group_38_parts_371_to_380
- Assigned PIN Codes: 48 (Range: 805101 to 811309)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_379.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_379.csv & .json
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

PART_ID = "part_379"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-379] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "805101",
  "805102",
  "805103",
  "805104",
  "805105",
  "805106",
  "805107",
  "805108",
  "805109",
  "805110",
  "805111",
  "805112",
  "805113",
  "805114",
  "805121",
  "805122",
  "805123",
  "805124",
  "805125",
  "805126",
  "805127",
  "805128",
  "805129",
  "805130",
  "805131",
  "805132",
  "811101",
  "811102",
  "811103",
  "811104",
  "811105",
  "811106",
  "811107",
  "811112",
  "811201",
  "811202",
  "811211",
  "811212",
  "811213",
  "811214",
  "811301",
  "811302",
  "811303",
  "811304",
  "811305",
  "811307",
  "811308",
  "811309"
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
  "805101": {
    "pincode": "805101",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Barasolapur BO",
      "Dhanar BO",
      "Dharhara BO",
      "Dhamul SO"
    ]
  },
  "805102": {
    "pincode": "805102",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Bisunpur BO",
      "Sarkanda BO",
      "Tetariya BO",
      "Govindpur SO Nawada"
    ]
  },
  "805103": {
    "pincode": "805103",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Baijnathpur BO",
      "Barat Sarai BO",
      "Barhauna BO",
      "Chhota Jamura BO",
      "Chhotashekhpur BO",
      "Chhotipali BO",
      "Hasua SO",
      "Dhanwan BO",
      "Dona BO",
      "Punther BO",
      "Sachaul BO",
      "Sihin BO"
    ]
  },
  "805104": {
    "pincode": "805104",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Kadirganj SO",
      "Bermi BO",
      "Dermobara BO",
      "Ghostama BO",
      "Khalsadhibari BO",
      "Marue BO",
      "Ohari BO",
      "Paura BO",
      "Rajapur Saur BO",
      "Sadipur BO",
      "Nazardih B.O"
    ]
  },
  "805105": {
    "pincode": "805105",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Katrisarai SO",
      "Badi BO",
      "Bahadurganj BO",
      "Bhagwanpur BO",
      "Gazipur BO",
      "Maira Barich BO"
    ]
  },
  "805106": {
    "pincode": "805106",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Kawakol SO",
      "D Garh Itpakwa BO",
      "Kadhar BO",
      "Kewaii BO",
      "Kharsari BO",
      "Lalpur BO",
      "Madhurapur BO",
      "Mahapurchhab BO",
      "Manjhila BO",
      "N Purwaritola BO",
      "Okhariyagolab BO",
      "Phuldih BO",
      "Shekhodaura BO",
      "Sundri Dumari BO"
    ]
  },
  "805107": {
    "pincode": "805107",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Kosi SO Nawada",
      "Bhattagarh BO",
      "Kundabhalua BO",
      "Mahrawan BO",
      "Marara BO",
      "Rupau BO",
      "Siur BO",
      "Roh BO"
    ]
  },
  "805108": {
    "pincode": "805108",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Lalbigha SO",
      "Apsai BO",
      "Bajitpur BO",
      "Parwati BO"
    ]
  },
  "805109": {
    "pincode": "805109",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Nardiganj SO",
      "Aranikesopur BO",
      "Dohra BO",
      "Kahuara BO",
      "Kosla BO",
      "Masaurha BO",
      "Prama BO",
      "Reula BO",
      "Sarsu BO",
      "Seotar BO",
      "ORRO BO",
      "PACHHIYA B.O",
      "Hanriya B.O"
    ]
  },
  "805110": {
    "pincode": "805110",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Nawadha HO"
    ]
  },
  "805111": {
    "pincode": "805111",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Nawada Kutchery SO",
      "Asarhi BO",
      "Gonawan BO",
      "Hasapur BO",
      "Mahnadpur BO",
      "Pachra BO",
      "Sisma BO",
      "Babhnaur BO",
      "Chhatihar BO"
    ]
  },
  "805112": {
    "pincode": "805112",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Parnawada SO",
      "Derma BO",
      "Diri BO",
      "Frahimabad BO",
      "Makhar BO",
      "Nanaura BO",
      "Pithauri BO",
      "Teyar BO"
    ]
  },
  "805113": {
    "pincode": "805113",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Bauri BO",
      "Belar BO",
      "Bhatta BO",
      "Kashichak SO",
      "Birnawan BO",
      "Dergoan BO",
      "Khakhari BO",
      "Subhanpur B.O"
    ]
  },
  "805114": {
    "pincode": "805114",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Bihar Keshri sri Krishn Singh SO"
    ]
  },
  "805121": {
    "pincode": "805121",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Nemdarganj SO",
      "Baksoti BO",
      "Bhanail BO",
      "Budhwara BO",
      "Chhoti Amawa BO",
      "Dumari BO",
      "Lkhamohna BO",
      "Mheshdih BO"
    ]
  },
  "805122": {
    "pincode": "805122",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Narhat SO",
      "Ankari BO",
      "Bargaon BO",
      "Biju Bigha BO",
      "G B Kendua Konihar BO",
      "Khanwan BO",
      "Lond BO",
      "Merkuri BO",
      "Meskaur BO",
      "Raja Bigha BO",
      "Rajan BO",
      "Rasalpur BO",
      "Spur Gowasa BO",
      "Tetariya BO",
      "Konibar BO"
    ]
  },
  "805123": {
    "pincode": "805123",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Orhanpur SO",
      "Akaunabazar BO",
      "Bhadokhara BO",
      "Kenasari BO",
      "Oraina BO",
      "Sabhari BO",
      "Sonsihari BO",
      "Pakariya B.O"
    ]
  },
  "805124": {
    "pincode": "805124",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Pakaribarwan SO",
      "Arha BO",
      "Budhauli BO",
      "Chandradeep BO",
      "Datraul BO",
      "Gulani BO",
      "Juri BO",
      "Konadpur BO",
      "Marwa BO",
      "Rayes BO",
      "Simariya BO"
    ]
  },
  "805125": {
    "pincode": "805125",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Rajauli SO",
      "Andharwari BO",
      "Bahadurpur BO",
      "Bansgopal BO",
      "Chamotha BO",
      "Dhamani BO",
      "Singar BO",
      "Targir BO"
    ]
  },
  "805126": {
    "pincode": "805126",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Koriauna BO",
      "Kuhila BO",
      "Kusmuhar BO",
      "Paharpur BO",
      "Phatehpurmore BO",
      "Sughari BO",
      "Rajhat SO",
      "B Garhrampur BO",
      "Baksanda BO"
    ]
  },
  "805127": {
    "pincode": "805127",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Chauby BO",
      "Manjhauli BO",
      "Ramraichack BO",
      "S Marighgawa BO",
      "Sherpur BO",
      "Sirdala SO"
    ]
  },
  "805128": {
    "pincode": "805128",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "B Makdumpur BO",
      "Baijdah BO",
      "Chowar BO",
      "Dharampur BO",
      "Dumri BO",
      "Kenar BO",
      "Kother BO",
      "M H Chack BO",
      "Mahugain BO",
      "Nagwan BO",
      "Pasarhi BO",
      "Tarwan SO"
    ]
  },
  "805129": {
    "pincode": "805129",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Arnadi BO",
      "Bhadseni BO",
      "Dariyapur BO",
      "Dhandhar BO",
      "Khanpur BO",
      "Manjhaway BO",
      "Tungi SO"
    ]
  },
  "805130": {
    "pincode": "805130",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Warisaliganj SO",
      "Bilari BO",
      "Chackwai BO",
      "Dariyapur BO",
      "Jalalpur BO",
      "Katauna BO",
      "Kochagoan BO",
      "Makanpur BO",
      "Manjaur BO",
      "Millky BO",
      "Mosama BO",
      "Nromurar BO",
      "Paingari BO",
      "Sambey BO"
    ]
  },
  "805131": {
    "pincode": "805131",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Wazirganj SO Gaya",
      "Amethi BO",
      "Arawan BO",
      "Bisunpur BO",
      "Ghariya BO",
      "Ghogha BO",
      "Kajoor BO",
      "Karisowa BO",
      "Kharaua BO",
      "Kharghara BO",
      "Khirwan BO",
      "Kurkihar BO",
      "Manjhauli BO",
      "Panley BO",
      "Rajwara BO",
      "S Nawada BO",
      "Sahiya BO",
      "Singhwara BO"
    ]
  },
  "805132": {
    "pincode": "805132",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Aruri BO",
      "Baghibardiha BO",
      "Dhewdha BO",
      "Dosut BO",
      "Keshauri BO",
      "Samharigarh BO",
      "Dumrawan SO"
    ]
  },
  "811101": {
    "pincode": "811101",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Barbigha SO",
      "Amawan BO",
      "Asthana BO",
      "Babhanbigha BO",
      "Belaw BO",
      "Dhanuki BO",
      "Gilani BO",
      "Ibrahimpur BO",
      "K Milki Chak BO",
      "Kajifatuchak BO",
      "Kenar Kalan BO",
      "Kewti BO",
      "Kishanpur BO",
      "Kuthaut BO",
      "Maldah BO",
      "Malwa BO",
      "Maur BO",
      "Mohani BO",
      "Onama BO",
      "Ramjan Pur BO",
      "Rampur Sinday BO",
      "Sadarpur BO",
      "Samas BO",
      "Sarba BO",
      "Subhanpur BO",
      "Teus BO",
      "Toygarh BO",
      "CR Barbigha BO"
    ]
  },
  "811102": {
    "pincode": "811102",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Mehus SO",
      "Esuwa BO",
      "Gagaur BO",
      "Katari BO",
      "Mafo BO",
      "Sasaur BO",
      "Singhaul BO"
    ]
  },
  "811103": {
    "pincode": "811103",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Nimi SO",
      "Ambari BO",
      "Chakdin BO",
      "Charuawan BO",
      "Jean Bigha BO",
      "Paharia BO",
      "Panhesa BO",
      "Ugaban BO"
    ]
  },
  "811104": {
    "pincode": "811104",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Nalanda Division",
    "offices": [
      "Chero B.O",
      "Gopalbad B.O",
      "Husaina B.O",
      "Karkain B.O",
      "Mirnagar B.O",
      "Pyarepur B.O",
      "Ramnathpur B.O",
      "Sahari B.O",
      "Sarmera S.O"
    ]
  },
  "811105": {
    "pincode": "811105",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Sheikhpura SO Sheikhpura",
      "Purankama BO",
      "Abgil Chandey BO",
      "Afni BO",
      "Audhey BO",
      "Belchhi BO",
      "Bhojdih BO",
      "Biman BO",
      "Brindaban BO",
      "Chorwar BO",
      "D Lodipur BO",
      "Deoley",
      "Diha BO",
      "Gabai BO",
      "Hathiyama BO",
      "Hussainabad BO",
      "Kamta B.O",
      "Kare BO",
      "Karki BO",
      "Kasar BO",
      "Kusherhi BO",
      "Kushumbha BO",
      "Lodipur BO",
      "Mandana BO",
      "Maninda BO",
      "Masodha BO",
      "Mobarakpur BO",
      "Navinagar Kakrar BO",
      "Pain BO",
      "Pathraitha BO",
      "Pharpar BO",
      "Pinjari BO",
      "SGhuskari BO",
      "Sanaiya BO",
      "Sheikhpura Bazar SO",
      "Sheikhpura R S SO"
    ]
  },
  "811106": {
    "pincode": "811106",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Akbarpur BO",
      "Alinagar BO",
      "Amarpur BO",
      "Bijulia BO",
      "Dighari BO",
      "K T Laxmipur BO",
      "Kiranpur BO",
      "Mustafapur BO",
      "Nandpur BO",
      "Paharpur BO",
      "Shamho BO",
      "Ss Barari BO",
      "Surajgaraha SO"
    ]
  },
  "811107": {
    "pincode": "811107",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Sirari SO",
      "BBartara BO",
      "Bhadausi BO",
      "Bhadous BO",
      "Billo BO",
      "Dih Kushumbha BO",
      "Kachhiyana BO",
      "Kaithwan BO",
      "Nadiyama BO",
      "Pachana BO",
      "Pratappur BO",
      "R Mahsaura BO",
      "S Imam Nagar BO"
    ]
  },
  "811112": {
    "pincode": "811112",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Piri Bazar SO",
      "Abhaypur BO",
      "Loshghani BO"
    ]
  },
  "811201": {
    "pincode": "811201",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Munger HO",
      "Jamalpur WS",
      "Ganga Darshan SO",
      "Lal Darwaza SO Munger",
      "Belan Bazar SO",
      "Munger College SO",
      "Munger Fort SO",
      "Munger Town SO",
      "NDRoad Munger SO",
      "Purab Sarai SO"
    ]
  },
  "811202": {
    "pincode": "811202",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Basudeopur SO",
      "B M Tola BO",
      "Baha Chowki BO",
      "Bank Harpur BO",
      "Benigir BO",
      "Dariayapur BO",
      "Farda BO",
      "Heru Diyara BO",
      "J Nagar BO",
      "Kutlupur BO",
      "Mathar BO",
      "Shiv Kund BO",
      "Singhia BO",
      "Taufir BO",
      "Tikarampur BO",
      "Zamin Degree BO",
      "Shankarpur BO"
    ]
  },
  "811211": {
    "pincode": "811211",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Bariarpur SO Munger",
      "Agrahan BO",
      "Bhadaura BO",
      "Binda Diyara BO",
      "Ekashi BO",
      "Ganganiya BO",
      "Ghorghat BO",
      "Jankinagar BO",
      "K Shampur BO",
      "Kalarampur BO",
      "Kalyanpur BO",
      "Kharia BO",
      "Lohchi BO",
      "Maheshpur BO",
      "Nawagarhi BO",
      "Purshotampur BO",
      "Ratanpur BO"
    ]
  },
  "811212": {
    "pincode": "811212",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Dharhara SO Munger",
      "Amari BO",
      "Barmasia BO",
      "Itwa BO",
      "Lahauta Ghatwari BO",
      "Mangarh BO",
      "Orabagicha BO",
      "Tola Bangalwa BO",
      "Mahgana B.O",
      "Bhalar BO"
    ]
  },
  "811213": {
    "pincode": "811213",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Haweli Kharagpur SO",
      "Banhara BO",
      "Barhauna BO",
      "Bichhichanchar BO",
      "Dariapur BO",
      "Gangata More BO",
      "Gaurab Dih BO",
      "Gobadda BO",
      "Kendua BO",
      "Khaira BO",
      "Khaira Doray BO",
      "L Laxmipur BO",
      "Lagma BO",
      "Manjhgay BO",
      "Muzaffarganj BO",
      "Parsando BO",
      "R CMaidan BO",
      "Raja Dih BO",
      "Ramankabad BO",
      "Rathaitha BO",
      "Sadobh BO",
      "Shivpur Logain BO",
      "Teghara BO",
      "Tetia Bambar BO"
    ]
  },
  "811214": {
    "pincode": "811214",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Jamalpur SO Munger",
      "Halimpur BO",
      "Ithari BO",
      "Pattam BO",
      "Safiabad BO",
      "Sarobagh BO",
      "Sadar Bazar Jamalpur SO"
    ]
  },
  "811301": {
    "pincode": "811301",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Abgil Chaurama BO",
      "Baladih BO",
      "Darkha BO",
      "Dhanama BO",
      "Dinnagar BO",
      "Hilsa BO",
      "Islamnagar BO",
      "Kaiyar BO",
      "Markama BO",
      "Mirzaganj BO",
      "Noni BO",
      "Pursanda BO",
      "Sahora BO",
      "Seway BO",
      "Aliganj SO Jamui"
    ]
  },
  "811302": {
    "pincode": "811302",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Barhiya SO",
      "B Dariyapur BO",
      "Birupur BO",
      "Chetan Tola Khutha BO",
      "Dumari BO",
      "Ganga Sarai BO",
      "Jaitpur BO",
      "Jalal Pur Nauranga BO",
      "Khutha BO",
      "L Kalyanpur BO",
      "Pipariya BO",
      "Sadaibigha BO",
      "Talsharama BO"
    ]
  },
  "811303": {
    "pincode": "811303",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Bamdah BO",
      "Basbittibo BO",
      "Bichkorwa BO",
      "Chandra Mandi BO",
      "Dulampur BO",
      "Gaganpur BO",
      "Chakai SO",
      "Kiya Jori BO",
      "Koluadih BO",
      "Korane BO",
      "Madhopur BO",
      "N Silfari BO",
      "Patauwa BO",
      "S Batpar BO",
      "Saraun BO",
      "Tola Urba BO",
      "Barmoria BO",
      "Dadhwa BO",
      "Phariatadih BO",
      "Pojha BO",
      "Ramchandradih BO"
    ]
  },
  "811304": {
    "pincode": "811304",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Nawadha Division",
    "offices": [
      "Chewara SO",
      "Akarha BO",
      "Asthama BO",
      "Bahachha BO",
      "Chakandra BO",
      "Chhatiara BO",
      "Chordargah BO",
      "Ekrama BO",
      "Gagri BO",
      "Hassari BO",
      "Karandey BO",
      "Lohan BO",
      "Sohdi BO"
    ]
  },
  "811305": {
    "pincode": "811305",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Gidhaur SO",
      "Banpur BO",
      "Chango Dih BO",
      "Chuwan BO",
      "Dabil BO",
      "Gagra BO",
      "Garsanda BO",
      "Ghughuldih BO",
      "Keshopur BO",
      "Kewal Fariyatta BO",
      "Kharhawa BO",
      "Mango Bandar BO",
      "Maura BO",
      "Nayagaon BO",
      "Ratanpur BO",
      "Sevai BO",
      "Sohjana BO",
      "Sonpai BO",
      "Suggi BO",
      "Dhamna BO",
      "Tihiya BO"
    ]
  },
  "811307": {
    "pincode": "811307",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Jamui HO",
      "Jamui Court SO"
    ]
  },
  "811308": {
    "pincode": "811308",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Jhajha SO",
      "Batia BO",
      "Chain BO",
      "Chhapar Dih BO",
      "Fatehpur BO",
      "Karhara BO",
      "Karma Borba BO",
      "Narganjo BO",
      "Phulhara BO",
      "Rajala BO",
      "Sahiya BO",
      "Sugba Urown BO",
      "T Barajor BO",
      "T Barmasia BO",
      "T Chhapa BO",
      "Tabha BO",
      "Bajila BO",
      "Hathia BO",
      "Jhajha Bazar SO"
    ]
  },
  "811309": {
    "pincode": "811309",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Munger Division",
    "offices": [
      "Kajra SO",
      "Arma BO",
      "Lai BO",
      "Pawai BO",
      "Pokhrama BO",
      "Urain BO"
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
