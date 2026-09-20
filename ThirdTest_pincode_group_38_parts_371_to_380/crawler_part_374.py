"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 374 / 400
================================================================================
- Group: ThirdTest_pincode_group_38_parts_371_to_380
- Assigned PIN Codes: 48 (Range: 799105 to 799280)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_374.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_374.csv & .json
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

PART_ID = "part_374"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-374] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "799105",
  "799113",
  "799114",
  "799115",
  "799120",
  "799125",
  "799130",
  "799131",
  "799132",
  "799141",
  "799142",
  "799143",
  "799144",
  "799145",
  "799150",
  "799153",
  "799155",
  "799156",
  "799157",
  "799201",
  "799202",
  "799203",
  "799204",
  "799205",
  "799207",
  "799210",
  "799211",
  "799212",
  "799250",
  "799251",
  "799253",
  "799254",
  "799256",
  "799260",
  "799261",
  "799262",
  "799263",
  "799264",
  "799266",
  "799269",
  "799270",
  "799271",
  "799273",
  "799275",
  "799277",
  "799278",
  "799279",
  "799280"
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
  "799105": {
    "pincode": "799105",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Kakraban S.O",
      "Bazar Amtali B.O",
      "Chandul B.O",
      "Dhuptali B.O",
      "East Mirza B.O",
      "Hadra B.O",
      "Ichacherra B.O",
      "Jitendranagar B.O",
      "Kamrangatali B.O",
      "Kishoreganj B.O",
      "Kusumara B.O",
      "Mogpuskarini B.O",
      "Mohanbhog B.O",
      "Palatana B.O",
      "Paschim Mogpuskarini B.O",
      "Rani B.O",
      "Samukcherra B.O",
      "Silghati B.O",
      "Taibandal B.O",
      "Upendranagar B.O"
    ]
  },
  "799113": {
    "pincode": "799113",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Tepania S.O",
      "Bagma B.O",
      "East Bagabassa B.O",
      "Kupilong B.O",
      "Bagma ED B.O",
      "Garjanmura B.O"
    ]
  },
  "799114": {
    "pincode": "799114",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Gakulpur S.O",
      "Khilpara B.O",
      "Killarbazar B.O",
      "Manikya B.O",
      "Pitra Bazar B.O",
      "Raiyabari B.O",
      "Rajarbag B.O",
      "Rajnagar Colony B.O",
      "Thelakum B.O",
      "Uttar Brojendranagar B.O",
      "Salgarah B.O"
    ]
  },
  "799115": {
    "pincode": "799115",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Melaghar S.O",
      "Bagabassa B.O",
      "Bardwal B.O",
      "Battali B.O",
      "Chandigarh B.O",
      "Chowmouhani B.O",
      "Durlavnarayan B.O",
      "Jumerdhepa B.O",
      "Kemtali B.O",
      "Khas Chowmouhani B.O",
      "Lakshmandhepa B.O",
      "Nalchar B.O",
      "Rudijala B.O",
      "Telkajla B.O",
      "Urmai B.O",
      "West Nalchar B.O"
    ]
  },
  "799120": {
    "pincode": "799120",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Radhakishorepur H.O"
    ]
  },
  "799125": {
    "pincode": "799125",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Garjee S.O",
      "Gangacherra B.O",
      "Kalabon B.O",
      "Paikhola B.O",
      "Paschim Paticherri Colony B.O",
      "Rajapur B.O",
      "Tainani B.O",
      "Takmacherra B.O"
    ]
  },
  "799130": {
    "pincode": "799130",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Sekerkote S.O",
      "Amtali B.O",
      "Champamura Bazar B.O",
      "Kanchanmala B.O",
      "Pandavpur B.O",
      "Paschim Gokulnagar B.O",
      "Pravapur B.O",
      "Rayermura B.O",
      "Surjamaninagar B.O",
      "Viveknagar B.O"
    ]
  },
  "799131": {
    "pincode": "799131",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Sonamura S.O",
      "Dhanirampur B.O",
      "Bejimara B.O",
      "Dhanpur B.O",
      "Durgapur B.O",
      "Induria B.O",
      "Kamalnagar B.O",
      "Kathalia B.O",
      "Khedabari B.O",
      "Kulubari B.O",
      "Matinagar B.O",
      "Nidaya BO",
      "Rabindranagar Colony B.O"
    ]
  },
  "799132": {
    "pincode": "799132",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Kathalia S.O",
      "Bhabanipur B.O",
      "Jatrapur B.O",
      "Maheshpur B.O",
      "Manaipathar B.O",
      "Nirvoypur B.O",
      "Kalikrishnagar B.O"
    ]
  },
  "799141": {
    "pincode": "799141",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Jolaibari S.O",
      "Dakshin Hichacherri B.O",
      "Debdaru Bazar B.O",
      "Kalashi Bazar B.O",
      "Kwaifung B.O",
      "Madhya Pillak B.O",
      "Paschim Pillak B.O",
      "Sakbari B.O",
      "Thakurcherra B.O"
    ]
  },
  "799142": {
    "pincode": "799142",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Muhuripur S.O",
      "Charakbai B.O",
      "Muhuri Ratanpur B.O"
    ]
  },
  "799143": {
    "pincode": "799143",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Manubazar S.O",
      "Amlighat B.O",
      "Bhuratali B.O",
      "Bishnupur B.O",
      "Dakshin Fulcherri B.O",
      "Dakshin Manubankul B.O",
      "Guachand B.O",
      "Magurcherra B.O",
      "Satchand B.O",
      "Sindukpathar B.O",
      "Srinagar B.O",
      "Poangbari B.O"
    ]
  },
  "799144": {
    "pincode": "799144",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Santirbazar S.O",
      "Bagafa B.O",
      "Baikhora B.O",
      "Betaga B.O",
      "East Bagafa B.O",
      "Kanchannagar B.O",
      "Kathaliacherra B.O",
      "Laxmicherra B.O",
      "Lowgang B.O",
      "Manpather B.O",
      "Purba Manu B.O",
      "Radhakishoreganj B.O",
      "Urrat Takmacherra B.O",
      "Uttar Debipur B.O"
    ]
  },
  "799145": {
    "pincode": "799145",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Sabroom S.O",
      "Baishnabpur B.O",
      "Bijoynagar B.O",
      "Chalitacherra B.O",
      "Doulbari B.O",
      "Harbatali B.O",
      "Harina Bazar B.O",
      "Jalefa Bazar B.O",
      "Kathalcherri B.O",
      "Ludhua B.O",
      "Ludhua Tea Estate B.O",
      "Rupaicherri B.O",
      "Sonaicherri B.O"
    ]
  },
  "799150": {
    "pincode": "799150",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Rajnagar S.O (South Tripura)",
      "Kasari B.O"
    ]
  },
  "799153": {
    "pincode": "799153",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Radhanagar S.O (West Tripura)",
      "Rangamura B.O"
    ]
  },
  "799155": {
    "pincode": "799155",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Beloniya S.O",
      "Abhoyganj Bazar B.O",
      "Barpathari B.O",
      "Baspadua B.O",
      "Belonia aerodrome B.O",
      "Chittamara B.O",
      "Dakshin Sonaicharri B.O",
      "Debipur B.O",
      "Matai B.O",
      "Paschim Pipariakhola B.O",
      "Sarasima B.O",
      "South Bharat Ch.Nagar B.O",
      "Subashnagar B.O",
      "Uttar Bharat Ch Nagar B.O",
      "Uttar Sonaicherri B.O"
    ]
  },
  "799156": {
    "pincode": "799156",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Hrishyamukh S.O",
      "Madhabnagar B.O",
      "Nalua B.O",
      "Samarendraganj B.O",
      "South Krishnagar B.O"
    ]
  },
  "799157": {
    "pincode": "799157",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Anandapur S.O (South Tripura)",
      "Chottakhola B.O",
      "Gouranga Bazar B.O",
      "Dimatali B.O",
      "Gabtali B.O",
      "Puran Rajbari B.O",
      "Siddhinagar B.O",
      "South srirampur B.O"
    ]
  },
  "799201": {
    "pincode": "799201",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Khowai S.O",
      "Agagartilla B.O",
      "Asharambari B.O",
      "Bachaibari B.O",
      "Banbazar B.O",
      "Belcherra B.O",
      "Behalabari B.O",
      "Champahaor B.O",
      "East Bachaibari B.O",
      "Gakulbari B.O",
      "Ganki B.O",
      "Karingicherra B.O",
      "Khowai T. E B.O",
      "Paharmora B.O",
      "Paschim Belcherra B.O",
      "Paschim Laxmicherra B.O",
      "Ratanpurbazar B.O",
      "Samatal Padmabil B.O",
      "Singicherra B.O",
      "Subashpark B.O"
    ]
  },
  "799202": {
    "pincode": "799202",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Khowai Court S.O"
    ]
  },
  "799203": {
    "pincode": "799203",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Kalyanpur S.O (West Tripura)",
      "Barmaidan Bazar B.O",
      "Dakshin Promode L. L.Colony B.O",
      "Durgapur B.O",
      "Dwarikapur B.O",
      "Ghilatali B.O",
      "Kalyanpur T. E B.O",
      "Moharcherra B.O",
      "Paschim Kalyanpur B.O",
      "Paschim Kunjaban B.O",
      "Paschim Santinagar B.O",
      "Ramdayal Lendless Colony B.O",
      "Ratia B.O",
      "Tablabari B.O",
      "Uttat Pulinpur B.O"
    ]
  },
  "799204": {
    "pincode": "799204",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Kulaibazar S.O",
      "Balarambazar B.O",
      "Nalicherra B.O"
    ]
  },
  "799205": {
    "pincode": "799205",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Teliamura S.O",
      "Ataromura B.O",
      "Barmura Project B.O",
      "Brahmacherra B.O",
      "Chakmaghat B.O",
      "Golabari B.O",
      "Hadrai B.O",
      "Howaibari B.O",
      "Karailong B.O",
      "Khasiamangal B.O",
      "Maharanipur B.O",
      "Maigonga B.O",
      "Mongtukupara B.O",
      "Nabakumar Rankhal Para B.O",
      "Nunacherra B.O",
      "Rupacherra B.O",
      "Uttar Gakulnagar B.O",
      "Kalitilla B.O",
      "Netajinagar B.O"
    ]
  },
  "799207": {
    "pincode": "799207",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Chebri S.O",
      "Baijalbari B.O",
      "Bharatsardarbari B.O",
      "Chankhala B.O",
      "Gouranga Tilla B.O",
      "Paglabari B.O",
      "Rajnagar Khowai B.O",
      "Ramchandraghat B.O",
      "Sonatala B.O",
      "Uttarchebri B.O"
    ]
  },
  "799210": {
    "pincode": "799210",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Kamalghat S.O",
      "Bodhjungnagar B.O",
      "Kalibazar B.O",
      "Chechuria B.O",
      "Fatikchhera B.O",
      "Harendranagar T. B.O",
      "Laifunga B.O",
      "Lembucherra B.O"
    ]
  },
  "799211": {
    "pincode": "799211",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Mohanpur S.O (West Tripura)",
      "Baikunthapur B.O",
      "Bamutia B.O",
      "Barkathalia B.O",
      "Bijoynagar Bazar B.O",
      "Chachubazar B.O",
      "Dighalia Bazar B.O",
      "Kalacherra T.E. B.O",
      "Kalkalia T.E. B.O",
      "Madhuchoudhurypara B.O",
      "Patnipara B.O",
      "Subalsing B.O",
      "Taranagar B.O",
      "Tarapur B.O",
      "Thamakari B.O",
      "Uttar debendranagar B.O"
    ]
  },
  "799212": {
    "pincode": "799212",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Agartala Division",
    "offices": [
      "Sidhai S.O",
      "Balubon B.O",
      "Darogamura B.O",
      "Ishanpur B.O",
      "Simna B.O",
      "Simna Colony B.O",
      "Sonaram Bazar B.O",
      "Sundartilla B.O",
      "Uttar Dasgharia B.O"
    ]
  },
  "799250": {
    "pincode": "799250",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Dharmanagar H.O"
    ]
  },
  "799251": {
    "pincode": "799251",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Chandrapur S.O (North Tripura)",
      "Bakbaki B.O",
      "Baruakandi B.O",
      "Bhagyapur B.O",
      "Dighalbak B.O",
      "Gobindapur B.O",
      "Huruah B.O",
      "Ichailalcherra B.O",
      "Ichaisonapur B.O",
      "Kalacherra B.O",
      "Paschimchandrapur B.O",
      "Pratyekroy B.O",
      "Raghna B.O",
      "Sakaibari B.O",
      "Sonarurbassa B.O",
      "Nayapara N.D B.O",
      "Thana Road B.O"
    ]
  },
  "799253": {
    "pincode": "799253",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Rajbari S.O",
      "Algapur B.O",
      "Bagbassa B.O",
      "Dharmanagar College B.O",
      "Dhupirbond B.O",
      "East Huruah B.O",
      "East Kamesawar B.O",
      "Kakrirpar B.O",
      "Kameswargaon B.O",
      "Krishnapur B.O",
      "Lalcherra Colony B.O",
      "Radhapur B.O",
      "South Ganganagar B.O",
      "Tripura Ganganagar B.O",
      "Zaithang B.O",
      "Dharmanagar Bazar B.O",
      "Padmapur B.O"
    ]
  },
  "799254": {
    "pincode": "799254",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Halflongcherra S.O",
      "Baithangbari B.O",
      "Balidhum B.O",
      "Dewanpassa B.O",
      "Jubarajnagar B.O",
      "Paschim Radhapur B.O",
      "Rajnagar Laxmipur B.O"
    ]
  },
  "799256": {
    "pincode": "799256",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Tripura Damcherra S.O",
      "Kacharicherra B.O",
      "Khedacherra B.O",
      "Narendranagar B.O",
      "Piplacherra B.O"
    ]
  },
  "799260": {
    "pincode": "799260",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Panisagar S.O",
      "Agnipassa B.O",
      "Bilthai B.O",
      "Deocherra B.O",
      "Indurail B.O",
      "Jalabazar B.O",
      "Jalebassa B.O",
      "Janata College B.O",
      "Juri R.F B.O",
      "North Padmabil B.O",
      "Padmabil B.O",
      "Pekucherr B.O",
      "Rahumcherra B.O",
      "Rowabazar B.O",
      "Tilthai B.O",
      "Tilthai Nutanbazar B.O",
      "Ujan Sailenbari B.O",
      "Uptakhali B.O",
      "West Panisagar B.O",
      "Noagang B.O"
    ]
  },
  "799261": {
    "pincode": "799261",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Kadamtala S.O (North Tripura)",
      "Amtilla B.O",
      "Brajendranagar B.O",
      "Kalagangar Par B.O",
      "Kurit B.O",
      "Pearacherra B.O",
      "Premtala B.O",
      "Rakhalgang B.O",
      "Ranibari B.O",
      "Sarala B.O",
      "Tarakpur B.O"
    ]
  },
  "799262": {
    "pincode": "799262",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Churaibari S.O",
      "Balicherra B.O",
      "Fulbari B.O",
      "Kheranjuri B.O",
      "Sanicherra B.O"
    ]
  },
  "799263": {
    "pincode": "799263",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Pecharthal S.O",
      "Andharcherra B.O",
      "Dhanicherra B.O",
      "Govidabari B.O",
      "Karaicherra B.O",
      "Krishnatilla B.O",
      "Machmara B.O",
      "Nabincherra B.O",
      "Nalkata B.O",
      "Ramgunapara B.O",
      "Ugalcherra B.O"
    ]
  },
  "799264": {
    "pincode": "799264",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Kumarghat S.O",
      "Bhatisonaimuri B.O",
      "Darchai B.O",
      "East Kanchanbari B.O",
      "Notting Chedrra B.O",
      "Pabiacherra B.O",
      "Sayedabari B.O",
      "South Unokoti B.O"
    ]
  },
  "799266": {
    "pincode": "799266",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Kanchancherra",
      "Karamcherra B.O",
      "West Nalkata B.O",
      "Betcherra B.O",
      "Purba Betcherra B.O"
    ]
  },
  "799269": {
    "pincode": "799269",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Jampui S.O",
      "Belianchief B.O",
      "Hmungchuan B.O",
      "Hmunpui B.O",
      "Kalagong B.O",
      "Kangrai B.O",
      "Phuldengsai B.O",
      "Sabual B.O",
      "Sailo B.O",
      "Tlangsang B.O"
    ]
  },
  "799270": {
    "pincode": "799270",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Kanchanpur S.O (North Tripura)",
      "Joyasree B.O",
      "Laljuri B.O",
      "Satnala B.O",
      "Sibnagar B.O",
      "Suknacherra B.O"
    ]
  },
  "799271": {
    "pincode": "799271",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Dasdabazar S.O",
      "Anandabazar B.O",
      "Barhaldi B.O",
      "Garchirampara B.O",
      "Kalapania B.O",
      "Sakhan Serhmun B.O",
      "Tuichama B.O"
    ]
  },
  "799273": {
    "pincode": "799273",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Chailengta S.O",
      "Bindulal Karbaripara B.O",
      "Chawmanu B.O",
      "Durgacherra B.O",
      "Laldingabari B.O",
      "Manikpur B.O",
      "Natinmanu B.O"
    ]
  },
  "799275": {
    "pincode": "799275",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Manughat S.O",
      "Demchedrra Colony B.O",
      "Dhumacherra B.O",
      "East Masli B.O",
      "Jamircherra B.O",
      "Karatichesdrra B.O",
      "Longtharai B.O",
      "Mainerma B.O",
      "Maslicherra B.O",
      "Sindhukumarpara B.O"
    ]
  },
  "799277": {
    "pincode": "799277",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Kailashahar S.O",
      "Bhadrapally B.O",
      "Chandipur B.O",
      "Gulakpur T.E B.O",
      "Ichabpur B.O",
      "Kalishahan B.O",
      "Manuvelly B.O",
      "Noorpur B.O",
      "Rangrung B.O",
      "Sadhanasram B.O",
      "Samrurpar B.O",
      "Sreerampur B.O",
      "Jamtolibari B.O",
      "Panichowkibazar S.O",
      "Tapaspalli B.O"
    ]
  },
  "799278": {
    "pincode": "799278",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Salema S.O",
      "Kachucherra B.O",
      "Maharani Bus Stop B.O",
      "Mechuria B.O",
      "Mendihaor B.O",
      "Paschim Dalucherra B.O"
    ]
  },
  "799279": {
    "pincode": "799279",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Paiturbazar S.O",
      "Gournagar SO",
      "Bhagabannagar B.O",
      "Chantail B.O",
      "Chinibagan B.O",
      "Deora Cherra B.O",
      "Goldharpur B.O",
      "Gournagar B.O",
      "Kirtantali B.O",
      "Zarultali B.O",
      "Kailashahar Air Port B.O"
    ]
  },
  "799280": {
    "pincode": "799280",
    "circle": "North Eastern Circle",
    "region": "Shillong HQ Region",
    "division": "Dharmanagar Division",
    "offices": [
      "Sonamukhi S.O (North Tripura)",
      "Balehar B.O",
      "Birchandranagar B.O",
      "Dalugaon B.O",
      "Dhanbilash B.O",
      "Fultali B.O",
      "Jagannathpur B.O",
      "Pechardahar B.O"
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
