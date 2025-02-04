import pandas as pd
import json
from datetime import datetime
import s3fs
import requests

def fetch_neo_data(start_date, end_date, api_key):
    #requesting data from nasa API 
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"
    response = requests.get(url)
    
    if response.status_code == 200:
        result = response.json()
        return result  # Return the JSON data if the request was successful  
    else:  
        print("Error fetching data:", response.status_code)  
        return None  


# Extracting NEO data
def extract_data(result):
    
    asteroids = result["near_earth_objects"]  
    for date, objects in asteroids.items():
        print(f"\nDate: {date}")
        for obj in objects:  
            print("\nAsteroid ID:", obj['id'])
            print("\nNEO ID:", obj['neo_reference_id'])
            print("Name:", obj['name'])
            print("NASA_JPL_URL:", obj['nasa_jpl_url'])
            print("Absolute_Magnitude_in H", obj['absolute_magnitude_h'])
            
            print("Is Potentially Hazardous:", obj['is_potentially_hazardous_asteroid'])

            # Estimated Diameter  
            diameter = obj['estimated_diameter']  
            print("Estimated Diameter (meters):")  
            print("  Max:", diameter['meters']['estimated_diameter_max'])  
            print("  Min:", diameter['meters']['estimated_diameter_min'])  
            
            # Close Approach Data  
            close_approaches = obj.get('close_approach_data', [])  
            for approach in close_approaches:  
                print("\n  Close Approach Date:", approach['close_approach_date_full'])  
                print("  Miss Distance (km):", approach['miss_distance']['kilometers'])  
                print("  Relative Velocity (km/h):", approach['relative_velocity']['kilometers_per_hour'])
                print("  Orbiting Body:", approach['orbiting_body'])
            
            
            
def extract_neo_to_dataframe(data_file):
    
    if not data_file:
        
        return None  
        
    all_asteroids = []
    asteroids = data_file['near_earth_objects']  
    for date, objects in asteroids.items():  
        for obj in objects:  
            asteroid_info = {  
                "Date": date,  
                "ID": obj['id'],  
                "Name": obj['name'],
                "NEO ID": obj['neo_reference_id'],
                "NASA_JPL_URL": obj['nasa_jpl_url'],
                "Absolute_Magnitude_in H": obj['absolute_magnitude_h'],
                "Is Potentially Hazardous": obj['is_potentially_hazardous_asteroid'],  
                "Max Diameter (m)": obj['estimated_diameter']['meters']['estimated_diameter_max'],  
                "Min Diameter (m)": obj['estimated_diameter']['meters']['estimated_diameter_min'],  
            }  
                
                # Include close approach data if available  
            close_approaches = obj.get('close_approach_data', [])  
            if close_approaches:  
                for approach in close_approaches:  
                    asteroid_info.update({  
                        "Close Approach Date": approach['close_approach_date'],  
                        "Miss Distance (km)": approach['miss_distance']['kilometers'],  
                        "Relative Velocity (km/h)": approach['relative_velocity']['kilometers_per_hour'],
                        "Orbiting Body": approach['orbiting_body']
                    })  
                    all_asteroids.append(asteroid_info)  
            else:  
                all_asteroids.append(asteroid_info)  
    
    return pd.DataFrame(all_asteroids)  

def run_nasa_etl():
    
    
    api_key = "HenZaHtMa8svygbKOtOGgIbbl3zvxasiu8HhFpNv"
    start_date = "2025-01-01"  
    end_date = "2025-01-07" 
    
    data_file = fetch_neo_data(start_date,end_date, api_key)
    extract_data(data_file)
    df = extract_neo_to_dataframe(data_file)
    df.to_csv("s3://nasa-neo-bucket//nasa-neo-data.csv")

    