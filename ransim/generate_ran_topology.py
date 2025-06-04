"""
Author: Dinesh Lakshmanan
Date: 2025-06-04

Notes:
This script generates a static network topology for a RAN (Radio Access Network) simulator.
It creates a 'cell_config.json' file containing configuration details for 2000 cells.

The generated topology adheres to the following rules:
- **Fixed Cell Properties**: Each cell has a unique ID, max capacity, geographical coordinates (latitude/longitude),
  assigned frequency bands, an area type (e.g., residential, industrial), and a city.
  These properties remain constant across all simulation intervals once loaded by the simulator.
- **Bidirectional Adjacencies**: If Cell A lists Cell B as an adjacent cell, Cell B will also list Cell A as adjacent.
- **Geographical Proximity**: Adjacent cells are prioritized based on their real-world geographical distance
  (calculated using the Haversine formula) rather than just numerical ID closeness.
- **Same City Constraint**: All adjacent cells to a given cell will belong to the same city as that cell.
- **Band Matching**: Adjacent cells are guaranteed to share at least one common frequency band,
  which is crucial for realistic handover scenarios.
- **Variable Neighbor Count (1-3 Max)**: Each cell aims to have a random number of adjacent cells between 1 and 3 (inclusive).
  This introduces diversity in network density.

This script should be run once to generate the 'cell_config.json' file. The RAN simulator
(e.g., 'ransim.py') should then be configured to load this static JSON file
at its startup to ensure consistent network topology across simulation runs.
"""


import random
import json
from collections import defaultdict
import math

# --- Configuration for cell properties ---
BAND_FREQUENCY_MAP = {
    'Band 29': 700,
    'Band 26': 850,
    'Band 71': 600,
    'Band 66': '1700-2100'
}

CITIES = [
    "Frisco", "Plano", "McKinney", "Denton", "Allen", "Prosper", "Celina",
    "The Colony", "Little Elm", "Anna", "Addison", "Carrollton", "Coppell",
    "Farmers Branch", "Irving", "Mesquite", "Grand Prairie", "Southlake",
    "Keller", "Argyle", "Northlake", "Westlake"
]

USAGE_PATTERNS = {
    'industrial': {'weekdays': {'day': (0.7, 0.9), 'night': (0.1, 0.3)},
                   'weekends': {'day': (0.4, 0.6), 'night': (0.1, 0.3)}},
    'commercial': {'weekdays': {'day': (0.3, 0.5), 'night': (0.6, 0.9)},
                   'weekends': {'day': (0.4, 0.6), 'night': (0.7, 0.9)}},
    'rural': {'weekdays': {'day': (0.1, 0.2), 'night': (0.1, 0.2)},
              'weekends': {'day': (0.1, 0.2), 'night': (0.1, 0.2)}},
    'residential': {'weekdays': {'day': (0.3, 0.2), 'night': (0.7, 0.9)},
                    'weekends': {'day': (0.4, 0.6), 'night': (0.5, 0.8)}}
}

# --- Note: MAX_ADJACENT_CELLS is now dynamic per cell (between 1 and 3)
# It's no longer a single global constant for the strict limit.
# However, for overall statistics and clarity in print statements, we can define
# a theoretical maximum to refer to in the output.
THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL = 3
# --- End Configuration ---

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371 # Radius of Earth in kilometers

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

def generate_static_cell_config(num_cells_param=2000, seed=None):
    """
    Generates cell configurations with static properties, ensuring:
    1. Bidirectional adjacencies.
    2. Adjacent cells are in the same city.
    3. Adjacent cells share at least 1 common band.
    4. Each cell aims for a random number of adjacent cells between 1 and 3.
    5. Adjacency preference for GEOGRAPHICALLY CLOSER cells (based on lat/lon).

    Args:
        num_cells_param (int): The total number of cells to generate.
        seed (int, optional): A seed for the random number generator to ensure
                              reproducible results. If None, uses current system time.

    Returns:
        tuple: A tuple containing:
            - dict: A dictionary with a "cells" key, containing a list of cell configuration dictionaries.
            - dict: A mapping from cell_id to its assigned city.
    """
    if seed is not None:
        random.seed(seed)

    # --- Step 1: Generate initial cell properties and create lookup maps ---
    initial_cells_data_list = []
    cell_id_to_data_map = {}
    city_to_cell_ids_map = {city: [] for city in CITIES}
    cell_id_to_city_map = {}
    
    # New: Assign a desired number of adjacent cells (1, 2, or 3) for each cell
    desired_adj_counts = {} 

    ids = list(range(num_cells_param))

    for i, cell_id in enumerate(ids):
        assigned_city = CITIES[i % len(CITIES)]
        cell_id_to_city_map[cell_id] = assigned_city
        city_to_cell_ids_map[assigned_city].append(cell_id)
        
        # Assign desired adjacent cell count for this cell
        desired_adj_counts[cell_id] = random.randint(1, THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL)

        cell_data = {
            'cell_id': int(cell_id),
            'max_capacity': random.randint(50, 100),
            'lat': round(random.uniform(37.7749, 37.8049), 6),
            'lon': round(random.uniform(-122.4194, -122.3894), 6),
            'bands': random.sample(list(BAND_FREQUENCY_MAP.keys()), k=random.randint(1, 3)),
            'area_type': random.choice(list(USAGE_PATTERNS.keys())),
            'city': assigned_city,
        }
        initial_cells_data_list.append(cell_data)
        cell_id_to_data_map[cell_id] = cell_data

    # Initialize a list of sets for the final adjacent cells for each cell.
    adjacencies_sets = [set() for _ in range(num_cells_param)]

    # --- Step 2: Build a master list of all *potential* valid bidirectional links ---
    potential_links = set()

    for i in range(num_cells_param):
        cell_A_id = ids[i]
        cell_A_data = cell_id_to_data_map[cell_A_id]
        cell_A_bands = set(cell_A_data['bands'])
        cell_A_city = cell_A_data['city']
        cell_A_lat, cell_A_lon = cell_A_data['lat'], cell_A_data['lon']

        for j in range(i + 1, num_cells_param):
            cell_B_id = ids[j]
            cell_B_data = cell_id_to_data_map[cell_B_id]
            cell_B_bands = set(cell_B_data['bands'])
            cell_B_city = cell_B_data['city']
            cell_B_lat, cell_B_lon = cell_B_data['lat'], cell_B_data['lon']

            if cell_A_city != cell_B_city:
                continue

            common_bands_count = len(cell_A_bands.intersection(cell_B_bands))
            if common_bands_count == 0:
                continue

            distance_km = haversine_distance(cell_A_lat, cell_A_lon, cell_B_lat, cell_B_lon)
            
            potential_links.add((cell_A_id, cell_B_id, distance_km, common_bands_count))

    # Convert the set of links to a list and sort it for prioritization:
    # 1. Smallest geographical distance (closer cells first).
    # 2. More common bands (descending order).
    sorted_potential_links = sorted(list(potential_links), key=lambda x: (x[2], -x[3]))

    # --- Step 3: Iteratively build the bidirectional graph aiming for desired counts ---
    for cell_A_id, cell_B_id, _, _ in sorted_potential_links:
        # Check if both cells can accept a new link without exceeding their *desired* adjacent count
        if len(adjacencies_sets[cell_A_id]) < desired_adj_counts[cell_A_id] and \
           len(adjacencies_sets[cell_B_id]) < desired_adj_counts[cell_B_id]:
            # Add the link bidirectionally
            adjacencies_sets[cell_A_id].add(cell_B_id)
            adjacencies_sets[cell_B_id].add(cell_A_id)

    # --- Step 4: Finalize cell data ---
    final_cells_config = []
    for cell_data in initial_cells_data_list:
        cell_id = cell_data['cell_id']
        cell_data['adjacent_cells'] = sorted(list(adjacencies_sets[cell_id]))
        final_cells_config.append(cell_data)

    return {"cells": final_cells_config}, cell_id_to_city_map

if __name__ == "__main__":
    fixed_seed = 42
    num_cells = 2000 # Define num_cells here

    print(f"Using random seed: {fixed_seed} for reproducible cell configuration.")

    cell_configuration, cell_id_to_city_map = generate_static_cell_config(num_cells_param=num_cells, seed=fixed_seed)

    file_name = "cell_config.json"
    with open(file_name, 'w') as f:
        json.dump(cell_configuration, f, indent=2)

    print(f"Successfully generated '{file_name}' with configuration for {len(cell_configuration['cells'])} cells.")
    print("This file contains a FIXED network topology with the following rules:")
    print(f"  - **Bidirectional Adjacencies:** If Cell A is adjacent to Cell B, Cell B is adjacent to Cell A.")
    print(f"  - **Variable Neighbors (1-{THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL} max):** Each cell aims for 1 to {THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL} adjacent cells.")
    print(f"  - **Same City:** Adjacent cells are always in the same city.")
    print(f"  - **Band Matching:** Adjacent cells share at least one common frequency band.")
    print(f"  - **Geographical Proximity:** Adjacency preference is given to physically closer cells (based on lat/lon).")

    # --- Verification Sample ---
    print("\n--- Verification Sample ---")
    sample_cell_ids_to_check = [0, 1, 10, 50, 100, 500, 1000, 1500, 1999]
    
    cells_by_id = {cell['cell_id']: cell for cell in cell_configuration['cells']}

    total_cells_verified = 0
    errors_in_sample = 0
    
    for cell_id_to_check in sample_cell_ids_to_check:
        if cell_id_to_check in cells_by_id:
            sample_cell = cells_by_id[cell_id_to_check]
            total_cells_verified += 1
            print(f"\n--- Checking Cell ID: {sample_cell['cell_id']} ---")
            print(f"  City: {sample_cell['city']}, Lat: {sample_cell['lat']:.6f}, Lon: {sample_cell['lon']:.6f}, Bands: {sample_cell['bands']}")
            print(f"  Adjacent Cells ({len(sample_cell['adjacent_cells'])} total): {sample_cell['adjacent_cells']}")
            
            # Check for the 0-3 max constraint on the sample cell itself (it can be 0 if no eligible partners)
            if not (0 <= len(sample_cell['adjacent_cells']) <= THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL):
                print(f"    !!! ERROR: Adjacent cell count ({len(sample_cell['adjacent_cells'])}) exceeds max {THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL} !!!")
                errors_in_sample += 1

            if not sample_cell['adjacent_cells']:
                print("  (This cell has no adjacent cells meeting all criteria and degree limits.)")
                continue

            for adj_id in sample_cell['adjacent_cells']:
                adj_cell = cells_by_id.get(adj_id)
                if adj_cell:
                    # 1. Verify Bidirectionality
                    is_bidirectional = sample_cell['cell_id'] in adj_cell['adjacent_cells']
                    if not is_bidirectional: errors_in_sample += 1
                    print(f"    - Adj Cell ID {adj_id}: City={adj_cell['city']}, Lat={adj_cell['lat']:.6f}, Lon={adj_cell['lon']:.6f}, Bands={adj_cell['bands']}")
                    print(f"      Bidirectional: {is_bidirectional} {'(OK)' if is_bidirectional else '(ERROR)'}")

                    # 2. Verify Same City
                    same_city = (adj_cell['city'] == sample_cell['city'])
                    if not same_city: errors_in_sample += 1
                    print(f"      Same City: {same_city} {'(OK)' if same_city else '(ERROR)'}")

                    # 3. Verify Band Match
                    common_bands = set(sample_cell['bands']).intersection(set(adj_cell['bands']))
                    band_match_ok = (len(common_bands) >= 1)
                    if not band_match_ok: errors_in_sample += 1
                    print(f"      Common Bands: {list(common_bands)}, Match OK: {band_match_ok} {'(OK)' if band_match_ok else '(ERROR)'}")

                    # 4. Verify Adjacent cell's count also <= theoretical max
                    if not (len(adj_cell['adjacent_cells']) <= THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL):
                         print(f"      !!! ERROR: Neighbor {adj_id} has {len(adj_cell['adjacent_cells'])} adjacent cells, exceeding max {THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL} !!!")
                         errors_in_sample += 1
                    
                    # 5. Show Geographical Distance (Informational)
                    dist = haversine_distance(sample_cell['lat'], sample_cell['lon'], adj_cell['lat'], adj_cell['lon'])
                    print(f"      Geographical Distance: {dist:.2f} km")

                else:
                    print(f"    - Adj Cell ID {adj_id}: (ERROR: Neighbor cell data not found!)")
                    errors_in_sample += 1
        else:
            print(f"\n--- Cell ID {cell_id_to_check} not found in generated config. ---")

    # Overall degree statistics for the entire graph
    all_final_degrees = [len(cell['adjacent_cells']) for cell in cell_configuration['cells']]
    min_degree = min(all_final_degrees) if all_final_degrees else 0
    max_degree = max(all_final_degrees) if all_final_degrees else 0
    avg_degree = sum(all_final_degrees) / len(all_final_degrees) if all_final_degrees else 0
    
    print(f"\n--- Overall Graph Degree Statistics (for all {num_cells} cells) ---")
    print(f"  Minimum adjacent cells per cell: {min_degree}")
    print(f"  Maximum adjacent cells per cell: {max_degree} (should be <= {THEORETICAL_MAX_ADJACENT_CELLS_PER_CELL})")
    print(f"  Average adjacent cells per cell: {avg_degree:.2f}")

    if errors_in_sample == 0 and total_cells_verified > 0:
        print("\nVerification completed. No critical errors found in sampled cells.")
    elif total_cells_verified == 0:
        print("\nNo cells were checked in verification sample.")
    else:
        print(f"\nVerification completed with {errors_in_sample} errors found in sample checks. Review warnings/errors above.")