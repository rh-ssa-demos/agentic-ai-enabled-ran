import random
import json

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
# --- End Configuration ---

def generate_static_cell_config(num_cells=2000, seed=None):
    """
    Generates cell configurations with static properties.
    Adjacent cells are selected from the same city as the originating cell.
    Adjacencies are unidirectional.

    Args:
        num_cells (int): The total number of cells to generate.
        seed (int, optional): A seed for the random number generator to ensure
                              reproducible results. If None, uses current system time.

    Returns:
        tuple: A tuple containing:
            - dict: A dictionary with a "cells" key, containing a list of cell configuration dictionaries.
            - dict: A mapping from cell_id to its assigned city.
    """
    if seed is not None:
        random.seed(seed) # Set the seed for reproducibility

    initial_cells_data = []
    city_to_cell_ids_map = {city: [] for city in CITIES}
    cell_id_to_city_map = {}

    ids = list(range(num_cells))

    for i, cell_id in enumerate(ids):
        assigned_city = CITIES[i % len(CITIES)]
        cell_id_to_city_map[cell_id] = assigned_city
        city_to_cell_ids_map[assigned_city].append(cell_id)

        initial_cells_data.append({
            'cell_id': int(cell_id),
            'max_capacity': random.randint(50, 100),
            'lat': round(random.uniform(37.7749, 37.8049), 6),
            'lon': round(random.uniform(-122.4194, -122.3894), 6),
            'bands': random.sample(list(BAND_FREQUENCY_MAP.keys()), k=random.randint(1, 3)),
            'area_type': random.choice(list(USAGE_PATTERNS.keys())),
            'city': assigned_city
        })

    final_cells_config = []
    for cell_data in initial_cells_data:
        current_cell_id = cell_data['cell_id']
        current_cell_city = cell_data['city']

        potential_adjacent_in_city = [
            cid for cid in city_to_cell_ids_map[current_cell_city]
            if cid != current_cell_id
        ]

        num_adjacent_to_pick = random.randint(1, 3)

        if len(potential_adjacent_in_city) < num_adjacent_to_pick:
            adjacent = potential_adjacent_in_city
        else:
            adjacent = random.sample(potential_adjacent_in_city, num_adjacent_to_pick)

        cell_data['adjacent_cells'] = sorted(adjacent)
        final_cells_config.append(cell_data)

    return {"cells": final_cells_config}, cell_id_to_city_map

if __name__ == "__main__":
    # --- IMPORTANT ---
    # Use a specific seed here to ensure the cell_config.json is ALWAYS the same
    # every time you run THIS SCRIPT. Change the seed for a different fixed topology.
    fixed_seed = 42 # You can pick any integer you like
    print(f"Using random seed: {fixed_seed} for reproducible cell configuration.")

    cell_configuration, cell_id_to_city_map = generate_static_cell_config(num_cells=2000, seed=fixed_seed)

    file_name = "cell_config.json"
    with open(file_name, 'w') as f:
        json.dump(cell_configuration, f, indent=2)

    print(f"Successfully generated '{file_name}' with configuration for 2000 cells.")
    print("Adjacent cells are guaranteed to be within the same city as the originating cell.")
    print("Adjacencies remain unidirectional.")
    print(f"This '{file_name}' file contains the FIXED topology data.")

    # --- Verification Sample ---
    print("\n--- Verification Sample ---")
    sample_cell = None
    for cell in cell_configuration['cells']:
        if cell['adjacent_cells']:
            sample_cell = cell
            break

    if sample_cell:
        print(f"Checking Cell ID: {sample_cell['cell_id']} in City: {sample_cell['city']}")
        print(f"Adjacent Cells: {sample_cell['adjacent_cells']}")
        for adj_id in sample_cell['adjacent_cells']:
            adj_city = cell_id_to_city_map[adj_id]
            print(f"  - Adjacent Cell ID {adj_id} is in City: {adj_city}")
            if adj_city != sample_cell['city']:
                print(f"    !!! ERROR: Mismatching city found for adjacent cell {adj_id} !!!")
            else:
                print(f"    (Confirmed: {adj_id} is in the same city as {sample_cell['cell_id']})")
    else:
        print("No cells found with adjacent cells to verify (this might happen if cities are very small).")