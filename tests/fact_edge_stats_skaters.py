import os
import time
import pandas as pd
from nhlpy import NHLClient

def get_ana_skater_schema_and_data(season: str = "20242025"):
    """
    Fetches raw EDGE data for ANA skaters and flattens dictionaries 
    into a primary Pandas DataFrame.
    """
    client = NHLClient()
    team_abbr = "ANA"
    all_skater_records = []
    
    try:
        print(f"1. Fetching {season} roster for {team_abbr}...")
        roster = client.teams.team_roster(team_abbr=team_abbr, season=season)
        
        # Combine forwards and defensemen (skaters)
        skaters = roster.get("forwards", []) + roster.get("defensemen", [])
        total_skaters = len(skaters)
        print(f"   Found {total_skaters} skaters on the roster.")
        
        print("\n2. Pulling live EDGE profiles (Looping API calls)...")
        for idx, skater in enumerate(skaters, start=1):
            player_id = skater.get("id")
            first_name = skater.get("firstName", {}).get("default", "")
            last_name = skater.get("lastName", {}).get("default", "")
            player_name = f"{first_name} {last_name}".strip()
            
            if not player_id:
                continue
                
            print(f"   [{idx}/{total_skaters}] Extracting: {player_name}")
            
            try:
                # API Call to EDGE module
                raw_edge_data = client.edge.skater_detail(player_id=player_id, season=season)
                
                if raw_edge_data:
                    # Inject basic metadata into the dictionary root context
                    raw_edge_data["meta_player_name"] = player_name
                    raw_edge_data["meta_position"] = skater.get("positionCode")
                    all_skater_records.append(raw_edge_data)
                    
            except Exception as player_err:
                print(f"    Could not fetch data for {player_name}: {player_err}")
            
            # Rate limiting safety buffer
            time.sleep(0.2)
            
        if not all_skater_records:
            print(" No EDGE data was returned for any rostered skaters.")
            return None

        print("\n3. Flaying structural JSON dictionaries into an initial DataFrame...")
        df = pd.json_normalize(all_skater_records)
        
        print("\n========================================================")
        print("            INITIAL UNPACKED DATAFRAME INFO             ")
        print("========================================================")
        print(df.info(verbose=False)) # Compact schema preview before arrays are broken out
        return df

    except Exception as e:
        print(f" Critical pipeline failure: {e}")
        return None

def flatten_list_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds columns containing nested lists and expands them into distinct 
    flat wide columns (handles both lists of scalars and lists of dicts).
    """
    df_flat = df.copy()
    
    # Locate column footprints containing actual array lists
    list_cols = [col for col in df_flat.columns if df_flat[col].apply(lambda x: isinstance(x, list)).any()]
    
    for col in list_cols:
        print(f"   -> Flattening nested array column: {col}")
        
        # Look at the first non-empty list item to see if it's a list of dictionaries or list of scalars
        sample_list = df_flat[col].dropna().values
        is_list_of_dicts = len(sample_list) > 0 and len(sample_list[0]) > 0 and isinstance(sample_list[0][0], dict)
        
        if is_list_of_dicts:
            # --- STRATEGY A: For lists of DICTIONARIES (like speed bands) ---
            expanded = pd.json_normalize(df_flat[col].explode())
            # Use matching list comprehension lengths to cleanly repeat the parent index
            expanded.index = [idx for idx, row in df_flat[col].items() for _ in range(len(row) if isinstance(row, list) else 1)]
            
            expanded['sub_idx'] = expanded.groupby(expanded.index).cumcount()
            pivoted = expanded.pivot(columns='sub_idx')
            pivoted.columns = [f"{col}.{sub_idx}.{prop}" for prop, sub_idx in pivoted.columns]
            df_flat = df_flat.join(pivoted).drop(columns=[col])
            
        else:
            # --- STRATEGY B: For lists of SCALARS (like your seasons list) ---
            # pd.DataFrame(....tolist()) instantly blows out an array into horizontal columns
            unpacked_scalars = pd.DataFrame(df_flat[col].tolist(), index=df_flat.index)
            # Name them cleanly: e.g., seasonsWithEdgeStats.0, seasonsWithEdgeStats.1
            unpacked_scalars.columns = [f"{col}.{i}" for i in unpacked_scalars.columns]
            
            df_flat = df_flat.join(unpacked_scalars).drop(columns=[col])
        
    return df_flat

if __name__ == "__main__":
    # 1. Fetch raw flattened-dict data frame
    ana_df = get_ana_skater_schema_and_data(season="20242025")
    
    if ana_df is not None and not ana_df.empty:
        print("\n4. Unpacking deep nested lists to flat column metrics...")
        final_flat_df = flatten_list_columns(ana_df)
        
        # 5. Handle directory constraints safely
        target_path = "tests\\CSV\\fact_edge_stats.csv"
        directory = os.path.dirname(target_path)
        
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            print(f"   Created missing directory path: {directory}")
            
        # 6. Save completely tabularized CSV data
        final_flat_df.to_csv(target_path, index=False)
        print(f"\n Success! Fully flattened data written directly to: {target_path}")
        print(f"Final Data Matrix Shape: {final_flat_df.shape[0]} rows (Skaters) x {final_flat_df.shape[1]} columns.")