import os
import time
import pandas as pd
from nhlpy import NHLClient

def get_ana_goalie_schema_and_data(season: str = "20242025"):
    """
    Fetches raw EDGE data for ANA goalies and flattens dictionaries 
    into a primary Pandas DataFrame.
    """
    client = NHLClient()
    team_abbr = "ANA"
    all_goalie_records = []
    
    try:
        print(f"1. Fetching {season} roster for {team_abbr}...")
        roster = client.teams.team_roster(team_abbr=team_abbr, season=season)
        
        # Isolate goalies only
        goalies = roster.get("goalies", [])
        total_goalies = len(goalies)
        print(f"   Found {total_goalies} goalies on the roster.")
        
        print("\n2. Pulling live Goalie EDGE profiles (Looping API calls)...")
        for idx, goalie in enumerate(goalies, start=1):
            player_id = goalie.get("id")
            first_name = goalie.get("firstName", {}).get("default", "")
            last_name = goalie.get("lastName", {}).get("default", "")
            player_name = f"{first_name} {last_name}".strip()
            
            if not player_id:
                continue
                
            print(f"   [{idx}/{total_goalies}] Extracting Goalie: {player_name}")
            
            try:
                # Specific endpoint designed for Goalie spatial and tracking metrics
                raw_edge_data = client.edge.goalie_save_percentage_detail(player_id=player_id, season=season)
                
                if raw_edge_data:
                    # Inject basic metadata into the dictionary root context
                    raw_edge_data["meta_player_name"] = player_name
                    raw_edge_data["meta_position"] = goalie.get("positionCode", "G")
                    all_goalie_records.append(raw_edge_data)
                    
            except Exception as player_err:
                print(f"    Could not fetch data for {player_name}: {player_err}")
            
            # Rate limiting safety buffer
            time.sleep(0.2)
            
        if not all_goalie_records:
            print(" No EDGE data was returned for any rostered goalies.")
            return None

        print("\n3. Flaying structural JSON dictionaries into an initial DataFrame...")
        df = pd.json_normalize(all_goalie_records)
        
        print("\n========================================================")
        print("            INITIAL UNPACKED DATAFRAME INFO             ")
        print("========================================================")
        print(df.info(verbose=False))
        return df

    except Exception as e:
        print(f" Critical pipeline failure: {e}")
        return None

def flatten_list_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds columns containing nested lists and expands them into distinct 
    flat wide columns. Gracefully handles lists of dicts AND lists of scalars.
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
            # STRATEGY A: For lists of DICTIONARIES (like shot segments)
            expanded = pd.json_normalize(df_flat[col].explode())
            # Cleanly build matching index repeats for the exploded records
            expanded.index = [idx for idx, row in df_flat[col].items() for _ in range(len(row) if isinstance(row, list) else 1)]
            
            expanded['sub_idx'] = expanded.groupby(expanded.index).cumcount()
            pivoted = expanded.pivot(columns='sub_idx')
            pivoted.columns = [f"{col}.{sub_idx}.{prop}" for prop, sub_idx in pivoted.columns]
            df_flat = df_flat.join(pivoted).drop(columns=[col])
            
        else:
            # STRATEGY B: For flat lists of scalars (like your seasons arrays)
            unpacked_scalars = pd.DataFrame(df_flat[col].tolist(), index=df_flat.index)
            unpacked_scalars.columns = [f"{col}.{i}" for i in unpacked_scalars.columns]
            df_flat = df_flat.join(unpacked_scalars).drop(columns=[col])
        
    return df_flat

if __name__ == "__main__":
    # 1. Fetch raw flattened-dict data frame
    ana_df = get_ana_goalie_schema_and_data(season="20242025")
    
    if ana_df is not None and not ana_df.empty:
        print("\n4. Unpacking deep nested lists to flat column metrics...")
        final_flat_df = flatten_list_columns(ana_df)
        
        # 5. Handle directory constraints safely
        target_path = "tests\\CSV\\fact_edge_stats_goalies.csv"
        directory = os.path.dirname(target_path)
        
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            print(f"   Created missing directory path: {directory}")
            
        # 6. Save completely tabularized Goalie CSV data
        final_flat_df.to_csv(target_path, index=False)
        print(f"\n Success! Fully flattened GOALIE data written directly to: {target_path}")
        print(f"Final Data Matrix Shape: {final_flat_df.shape[0]} rows (Goalies) x {final_flat_df.shape[1]} columns.")