#%%
import sys
import os

# Añade el directorio padre al sys.path
sys.path.append(os.path.abspath("/eos/home-d/dcostasr/SWAN_projects/2025_data"))
sys.path.append(os.path.abspath("/eos/home-d/dcostasr/SWAN_projects/NiCf/offline_trigger"))

from src.read_data import process_and_write_parts, load_concatenated, read_mpmt_offsets
from wcte.brbtools import sort_run_files, get_part_files
#%%
files = input("Do you want calibrated or uncalibrated data?: ")
run = input("Which run do you want to process? (####) --> ")

print("Reading run and part files...")
if files == "uncalibrated":
    run_files  = sort_run_files(f"/eos/experiment/wcte/data/2025_commissioning/offline_data/{run}/WCTE_offline_R{run}S*P*.root")

elif files == "calibrated":
    run_files = sort_run_files(f"/eos/experiment/wcte/data/2025_commissioning/processed_offline_data/production_v0/{run}/WCTE_offline_R{run}S*P*.root")

part_files = get_part_files(run_files)
#%%
mpmt_map   = read_mpmt_offsets("/eos/home-d/dcostasr/SWAN_projects/AmBe/data/mpmt_tof_pos1.json")
#%%

print(f"Run {run} has {len(part_files)} part files. You can read a single part, multiple, or all")
print(f"    Example_1: 3 (Single run file)")
print(f"    Example_2: 0,1,2,3,4 (List of runs)")
print(f"    Example_3: all (Read all runs. This might take a while.)")

parts_to_process = input("Which parts do you want to read? --> ")

# Case: "all"
if parts_to_process.strip().lower() == "all":
    if files == "calibrated":
        process_and_write_parts(run_files, part_files, mpmt_map=mpmt_map, max_slot=106, max_pos=19, outdir=f"tmp_parquet/{run}_calibrated/")
    
    elif files == "uncalibrated":
        process_and_write_parts(run_files, part_files, mpmt_map, max_slot=106, max_pos=19, outdir=f"tmp_parquet/{run}/")

# Case: input just one number, e.g. "3"
elif parts_to_process.isdigit():
    part_idx = int(parts_to_process)
    if files == "calibrated":
        process_and_write_parts(run_files, [part_idx], mpmt_map=mpmt_map, max_slot=106, max_pos=19, outdir=f"tmp_parquet/{run}_calibrated/")
    
    elif files == "uncalibrated":
        process_and_write_parts(run_files, [part_idx], mpmt_map, max_slot=106, max_pos=19, outdir=f"tmp_parquet/{run}/")

# Case: comma separated list of numbers, e.g. "1,2,5"
else:
    try:
        parts_list = [int(p.strip()) for p in parts_to_process.split(',')]
        
        # Duplicates verification
        duplicates = [x for x in set(parts_list) if parts_list.count(x) > 1]
        if duplicates:
            raise ValueError(f"Duplicated values found in the list: {duplicates}")

        if files == "calibrated":
            process_and_write_parts(run_files, [part_files[i] for i in parts_list], mpmt_map=mpmt_map, max_slot=106, max_pos=19, outdir=f"tmp_parquet/{run}_calibrated_test/")
        
        elif files == "uncalibrated":
            process_and_write_parts(run_files, [part_files[i] for i in parts_list], mpmt_map, max_slot=106, max_pos=19, outdir=f"tmp_parquet/{run}/")
        
    except ValueError as e:
        raise ValueError(f"Invalid input. {e}")
