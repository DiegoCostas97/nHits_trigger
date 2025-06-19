import glob
import os
import uproot
import json

import awkward as ak
import numpy   as np

from tqdm import tqdm

def read_file(files, parts, evt_offset):
    tree = uproot.open(files[parts] + ":WCTEReadoutWindows")
    
    # Read the variables
    file_hit_card_ids     = ak.values_astype(tree["hit_mpmt_card_ids"]   .array(), np.int16)
    file_hit_channel_ids  = ak.values_astype(tree["hit_pmt_channel_ids"] .array(), np.int8)
    file_hit_times        = ak.values_astype(tree["hit_pmt_times"]       .array(), np.float64)
    file_hit_charges      = ak.values_astype(tree["hit_pmt_charges"]     .array(), np.float64)
    file_hit_slot_ids     = ak.values_astype(tree["hit_mpmt_slot_ids"]   .array(), np.int16)
    file_hit_position_ids = ak.values_astype(tree["hit_pmt_position_ids"].array(), np.int16)
    window_time           = ak.values_astype(tree["window_time"]         .array(), np.float64)
    event_number          = ak.values_astype(tree["event_number"]        .array(), np.int64)

    # Number of hits per event
    n_hits_per_event = ak.num(file_hit_card_ids)

    # Create the repeated event array
    file_event_number = np.repeat(event_number, n_hits_per_event) + evt_offset
    file_window_time  = np.repeat(window_time, n_hits_per_event)
    file_event_number = ak.unflatten(file_event_number, n_hits_per_event)
    file_window_time  = ak.unflatten(file_window_time, n_hits_per_event)

    return file_hit_card_ids, file_hit_channel_ids, file_hit_times, file_hit_charges, file_event_number, file_window_time, file_hit_slot_ids, file_hit_position_ids

def process_and_write_parts(run_files, good_parts, mpmt_map, max_slot, max_pos, outdir="tmp_parquet"):
    os.makedirs(outdir, exist_ok=True)

    # Remove Directories If Run Is Repeated
    for f in glob.glob(f"{outdir}/*.parquet"):
        os.remove(f)

    evt_offset = 0

    for i, part in enumerate(tqdm(good_parts, desc="Processing parts")):
        file_hit_card_ids, file_hit_channel_ids, file_hit_times, file_hit_charges, file_event_number, file_window_time, file_hit_slot_ids, file_hit_position_ids = read_file(run_files, part, evt_offset)

        if mpmt_map != None:
            # Build lookup
            lookup = np.zeros((max_slot + 1, max_pos + 1))
            for (card, chan), shift in mpmt_map.items():
                lookup[card, chan] = shift

            # Hit Times Correction
            flat_slot_ids     = ak.ravel(file_hit_slot_ids)
            flat_pos_ids      = ak.ravel(file_hit_position_ids)
            flat_corrections  = lookup[flat_slot_ids, flat_pos_ids]
            corrections       = ak.unflatten(flat_corrections, ak.num(file_hit_card_ids))

            term1 = file_hit_times
            # term2 = (file_event_number // 512) * (2**35)
            # term3 = ((file_event_number % 512 == 511) & (file_hit_times < 2**34)) * (2**35)
            term4 = file_window_time
            # corrected_times = term1 + term2 + term3 + corrections
            corrected_times = term1 + term4 - corrections 
        
        elif mpmt_map == None:

            term1 = file_hit_times
            # term2 = (file_event_number // 512) * (2**35)
            # term3 = ((file_event_number % 512 == 511) & (file_hit_times < 2**34)) * (2**35)
            term4 = file_window_time
            # corrected_times = term1 + term2 + term3 + corrections
            corrected_times = term1 + term4
            
        # Save to Disk
        ak.to_parquet(file_hit_card_ids,     f"{outdir}/card_ids_part{i}.parquet")
        ak.to_parquet(file_hit_channel_ids,  f"{outdir}/channel_ids_part{i}.parquet")
        ak.to_parquet(file_hit_slot_ids,     f"{outdir}/slot_ids_part{i}.parquet")
        ak.to_parquet(file_hit_position_ids, f"{outdir}/position_ids_part{i}.parquet")
        ak.to_parquet(file_hit_charges,      f"{outdir}/charges_part{i}.parquet")
        ak.to_parquet(file_event_number,     f"{outdir}/event_number_part{i}.parquet")
        ak.to_parquet(file_window_time,      f"{outdir}/window_time_part{i}.parquet")
        ak.to_parquet(corrected_times,       f"{outdir}/hit_times_part{i}.parquet")

        evt_offset = file_event_number[-1][-1] + 1

def load_concatenated(outdir="./tmp_parquet"):
    def load(pattern):
        files = sorted(glob.glob(os.path.join(outdir, pattern)))
        return ak.concatenate([ak.from_parquet(f) for f in files], axis=0)

    return {
        "card_ids":     load("card_ids_part*.parquet"),
        "channel_ids":  load("channel_ids_part*.parquet"),
        "slot_ids":     load("slot_ids_part*.parquet"),
        "position_ids": load("position_ids_part*.parquet"),
        "charges":      load("charges_part*.parquet"),
        "event_number": load("event_number_part*.parquet"),
        "hit_times":    load("hit_times_part*.parquet"),
        "window_time":  load("window_time_part*.parquet")
    }

def read_mpmt_offsets(path='/eos/home-d/dcostasr/SWAN_projects/NiCf/offline_trigger/mmc_map_R1609.json'):
    with open(path) as f:
        mcc_map = json.load(f)

        d = {}
        for k,v in zip(mcc_map.keys(), mcc_map.values()):
            card, channel = [int(i) for i in str(int(k)/100).split(".")]
            d[(card, channel)] = v

    return d

def read_parquet(data, mask=True):
    
    if mask:
        run_cards = data["card_ids"]
        mask      = (run_cards != 130) & (run_cards != 131) & (run_cards != 132)
        run_cards     = data["card_ids"]    [mask]
        run_channels  = data["channel_ids"] [mask]
        run_slots     = data["slot_ids"]    [mask]
        run_positions = data["position_ids"][mask]
        run_times     = data["hit_times"]   [mask]   
        run_events    = data["event_number"][mask]
        run_charges   = data["charges"]     [mask]
        run_window_times = data["window_time"][mask]

    else:
        run_cards     = data["card_ids"]
        run_channels  = data["channel_ids"] 
        run_slots     = data["slot_ids"]    
        run_positions = data["position_ids"]  
        run_times     = data["hit_times"]      
        run_events    = data["event_number"]
        run_charges   = data["charges"]   
        run_window_times = data["window_time"]   

    return run_cards, run_channels, run_slots, run_positions, run_times, run_events, run_charges, run_window_times

def nHits(mode, hit_times, w, thresh_min, thresh_max, pre_window, post_window, jump, event=0, progress_bar=True):
    
    # SINGLE EVENT MODE
    if mode=="single_event":
        triggered_hits_index = {}
        triggered_hits_time  = {}

        ht = ak.to_numpy(hit_times[event])

        # Sliding window logic
        ends   = ht + w
        right  = np.searchsorted(ht, ends, side="left")
        left   = np.arange(len(ht))
        counts = right - left

        # Get the indices of all the hit times that triggered the nHits algorithm
        trigger_indices = np.where((counts >= thresh_min) & (counts < thresh_max))[0]

        event_hits = []
        event_hit_times = []
        last_trigger_time = -np.inf  # First trigger always need to exist

        # Search for the rest of the hits in the trigger
        for idx in trigger_indices:
            # Define actual sliding window that triggered
            i_start = idx
            i_end = right[idx]  # one past the last hit in window
            window_hits = ht[i_start:i_end]

            first_hit_time = window_hits[0]
            last_hit_time  = window_hits[-1]

            # Avoid overlap due to dead time
            if first_hit_time < last_trigger_time + jump:
                continue

            # Expand window
            t_min = first_hit_time - pre_window
            t_max = last_hit_time + post_window

            indices_in_window   = np.where((ht >= t_min) & (ht <= t_max))[0]
            hit_times_in_window = ht[(ht >= t_min) & (ht <= t_max)]
            event_hits.append(indices_in_window)
            event_hit_times.append(hit_times_in_window)

            last_trigger_time = first_hit_time  # or maybe last_hit_time 

        # Update dictionary
        if len(event_hits) > 0:
            triggered_hits_index[event] = event_hits
            triggered_hits_time[event]  = event_hit_times

    # MULTIPLE EVENTS MODE
    elif mode == "multiple_events":
        nevents = len(hit_times)
        triggered_hits_index = {}
        triggered_hits_time  = {}

        for event in tqdm(range(nevents), total=nevents, leave=progress_bar):
            ht = ak.to_numpy(hit_times[event])
            if len(ht) == 0:
                continue
            
            # Sliding window logic
            ends   = ht + w
            right  = np.searchsorted(ht, ends, side="left")
            left   = np.arange(len(ht))
            counts = right - left

            # Get the indices of all the hit times that triggered the nHits algorithm
            trigger_indices = np.where((counts >= thresh_min) & (counts < thresh_max))[0]
            if len(trigger_indices) == 0: # Skip if no triggers in event
                continue

            event_hits = []
            event_hit_times = []
            last_trigger_time = -np.inf  # First trigger always need to exist

            # Search for the rest of the hits in the trigger
            for idx in trigger_indices:
                # Define actual sliding window that triggered
                i_start = idx
                i_end = right[idx]  # one past the last hit in window
                window_hits = ht[i_start:i_end]

                first_hit_time = window_hits[0]
                last_hit_time  = window_hits[-1]

                # Avoid overlap due to dead time
                if first_hit_time < last_trigger_time + jump:
                    continue

                # Expand window
                t_min = first_hit_time - pre_window
                t_max = last_hit_time + post_window

                indices_in_window = np.where((ht >= t_min) & (ht <= t_max))[0]
                hit_times_in_window = ht[(ht >= t_min) & (ht <= t_max)]
                event_hits.append(indices_in_window)
                event_hit_times.append(hit_times_in_window)

                last_trigger_time = first_hit_time  # or maybe last_hit_time 

            # Update dictionary
            if len(event_hits) > 0:
                triggered_hits_index[event] = event_hits
                triggered_hits_time[event]  = event_hit_times
    else:
        print("enter a valid mode name")

    return triggered_hits_index, triggered_hits_time