from web3 import Web3
from web3.providers.rpc import HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware #Necessary for POA chains
from pathlib import Path
import json
from datetime import datetime
import pandas as pd


def scan_blocks(chain, start_block, end_block, contract_address, eventfile='deposit_logs.csv'):
    """
    chain - string (Either 'bsc' or 'avax')
    start_block - integer first block to scan
    end_block - integer last block to scan
    contract_address - the address of the deployed contract

    This function reads "Deposit" events from the specified contract, 
    and writes information about the events to the file "deposit_logs.csv"
    """
    if chain == 'avax':
        api_url = f"https://api.avax-test.network/ext/bc/C/rpc" #AVAX C-chain testnet

    if chain == 'bsc':
        api_url = f"https://data-seed-prebsc-1-s1.binance.org:8545/" #BSC testnet

    if chain in ['avax','bsc']:
        w3 = Web3(Web3.HTTPProvider(api_url))
        # inject the poa compatibility middleware to the innermost layer
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    else:
        w3 = Web3(Web3.HTTPProvider(api_url))

    # Minimal ABI (matches skeleton; token/recipient indexed, amount not indexed)
    DEPOSIT_ABI = json.loads('[ { "anonymous": false, "inputs": [ { "indexed": true, "internalType": "address", "name": "token", "type": "address" }, { "indexed": true, "internalType": "address", "name": "recipient", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Deposit", "type": "event" }]')
    contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=DEPOSIT_ABI)

    arg_filter = {}  # keep as in skeleton; you can add topics later if desired

    # Allow "latest" strings as in skeleton
    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    # Safety caps
    latest = w3.eth.get_block_number()
    if isinstance(end_block, int) and end_block > latest:
        end_block = latest

    if end_block < start_block:
        print(f"Error end_block < start_block!")
        print(f"end_block = {end_block}")
        print(f"start_block = {start_block}")
        return 0

    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    total_found = 0
    rows = []

    def flush_rows():
        nonlocal rows
        if not rows:
            return
        df = pd.DataFrame(rows, columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
        # Write header only if file does not exist
        if Path(eventfile).exists():
            df.to_csv(eventfile, mode='a', header=False, index=False)
        else:
            df.to_csv(eventfile, index=False)
        print(f"Wrote {len(rows)} row(s) to {eventfile}")
        rows = []

    # Small window: single filter call
    if end_block - start_block < 30:
        event_filter = contract.events.Deposit.create_filter(
            from_block=start_block,
            to_block=end_block,
            argument_filters=arg_filter
        )
        events = event_filter.get_all_entries()
        # >>>>>> FILLED TODO: collect & write
        for event in events:
            token = event.args['token']
            recipient = event.args['recipient']
            amount = int(event.args['amount'])
            tx_hash = event.transactionHash.hex()
            address = event.address
            rows.append([chain, token, recipient, amount, tx_hash, address])

        total_found += len(events)
        flush_rows()
    else:
        # Large window: scan block-by-block, as in skeleton
        for block_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.create_filter(
                from_block=block_num,
                to_block=block_num,
                argument_filters=arg_filter
            )
            events = event_filter.get_all_entries()
            # >>>>>> FILLED TODO: collect per block
            for event in events:
                token = event.args['token']
                recipient = event.args['recipient']
                amount = int(event.args['amount'])
                tx_hash = event.transactionHash.hex()
                address = event.address
                rows.append([chain, token, recipient, amount, tx_hash, address])

            total_found += len(events)
            # Write frequently to avoid memory growth on big ranges
            if len(rows) >= 1000:
                flush_rows()

        # Final flush after loop
        flush_rows()

    print(f"Done. Total Deposit events found: {total_found}")
    return total_found
