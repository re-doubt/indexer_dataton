#!/use/bin/env python

def op_to_signed(opcode):
    return opcode if opcode < 0x80000000 else -1 * (0x100000000 - opcode)

SUPPORTED_OP_CODES = set(map(op_to_signed, [
    0x0f8a7ea5, # Jetton transfer
    0x178d4519, # Jetton mint
    0x595f07bc, # Jetton burn
    0x5fcc3d14, # NFT transfer
    0x487a8e81, # Telemint start auction
    0x371638ae, # Telemint cancel auction
    0x05138d91, # Telemint ownership assignment
    0x9c610de3, # DedustV2 ext out
    0xde1ddbcc, # storm execute_order
    0xcf90d618, # storm complete_order
    0x60dfc677, # storm update_position
    0x5d1b17b8, # storm update stop_loss position
    0x3475fdd2, # storm trade_notification
    0x6691fda5, # TON Raffles fairlaunch purchase
    0x256c5472, # TON Raffles fairlaunch reward
    0x00000009, # DAOLama extend loan
    0x5445efee, # Hipo tokens_minted
    0xf93bb43f, # Ston.fi payment request
    0x6c6c2080, # Getgems sale V3 price changing
]))
EVAA_ROUTER = 'EQC8rUZqR_pWV1BylWUlPNBzyiTYVoBEmQkMIQDZXICfnuRr'

"""
Checks if messages supported by parsers
Note: all messages discarded by this function wouldn't be parsed
"""
def message_supported(msg):
    if msg['op'] in SUPPORTED_OP_CODES:
        return True

    if msg['source'] == EVAA_ROUTER or msg['destination'] == EVAA_ROUTER:
        return True

    comment = msg.get('comment', None)
    if comment and len(comment.strip()) > 0 and comment.startswith("""data:application/json,{"p":"ton-20","op":"transfer","""):
        return True

    return False
