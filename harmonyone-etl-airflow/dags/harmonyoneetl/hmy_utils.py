from pyhmy import blockchain
import os
import csv

sleep_interval = 300

fieldnames = [
	'difficulty',
	'epoch',
	'extraData',
	'gasLimit',
	'gasUsed',
	'hash',
	'logBloom',
	'miner',
	'mixHash',
	'nonce',
	'number',
	'parentHash',
	'receiptsRoot',
	'size',
	'stakingTransactions',
	'stateRoot',
	'stakingTransactions',
	'timestamp',
	'transactions',
	'transactionsInEthHash',
	'transactionsroot',
	'uncles',
	'viewid',
]

def get_block_by_number(block_num, network):
	result = blockchain.get_block_by_number(block_num=block_num, endpoint=network)
	return result

def get_latest_block(network):
	latest_block = blockchain.get_latest_header(network)
	return latest_block['blockNumber']

def get_blocks(network, fileObject, blockRange):
	try:
		latest_block_number = get_latest_block(network)
		fileObject.writeheader()
		i = latest_block_number
		while(i > 0):
			block = get_block_by_number(i, network)
			print('currently on block ', i, ' network ', network)
			fileObject.writerow({
				'difficulty': block['difficulty'],
				'epoch': block['epoch'],
				'extraData': block['extraData'],
				'gasLimit': block['gasLimit'],
				'gasUsed': block['gasUsed'],
				'hash': block['hash'],
				'logBloom': block['logBloom'],
				'miner': block['miner'],
				'mixHash': block['mixHash'],
				'nonce': block['nonce'],
				'number': block['number'],
				'parentHash': block['parentHash'],
				'receiptsRoot': block['receiptsRoot'],
				'size': block['size'],
				'stakingTransactions': block['stakingTransactions'],
				'stateRoot': block['stateRoot'],
				'stakingTransactions': block['stakingTransactions'],
				'timestamp': block['timestamp'],
				'transactions': block['transactions'],
				'transactionsInEthHash': block['transactionsInEthHash'],
				'transactionsroot': block['transactionsRoot'],
				'uncles': block['uncles'],
				'viewid': block['viewID']
			})
			i-=1
	except Exception as e:
		print('Error Printing ', e)

def getFileWriteStatus(file):
	append_write = 'w'
	if os.path.exists(file):
		append_write = 'a'
	return append_write

def getBlockData(filename, url): 
	appendWrite = getFileWriteStatus(filename)
	with open(filename, appendWrite) as file:
		writer = csv.DictWriter(file, fieldnames=fieldnames)		
		get_blocks(network=url, fileObject=writer)
